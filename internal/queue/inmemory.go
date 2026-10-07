package queue

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mstone/fleetpulse/internal/ingest"
)

// DefaultVisibilityTimeout mirrors a reasonable SQS default: long enough for
// a worker to run the (stubbed) enrichment step, short enough that a dead
// worker's messages come back quickly in a demo.
const DefaultVisibilityTimeout = 10 * time.Second

// DefaultMaxDeliveries bounds how many times a message will be redelivered
// before it is moved to the dead-letter sink. This is what turns "a worker
// keeps crashing on this message" into a bounded, observable failure instead
// of an infinite redelivery loop.
const DefaultMaxDeliveries = 5

// inFlightMessage tracks a message that has been handed to a consumer but
// not yet acked/nacked.
type inFlightMessage struct {
	msg          Message
	visibleAfter time.Time
}

// InMemoryQueue is a single-process, buffered-channel-backed Queue
// implementation with SQS-style visibility timeouts and redelivery. It is
// appropriately scoped for local development and the demo: it gives up
// durability (a process restart loses everything in-flight or backlogged)
// and cross-instance fan-out (only goroutines in this process can consume
// from it) in exchange for zero operational dependencies.
//
// A production deployment would use internal/aws.SQSQueue instead -- see
// the package doc in queue.go.
type InMemoryQueue struct {
	visibilityTimeout time.Duration
	maxDeliveries     int

	mu        sync.Mutex
	ready     []Message                  // backlog: available for delivery
	inFlight  map[string]*inFlightMessage // messageID -> in-flight record
	deadLetter []Message

	notify chan struct{} // signaled whenever a message becomes ready

	closeOnce sync.Once
	closed    chan struct{}
	reaperWG  sync.WaitGroup

	logger *slog.Logger
}

// NewInMemoryQueue constructs an InMemoryQueue and starts its background
// visibility-timeout reaper goroutine.
func NewInMemoryQueue(logger *slog.Logger) *InMemoryQueue {
	if logger == nil {
		logger = slog.Default()
	}
	q := &InMemoryQueue{
		visibilityTimeout: DefaultVisibilityTimeout,
		maxDeliveries:     DefaultMaxDeliveries,
		inFlight:          make(map[string]*inFlightMessage),
		notify:            make(chan struct{}, 1),
		closed:            make(chan struct{}),
		logger:            logger,
	}
	q.reaperWG.Add(1)
	go q.reapExpiredVisibility()
	return q
}

func (q *InMemoryQueue) Publish(ctx context.Context, evt ingest.Event) error {
	msg := Message{
		ID:            uuid.NewString(),
		Event:         evt,
		DeliveryCount: 0,
		EnqueuedAt:    time.Now().UTC(),
	}

	q.mu.Lock()
	q.ready = append(q.ready, msg)
	q.mu.Unlock()

	q.wake()
	return nil
}

func (q *InMemoryQueue) Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]Delivery, error) {
	deadline := time.Now().Add(waitTime)
	for {
		q.mu.Lock()
		n := len(q.ready)
		if n > maxMessages {
			n = maxMessages
		}
		var out []Delivery
		if n > 0 {
			batch := q.ready[:n]
			q.ready = q.ready[n:]
			out = make([]Delivery, 0, n)
			for _, m := range batch {
				m.DeliveryCount++
				rec := &inFlightMessage{msg: m, visibleAfter: time.Now().Add(q.visibilityTimeout)}
				q.inFlight[m.ID] = rec
				out = append(out, &inMemoryDelivery{q: q, id: m.ID, msg: m})
			}
		}
		q.mu.Unlock()

		if len(out) > 0 {
			return out, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.closed:
			return nil, nil
		case <-q.notify:
			// something published or was redelivered; loop and re-check
		case <-time.After(remaining):
			return nil, nil
		}
	}
}

func (q *InMemoryQueue) Metrics(ctx context.Context) (Metrics, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Metrics{
		Backlog:      len(q.ready),
		InFlight:     len(q.inFlight),
		DeadLettered: len(q.deadLetter),
	}, nil
}

func (q *InMemoryQueue) Close() error {
	q.closeOnce.Do(func() { close(q.closed) })
	q.reaperWG.Wait()
	return nil
}

func (q *InMemoryQueue) wake() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// reapExpiredVisibility is the mechanism that makes "kill a worker mid-run
// and the job gets picked back up" real: any in-flight message whose
// visibility timeout has elapsed without an Ack/Nack is put back on the
// ready backlog (or moved to the dead letter sink if it has exceeded
// maxDeliveries), exactly as SQS would do.
func (q *InMemoryQueue) reapExpiredVisibility() {
	defer q.reaperWG.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-q.closed:
			return
		case now := <-ticker.C:
			q.mu.Lock()
			for id, rec := range q.inFlight {
				if now.Before(rec.visibleAfter) {
					continue
				}
				delete(q.inFlight, id)
				if rec.msg.DeliveryCount >= q.maxDeliveries {
					q.logger.Warn("message exceeded max deliveries, dead-lettering",
						"message_id", id, "delivery_count", rec.msg.DeliveryCount,
						"client_id", rec.msg.Event.ClientID)
					q.deadLetter = append(q.deadLetter, rec.msg)
					continue
				}
				q.logger.Info("visibility timeout expired, redelivering",
					"message_id", id, "delivery_count", rec.msg.DeliveryCount)
				q.ready = append(q.ready, rec.msg)
			}
			q.mu.Unlock()
			q.wake()
		}
	}
}

func (q *InMemoryQueue) ack(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inFlight, id)
	return nil
}

func (q *InMemoryQueue) nack(id string) error {
	q.mu.Lock()
	rec, ok := q.inFlight[id]
	if !ok {
		q.mu.Unlock()
		return nil
	}
	delete(q.inFlight, id)
	if rec.msg.DeliveryCount >= q.maxDeliveries {
		q.deadLetter = append(q.deadLetter, rec.msg)
		q.mu.Unlock()
		return nil
	}
	q.ready = append(q.ready, rec.msg)
	q.mu.Unlock()
	q.wake()
	return nil
}

// inMemoryDelivery implements Delivery for InMemoryQueue.
type inMemoryDelivery struct {
	q   *InMemoryQueue
	id  string
	msg Message
}

func (d *inMemoryDelivery) Message() Message { return d.msg }

func (d *inMemoryDelivery) Ack(ctx context.Context) error { return d.q.ack(d.id) }

func (d *inMemoryDelivery) Nack(ctx context.Context) error { return d.q.nack(d.id) }
