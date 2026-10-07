// Package worker implements the processing-pipeline worker pool that
// consumes events off the queue (internal/queue), runs them through
// enrichment, updates client state, and feeds the signals computer.
//
// Resilience note: this pool demonstrates queue-level redelivery for real.
// A worker goroutine that never acks or nacks a message (because it panicked,
// because the process was killed with -9, because ctx was canceled mid-call)
// simply leaves that message in-flight; the queue's visibility-timeout
// reaper (see internal/queue/inmemory.go, or SQS's own visibility timeout
// for internal/aws.SQSQueue) puts it back on the backlog for another worker
// to pick up. There is no separate "detect a dead worker" mechanism here --
// that's the point of building retries on ack/nack semantics instead of
// worker-side bookkeeping.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mstone/fleetpulse/internal/ingest"
	"github.com/mstone/fleetpulse/internal/queue"
	"github.com/mstone/fleetpulse/internal/signals"
	"github.com/mstone/fleetpulse/internal/state"
)

// Config controls worker pool sizing and queue polling behavior.
type Config struct {
	NumWorkers      int
	ReceiveBatch    int
	ReceiveWait     time.Duration
	MetricsInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		NumWorkers:      8,
		ReceiveBatch:    10,
		ReceiveWait:     2 * time.Second,
		MetricsInterval: 2 * time.Second,
	}
}

// Pool is the worker pool that drains the queue and processes events.
type Pool struct {
	cfg      Config
	q        queue.Queue
	store    state.Store
	computer *signals.Computer
	enricher Enricher
	metrics  *signals.Metrics
	logger   *slog.Logger
}

func NewPool(cfg Config, q queue.Queue, store state.Store, computer *signals.Computer, enricher Enricher, metrics *signals.Metrics, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.Default()
	}
	return &Pool{cfg: cfg, q: q, store: store, computer: computer, enricher: enricher, metrics: metrics, logger: logger}
}

// Run blocks, running NumWorkers consumer goroutines plus a queue-metrics
// reporter, until ctx is canceled.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.reportQueueMetrics(ctx)
	}()

	for i := 0; i < p.cfg.NumWorkers; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			p.workerLoop(ctx, workerID)
		}()
	}

	wg.Wait()
}

func (p *Pool) workerLoop(ctx context.Context, workerID int) {
	p.logger.Info("worker starting", "worker_id", workerID)
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("worker stopping", "worker_id", workerID)
			return
		default:
		}

		deliveries, err := p.q.Receive(ctx, p.cfg.ReceiveBatch, p.cfg.ReceiveWait)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.logger.Error("queue receive failed", "worker_id", workerID, "error", err)
			continue
		}

		for _, d := range deliveries {
			p.process(ctx, workerID, d)
		}
	}
}

func (p *Pool) process(ctx context.Context, workerID int, d queue.Delivery) {
	msg := d.Message()
	evt := msg.Event

	start := time.Now()
	err := p.handleEvent(ctx, evt)
	p.metrics.ProcessingDuration.WithLabelValues(string(evt.Type)).Observe(time.Since(start).Seconds())

	if err == nil {
		if ackErr := d.Ack(ctx); ackErr != nil {
			p.logger.Error("ack failed", "worker_id", workerID, "message_id", msg.ID, "error", ackErr)
		}
		p.metrics.EventsProcessed.WithLabelValues(string(evt.Type), "success").Inc()
		return
	}

	if IsTransient(err) && msg.DeliveryCount < queue.DefaultMaxDeliveries {
		p.logger.Warn("transient processing failure, will retry",
			"worker_id", workerID, "message_id", msg.ID, "client_id", evt.ClientID,
			"delivery_count", msg.DeliveryCount, "error", err)
		p.metrics.WorkerRetries.Inc()
		p.metrics.EventsProcessed.WithLabelValues(string(evt.Type), "retry").Inc()
		if nackErr := d.Nack(ctx); nackErr != nil {
			p.logger.Error("nack failed", "worker_id", workerID, "message_id", msg.ID, "error", nackErr)
		}
		return
	}

	p.logger.Error("permanent (or retry-exhausted) processing failure, dead-lettering",
		"worker_id", workerID, "message_id", msg.ID, "client_id", evt.ClientID,
		"delivery_count", msg.DeliveryCount, "error", err)
	p.metrics.EventsProcessed.WithLabelValues(string(evt.Type), "dead_letter").Inc()
	// Nack here rather than a separate dead-letter call: the queue itself
	// owns the max-delivery bookkeeping and will route to its dead-letter
	// sink once DeliveryCount is exhausted (see queue.DefaultMaxDeliveries).
	_ = d.Nack(ctx)
}

// handleEvent updates client state and runs enrichment for one event. It
// returns an error suitable for the retry/dead-letter decision in process().
func (p *Pool) handleEvent(ctx context.Context, evt ingest.Event) error {
	isHeartbeat := evt.Type == ingest.EventHeartbeat
	if err := p.store.Upsert(ctx, evt.ClientID, evt.Cohort, evt.Timestamp, isHeartbeat, evt.SessionID); err != nil {
		return fmt.Errorf("state upsert: %w", err)
	}

	if err := p.enricher.Enrich(ctx, evt); err != nil {
		return err
	}

	if evt.Type == ingest.EventCapture {
		p.computer.RecordCapture(evt)
	}
	return nil
}

func (p *Pool) reportQueueMetrics(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.MetricsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m, err := p.q.Metrics(ctx)
			if err != nil {
				p.logger.Error("queue metrics fetch failed", "error", err)
				continue
			}
			p.metrics.QueueBacklog.Set(float64(m.Backlog))
			p.metrics.QueueInFlight.Set(float64(m.InFlight))
			p.metrics.QueueDeadLettered.Set(float64(m.DeadLettered))
		}
	}
}
