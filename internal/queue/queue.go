// Package queue defines the processing-pipeline queue abstraction that sits
// between ingestion and the worker pool.
//
// There are two implementations behind this same interface:
//   - inmemory.go: a buffered-channel queue with SQS-style visibility
//     timeouts, appropriately scoped for a single-process local demo.
//   - internal/aws.SQSQueue: a real (structured, not run in this scaffold)
//     implementation backed by Amazon SQS via LocalStack, for the
//     one-real-AWS-shaped-deployment demo video.
//
// A real production deployment of FleetPulse would use the SQS (or
// NATS/Kafka) backend exclusively -- the in-memory channel implementation
// exists so the whole pipeline can be exercised with `go run` and no
// external dependencies, which is the right tradeoff for a local dev loop
// but is explicitly NOT how this would be deployed for a fleet of any real
// size (no durability across process restarts, no cross-instance fan-out).
package queue

import (
	"context"
	"time"

	"github.com/mstone/fleetpulse/internal/ingest"
)

// Message is an envelope around an ingest.Event as it flows through the
// queue, carrying queue-level metadata needed for retry/redelivery
// decisions.
type Message struct {
	ID            string
	Event         ingest.Event
	DeliveryCount int // 1 on first delivery; incremented on every redelivery
	EnqueuedAt    time.Time
}

// Delivery represents one handout of a Message to a consumer. The consumer
// MUST call exactly one of Ack or Nack (or let the visibility timeout
// expire, which is equivalent to an implicit Nack) once it is done
// processing.
//
// This ack/nack + visibility-timeout shape is deliberately modeled on SQS
// semantics so the in-memory and SQS-backed implementations can share
// identical worker-pool code: a worker that dies mid-processing simply never
// acks, the message's visibility timeout expires, and it becomes available
// for redelivery to another worker -- no special-cased "detect a dead
// worker" logic required anywhere.
type Delivery interface {
	Message() Message

	// Ack permanently removes the message from the queue. Call this only
	// after the message has been fully and successfully processed.
	Ack(ctx context.Context) error

	// Nack makes the message immediately available for redelivery (rather
	// than waiting out the remainder of the visibility timeout). Use this
	// for errors known to be transient, where waiting doesn't help.
	Nack(ctx context.Context) error
}

// Metrics is a point-in-time snapshot of queue health, used to drive the
// idle/backlog metrics exposed by the worker pool.
type Metrics struct {
	// Backlog is the number of messages that are enqueued and available for
	// delivery (i.e. not currently in-flight to a consumer).
	Backlog int
	// InFlight is the number of messages delivered to a consumer but not
	// yet acked or nacked.
	InFlight int
	// DeadLettered is the number of messages that exceeded MaxDeliveries
	// and were moved to the dead-letter sink instead of being redelivered.
	DeadLettered int
}

// Queue is the processing-pipeline queue interface. Implementations:
//   - inmemory.InMemoryQueue (this package)
//   - internal/aws.SQSQueue
type Queue interface {
	// Publish enqueues a normalized event for processing.
	Publish(ctx context.Context, evt ingest.Event) error

	// Receive long-polls for up to maxMessages deliveries, waiting at most
	// waitTime if the queue is empty. It returns immediately if at least
	// one message is available.
	Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]Delivery, error)

	// Metrics returns a snapshot of current queue health.
	Metrics(ctx context.Context) (Metrics, error)

	// Close releases any resources held by the queue (background reaper
	// goroutines, network clients, etc).
	Close() error
}
