package worker

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/mstone/fleetpulse/internal/downstreamclient"
	"github.com/mstone/fleetpulse/internal/ingest"
	"github.com/mstone/fleetpulse/internal/signals"
)

// Enricher performs the (stubbed) enrichment step that stands in for a real
// downstream AI/analytics pipeline -- in a production FleetPulse this is
// where a capture session's raw events would be handed off to whatever does
// classification/analysis on them. Here it's deliberately shallow: its job
// in this project is to exercise the retry/dead-letter machinery and to
// produce real mapping-availability data, not to do real analysis.
type Enricher interface {
	Enrich(ctx context.Context, evt ingest.Event) error
}

// StubEnricher is the default Enricher. For capture events it makes a real
// HTTP call to the downstream Workflow Mapping stub service and records the
// outcome against the mapping-availability signal. For all event types it
// also injects a configurable rate of simulated transient failures, so the
// worker pool's retry path has something to exercise even when the
// downstream stub is perfectly healthy (real enrichment/analytics steps
// fail transiently too -- OOM, rate limits, node preemption).
type StubEnricher struct {
	downstream *downstreamclient.Client
	computer   *signals.Computer

	transientFailureRate float64 // e.g. 0.05 == 5% of enrichment calls fail transiently
	simulatedLatency      time.Duration

	mu  sync.Mutex
	rng *rand.Rand
}

func NewStubEnricher(downstream *downstreamclient.Client, computer *signals.Computer, transientFailureRate float64, simulatedLatency time.Duration) *StubEnricher {
	return &StubEnricher{
		downstream:            downstream,
		computer:              computer,
		transientFailureRate: transientFailureRate,
		simulatedLatency:      simulatedLatency,
		rng:                   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (e *StubEnricher) Enrich(ctx context.Context, evt ingest.Event) error {
	// Simulate the fixed cost of an enrichment step (feature extraction,
	// a small model call, whatever the real pipeline would do) so the
	// processing-duration histogram has realistic non-zero values.
	if e.simulatedLatency > 0 {
		select {
		case <-time.After(e.simulatedLatency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	switch evt.Type {
	case ingest.EventCapture:
		if err := e.mapWorkflow(ctx, evt); err != nil {
			return err
		}
	case ingest.EventHeartbeat, ingest.EventError:
		// No downstream dependency for these event types in this scaffold;
		// they still flow through enrichment so retry/backpressure behavior
		// is uniform across event types.
	}

	if e.rollTransientFailure() {
		return fmt.Errorf("simulated enrichment hiccup: %w", ErrTransient)
	}
	return nil
}

func (e *StubEnricher) mapWorkflow(ctx context.Context, evt ingest.Event) error {
	_, err := e.downstream.Map(ctx, downstreamclient.MapRequest{
		ClientID:  evt.ClientID,
		SessionID: evt.SessionID,
		Sequence:  evt.SequenceNumber,
	})
	e.computer.RecordMappingResult(err == nil)
	if err != nil {
		// Downstream unavailability is exactly the kind of thing a retry
		// might resolve (the stub might be mid-restart, or configured with
		// a transient failure-injection rate), so wrap it as transient.
		return fmt.Errorf("workflow mapping call failed: %w: %v", ErrTransient, err)
	}
	return nil
}

func (e *StubEnricher) rollTransientFailure() bool {
	if e.transientFailureRate <= 0 {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rng.Float64() < e.transientFailureRate
}
