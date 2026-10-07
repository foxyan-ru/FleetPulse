// Package signals computes the three signals defined in docs/signals.md
// (heartbeat freshness, capture completeness, mapping availability) from
// live ingested data and exposes them as Prometheus metrics, labeled by
// cohort so the silent-cohort-failure demo is visible as a metric dropping
// for exactly one (version, os, region) combination.
//
// Read docs/signals.md alongside this file -- the doc defines *why* each
// signal is computed this way and what its target threshold is; this file
// is just the "how".
package signals

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/mstone/fleetpulse/internal/ingest"
	"github.com/mstone/fleetpulse/internal/state"
)

// FreshnessWindow is the window used by the heartbeat-freshness signal: a
// client counts as "fresh" if it has sent a heartbeat within this window.
// See docs/signals.md for the rationale (60s = 2x the default simulator
// heartbeat interval of 30s, giving one missed beat of slack before a
// client is counted as stale).
const FreshnessWindow = 60 * time.Second

// sessionProgress tracks how many distinct sequence numbers of an expected
// capture session have been received, to compute capture completeness.
type sessionProgress struct {
	cohort      ingest.Cohort
	expected    int
	received    map[int]struct{}
	lastUpdated time.Time
}

// Computer computes all three catalog signals on a periodic tick and
// publishes them to the Metrics registered gauges.
type Computer struct {
	store   state.Store
	metrics *Metrics
	mapping *mappingHealth
	logger  *slog.Logger

	mu       sync.Mutex
	sessions map[string]*sessionProgress // session_id -> progress

	// sessionTTL bounds how long a completed/abandoned session's progress
	// record is kept before being pruned, so long-running processes don't
	// accumulate unbounded memory from a fleet that's been running for days.
	sessionTTL time.Duration
}

func NewComputer(store state.Store, metrics *Metrics, logger *slog.Logger) *Computer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Computer{
		store:      store,
		metrics:    metrics,
		mapping:    newMappingHealth(5 * time.Minute),
		logger:     logger,
		sessions:   make(map[string]*sessionProgress),
		sessionTTL: 30 * time.Minute,
	}
}

// RecordCapture is called by the worker pool for every successfully
// processed capture event, feeding the capture-completeness signal.
func (c *Computer) RecordCapture(evt ingest.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()

	sp, ok := c.sessions[evt.SessionID]
	if !ok {
		sp = &sessionProgress{
			cohort:   evt.Cohort,
			expected: evt.ExpectedEventCount,
			received: make(map[int]struct{}),
		}
		c.sessions[evt.SessionID] = sp
	}
	sp.received[evt.SequenceNumber] = struct{}{}
	sp.lastUpdated = evt.ReceivedAt
}

// RecordMappingResult is called by the worker pool after every call to the
// downstream Workflow Mapping stub, feeding the mapping-availability
// signal.
func (c *Computer) RecordMappingResult(success bool) {
	c.mapping.record(success)
}

// Start runs the periodic recomputation loop until ctx is canceled.
func (c *Computer) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Compute once immediately so /metrics isn't empty for a full interval
	// after startup.
	c.tick(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

func (c *Computer) tick(ctx context.Context) {
	c.computeHeartbeatFreshness(ctx)
	c.computeCaptureCompleteness()
	c.metrics.MappingAvailability.Set(c.mapping.ratio())
}

// computeHeartbeatFreshness implements:
//
//	heartbeat_freshness(cohort) = |{clients in cohort with last_heartbeat within 60s}|
//	                               ------------------------------------------------------
//	                               |{clients in cohort}|
func (c *Computer) computeHeartbeatFreshness(ctx context.Context) {
	clients, err := c.store.All(ctx)
	if err != nil {
		c.logger.Error("computeHeartbeatFreshness: store.All failed", "error", err)
		return
	}

	type bucket struct{ fresh, total int }
	buckets := make(map[ingest.Cohort]*bucket)

	now := time.Now()
	for _, cl := range clients {
		b, ok := buckets[cl.Cohort]
		if !ok {
			b = &bucket{}
			buckets[cl.Cohort] = b
		}
		b.total++
		if now.Sub(cl.LastHeartbeatAt) <= FreshnessWindow {
			b.fresh++
		}
	}

	for cohort, b := range buckets {
		ratio := 0.0
		if b.total > 0 {
			ratio = float64(b.fresh) / float64(b.total)
		}
		c.metrics.HeartbeatFreshness.WithLabelValues(cohort.Version, cohort.OS, cohort.Region).Set(ratio)
	}
}

// computeCaptureCompleteness implements, per cohort:
//
//	capture_completeness(cohort) = mean over sessions in cohort of
//	                                 |distinct sequence numbers received| / expected_event_count
func (c *Computer) computeCaptureCompleteness() {
	c.mu.Lock()
	defer c.mu.Unlock()

	type bucket struct {
		sumRatio float64
		count    int
	}
	buckets := make(map[ingest.Cohort]*bucket)

	now := time.Now()
	for id, sp := range c.sessions {
		// prune stale session progress records
		if now.Sub(sp.lastUpdated) > c.sessionTTL {
			delete(c.sessions, id)
			continue
		}
		if sp.expected == 0 {
			continue
		}
		ratio := float64(len(sp.received)) / float64(sp.expected)
		if ratio > 1 {
			ratio = 1 // defensive: duplicate/replayed sequence numbers shouldn't push this over 100%
		}
		b, ok := buckets[sp.cohort]
		if !ok {
			b = &bucket{}
			buckets[sp.cohort] = b
		}
		b.sumRatio += ratio
		b.count++
	}

	for cohort, b := range buckets {
		if b.count == 0 {
			continue
		}
		c.metrics.CaptureCompleteness.WithLabelValues(cohort.Version, cohort.OS, cohort.Region).Set(b.sumRatio / float64(b.count))
	}
}
