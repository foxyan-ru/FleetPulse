package signals

import (
	"sync"
	"time"
)

// mappingHealth is a small time-windowed success/failure tracker for calls
// to the downstream Workflow Mapping service. It intentionally does NOT use
// a simple all-time success rate -- an all-time ratio would take a very
// long time to recover visibly after a real outage ends, which would make
// the mapping_availability_ratio signal sluggish exactly when it matters
// most (right after a downstream incident is resolved).
type mappingHealth struct {
	window time.Duration

	mu      sync.Mutex
	samples []sample
}

type sample struct {
	at      time.Time
	success bool
}

func newMappingHealth(window time.Duration) *mappingHealth {
	return &mappingHealth{window: window}
}

func (m *mappingHealth) record(success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.samples = append(m.samples, sample{at: time.Now(), success: success})
	m.prune()
}

// ratio returns the success ratio over the trailing window. An empty window
// (no calls yet, e.g. at startup) is reported as fully healthy (1.0) rather
// than 0, since "no data" and "all failures" are meaningfully different and
// a fresh /metrics scrape shouldn't page anyone.
func (m *mappingHealth) ratio() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prune()

	if len(m.samples) == 0 {
		return 1.0
	}
	successes := 0
	for _, s := range m.samples {
		if s.success {
			successes++
		}
	}
	return float64(successes) / float64(len(m.samples))
}

// prune drops samples older than the window. Caller must hold m.mu.
func (m *mappingHealth) prune() {
	cutoff := time.Now().Add(-m.window)
	i := 0
	for i < len(m.samples) && m.samples[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		m.samples = m.samples[i:]
	}
}
