package simulator

import "sync"

// SilencedCohorts is a thread-safe set of client versions that have "gone
// dark" -- the single most important demo hook in FleetPulse (see
// docs/runbook.md): a version being silenced makes every simulated client
// in that cohort stop sending heartbeats and capture events entirely,
// exactly as a real desktop-app cohort would if, say, a bad update broke
// its networking code or it lost the ability to reach the ingestion
// endpoint.
//
// This is checked by each client goroutine on every tick rather than by
// killing goroutines outright, so a silenced cohort can (in principle) be
// revived by unsilencing it -- useful for demoing recovery, not just
// failure.
type SilencedCohorts struct {
	mu  sync.RWMutex
	set map[string]bool
}

func NewSilencedCohorts() *SilencedCohorts {
	return &SilencedCohorts{set: make(map[string]bool)}
}

func (s *SilencedCohorts) Silence(version string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set[version] = true
}

func (s *SilencedCohorts) Unsilence(version string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.set, version)
}

func (s *SilencedCohorts) IsSilenced(version string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.set[version]
}
