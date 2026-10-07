// Package simulator implements FleetPulse's fake fleet: a configurable
// number of concurrent goroutines, each standing in for one desktop-app or
// browser-extension install, phoning home to the ingestion endpoint with
// heartbeats, capture events, and occasional errors.
//
// The scope here is deliberate: this never builds a real desktop app or
// browser extension (see the project README's scope-control note) -- it
// exists purely to give the ingestion/queue/worker/signals pipeline
// something realistic to observe.
package simulator

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// Config controls the simulated fleet's size and behavior.
type Config struct {
	NumClients        int
	IngestURL         string
	HeartbeatInterval time.Duration

	// CaptureProbability is the chance, per heartbeat tick, that a client
	// also starts a capture session.
	CaptureProbability float64
	// ErrorProbability is the chance, per heartbeat tick, that a client
	// also emits a simulated error event.
	ErrorProbability float64
	// CaptureLossRate is the fraction of individual capture fragments
	// within a session that are simulated as "never sent" -- this is what
	// gives the capture-completeness signal something realistic to
	// measure below 100%.
	CaptureLossRate float64

	// KillCohort, if non-empty, is the client version that goes silent
	// after KillCohortAfter has elapsed since the simulator started. This
	// is the "silent-cohort-failure" demo scenario described in
	// docs/runbook.md. Leave KillCohort empty to disable the scenario.
	KillCohort      string
	KillCohortAfter time.Duration
}

// DefaultConfig returns reasonable defaults for a local demo run.
func DefaultConfig() Config {
	return Config{
		NumClients:         300,
		IngestURL:          "http://localhost:8080/v1/events",
		HeartbeatInterval:  30 * time.Second,
		CaptureProbability: 0.15,
		ErrorProbability:   0.03,
		CaptureLossRate:    0.08,
		KillCohort:         "",
		KillCohortAfter:    2 * time.Minute,
	}
}

// Simulator owns the fleet of simulated clients and the kill-cohort timer.
type Simulator struct {
	cfg      Config
	silenced *SilencedCohorts
	logger   *slog.Logger
}

func New(cfg Config, logger *slog.Logger) *Simulator {
	if logger == nil {
		logger = slog.Default()
	}
	return &Simulator{cfg: cfg, silenced: NewSilencedCohorts(), logger: logger}
}

// Run spins up cfg.NumClients simulated clients and blocks until ctx is
// canceled, at which point it waits for all client goroutines to return.
func (s *Simulator) Run(ctx context.Context) error {
	if s.cfg.NumClients <= 0 {
		return fmt.Errorf("simulator: NumClients must be > 0, got %d", s.cfg.NumClients)
	}

	s.logger.Info("starting simulated fleet",
		"num_clients", s.cfg.NumClients,
		"ingest_url", s.cfg.IngestURL,
		"heartbeat_interval", s.cfg.HeartbeatInterval,
		"kill_cohort", s.cfg.KillCohort,
		"kill_cohort_after", s.cfg.KillCohortAfter,
	)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	var wg sync.WaitGroup
	cohortCounts := make(map[string]int)

	for i := 0; i < s.cfg.NumClients; i++ {
		id := fmt.Sprintf("sim-client-%05d", i)
		cohort := RandomCohort(rng)
		cohortCounts[cohort.Version]++

		c := newClient(id, cohort, s.cfg, s.silenced, s.logger)
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.run(ctx)
		}()
	}

	s.logger.Info("fleet cohort distribution", "by_version", cohortCounts)

	if s.cfg.KillCohort != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runKillCohortTimer(ctx)
		}()
	}

	<-ctx.Done()
	s.logger.Info("shutdown signal received, waiting for simulated clients to stop")
	wg.Wait()
	s.logger.Info("simulated fleet stopped")
	return nil
}

// runKillCohortTimer implements the single most important demo hook in this
// project: after KillCohortAfter elapses, every simulated client whose
// version matches KillCohort stops sending traffic entirely, and stays
// silent for the rest of the run. See docs/runbook.md for how this is meant
// to be diagnosed from the dashboard down to this cause.
func (s *Simulator) runKillCohortTimer(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(s.cfg.KillCohortAfter):
	}

	s.logger.Warn("simulating silent-cohort failure: cohort going dark now",
		"version", s.cfg.KillCohort)
	s.silenced.Silence(s.cfg.KillCohort)
}
