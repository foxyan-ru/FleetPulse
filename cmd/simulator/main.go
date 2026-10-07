// Command simulator runs FleetPulse's fake fleet: N concurrent simulated
// desktop-app/browser-extension clients phoning home to the ingestion
// endpoint. See internal/simulator for the implementation and
// docs/runbook.md for how the --kill-cohort flag is used in the
// silent-cohort-failure demo.
//
// Example:
//
//	go run ./cmd/simulator \
//	  --clients=300 \
//	  --interval=30s \
//	  --ingest-url=http://localhost:8080/v1/events \
//	  --kill-cohort=2.6.0-beta \
//	  --kill-cohort-after=2m
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mstone/fleetpulse/internal/simulator"
)

func main() {
	defaults := simulator.DefaultConfig()

	var (
		numClients         = flag.Int("clients", envInt("FLEETPULSE_SIM_CLIENTS", defaults.NumClients), "number of simulated concurrent clients")
		ingestURL          = flag.String("ingest-url", envString("FLEETPULSE_SIM_INGEST_URL", defaults.IngestURL), "ingestion endpoint URL to POST events to")
		heartbeatInterval  = flag.Duration("interval", envDuration("FLEETPULSE_SIM_INTERVAL", defaults.HeartbeatInterval), "base heartbeat emission interval per client")
		captureProbability = flag.Float64("capture-probability", defaults.CaptureProbability, "probability per tick that a client starts a capture session")
		errorProbability   = flag.Float64("error-probability", defaults.ErrorProbability, "probability per tick that a client emits a simulated error")
		captureLossRate    = flag.Float64("capture-loss-rate", defaults.CaptureLossRate, "fraction of capture fragments simulated as dropped in transit")
		killCohort         = flag.String("kill-cohort", envString("FLEETPULSE_SIM_KILL_COHORT", defaults.KillCohort), "client version to silence after --kill-cohort-after (demo: silent-cohort-failure scenario)")
		killCohortAfter    = flag.Duration("kill-cohort-after", envDuration("FLEETPULSE_SIM_KILL_COHORT_AFTER", defaults.KillCohortAfter), "delay before --kill-cohort takes effect")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := simulator.Config{
		NumClients:         *numClients,
		IngestURL:          *ingestURL,
		HeartbeatInterval:  *heartbeatInterval,
		CaptureProbability: *captureProbability,
		ErrorProbability:   *errorProbability,
		CaptureLossRate:    *captureLossRate,
		KillCohort:         *killCohort,
		KillCohortAfter:    *killCohortAfter,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sim := simulator.New(cfg, logger)
	if err := sim.Run(ctx); err != nil {
		logger.Error("simulator exited with error", "error", err)
		os.Exit(1)
	}
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
