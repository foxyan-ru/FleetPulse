// Command server is FleetPulse's ingestion + processing service: it accepts
// fleet events over HTTP, normalizes and validates them, hands them to a
// queue-backed worker pool for (stubbed) enrichment, and exposes the
// resulting signals as Prometheus metrics.
//
// Backends for the queue and client-state store are selectable via env vars
// so the same binary runs against zero external dependencies locally (the
// default) or against LocalStack/AWS for the demo-video deployment:
//
//	FLEETPULSE_QUEUE_BACKEND=memory|sqs        (default: memory)
//	FLEETPULSE_STATE_BACKEND=memory|dynamodb   (default: memory)
//	FLEETPULSE_SQS_QUEUE_URL=...               (required if QUEUE_BACKEND=sqs)
//	FLEETPULSE_DYNAMODB_TABLE=...              (required if STATE_BACKEND=dynamodb)
//	AWS_ENDPOINT_URL=http://localhost:4566     (point AWS SDK calls at LocalStack)
//	FLEETPULSE_DOWNSTREAM_URL=http://localhost:8090  (Workflow Mapping stub)
//	FLEETPULSE_ADDR=:8080
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	fleetaws "github.com/mstone/fleetpulse/internal/aws"
	"github.com/mstone/fleetpulse/internal/downstreamclient"
	"github.com/mstone/fleetpulse/internal/ingest"
	"github.com/mstone/fleetpulse/internal/queue"
	"github.com/mstone/fleetpulse/internal/signals"
	"github.com/mstone/fleetpulse/internal/state"
	"github.com/mstone/fleetpulse/internal/worker"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := run(logger); err != nil {
		logger.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	addr := envString("FLEETPULSE_ADDR", ":8080")
	downstreamURL := envString("FLEETPULSE_DOWNSTREAM_URL", "http://localhost:8090")

	registry := prometheus.NewRegistry()
	metrics := signals.NewMetrics(registry)

	q, closeQueue, err := buildQueue(ctx, logger)
	if err != nil {
		return err
	}
	defer closeQueue()

	stateStore, err := buildStateStore(ctx, logger)
	if err != nil {
		return err
	}

	computer := signals.NewComputer(stateStore, metrics, logger)
	downstream := downstreamclient.New(downstreamURL, 3*time.Second)
	enricher := worker.NewStubEnricher(downstream, computer, 0.03, 50*time.Millisecond)

	pool := worker.NewPool(worker.DefaultConfig(), q, stateStore, computer, enricher, metrics, logger)

	ingestHandler := ingest.NewHandler(publisherAdapter{q}, metrics, logger)

	mux := http.NewServeMux()
	mux.Handle("/v1/events", ingestHandler)
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Background loops: the worker pool and the signals recomputation
	// ticker both run for the lifetime of the process, stopping when ctx is
	// canceled (SIGINT/SIGTERM).
	go pool.Run(ctx)
	go computer.Start(ctx, 10*time.Second)

	go func() {
		<-ctx.Done()
		logger.Info("shutdown signal received, stopping HTTP server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("http server shutdown error", "error", err)
		}
	}()

	logger.Info("fleetpulse server listening", "addr", addr, "downstream_url", downstreamURL)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// publisherAdapter adapts queue.Queue to ingest.Publisher so the ingest
// package doesn't need to depend on the full queue.Queue interface (it only
// ever needs to Publish).
type publisherAdapter struct {
	q queue.Queue
}

func (p publisherAdapter) Publish(ctx context.Context, evt ingest.Event) error {
	return p.q.Publish(ctx, evt)
}

// buildQueue selects the in-memory or SQS-backed queue implementation based
// on FLEETPULSE_QUEUE_BACKEND. See internal/queue's package doc for why both
// exist.
func buildQueue(ctx context.Context, logger *slog.Logger) (queue.Queue, func(), error) {
	backend := envString("FLEETPULSE_QUEUE_BACKEND", "memory")
	switch backend {
	case "memory":
		q := queue.NewInMemoryQueue(logger)
		return q, func() { _ = q.Close() }, nil
	case "sqs":
		queueURL := os.Getenv("FLEETPULSE_SQS_QUEUE_URL")
		if queueURL == "" {
			return nil, nil, errors.New("FLEETPULSE_SQS_QUEUE_URL must be set when FLEETPULSE_QUEUE_BACKEND=sqs")
		}
		cfg, err := fleetaws.LoadLocalConfig(ctx)
		if err != nil {
			return nil, nil, err
		}
		q := fleetaws.NewSQSQueue(cfg, queueURL, queue.DefaultVisibilityTimeout, logger)
		logger.Info("using SQS-backed queue", "queue_url", queueURL, "endpoint", fleetaws.Endpoint())
		return q, func() { _ = q.Close() }, nil
	default:
		return nil, nil, errors.New("unknown FLEETPULSE_QUEUE_BACKEND: " + backend + " (want memory|sqs)")
	}
}

// buildStateStore selects the in-memory or DynamoDB-backed client-state
// store based on FLEETPULSE_STATE_BACKEND.
func buildStateStore(ctx context.Context, logger *slog.Logger) (state.Store, error) {
	backend := envString("FLEETPULSE_STATE_BACKEND", "memory")
	switch backend {
	case "memory":
		return state.NewInMemoryStore(), nil
	case "dynamodb":
		table := os.Getenv("FLEETPULSE_DYNAMODB_TABLE")
		if table == "" {
			return nil, errors.New("FLEETPULSE_DYNAMODB_TABLE must be set when FLEETPULSE_STATE_BACKEND=dynamodb")
		}
		cfg, err := fleetaws.LoadLocalConfig(ctx)
		if err != nil {
			return nil, err
		}
		logger.Info("using DynamoDB-backed state store", "table", table, "endpoint", fleetaws.Endpoint())
		return fleetaws.NewDynamoDBStore(cfg, table, logger), nil
	default:
		return nil, errors.New("unknown FLEETPULSE_STATE_BACKEND: " + backend + " (want memory|dynamodb)")
	}
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
