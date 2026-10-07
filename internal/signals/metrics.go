package signals

import (
	"github.com/mstone/fleetpulse/internal/ingest"
	"github.com/prometheus/client_golang/prometheus"
)

// cohortLabels are attached to every per-cohort gauge so the silent-cohort
// demo is visible as: every OTHER label combination stays flat/healthy while
// exactly one (version, os, region) tuple's heartbeat_freshness_ratio and
// capture_completeness_ratio drop to zero.
var cohortLabelNames = []string{"version", "os", "region"}

// Metrics owns every Prometheus collector FleetPulse exposes at /metrics.
// It is constructed once in cmd/server and threaded through ingest, the
// worker pool, and the signals Computer -- keeping registration in one
// place avoids duplicate-registration panics and makes /metrics a complete,
// reviewable list of everything the system claims to measure.
type Metrics struct {
	// --- Signals catalog (docs/signals.md) ---
	HeartbeatFreshness  *prometheus.GaugeVec // fleetpulse_heartbeat_freshness_ratio
	CaptureCompleteness *prometheus.GaugeVec // fleetpulse_capture_completeness_ratio
	MappingAvailability prometheus.Gauge     // fleetpulse_mapping_availability_ratio

	// --- Ingest ---
	EventsAccepted *prometheus.CounterVec // fleetpulse_ingest_events_accepted_total{type,version,os,region}
	EventsRejected *prometheus.CounterVec // fleetpulse_ingest_events_rejected_total{reason}

	// --- Queue / worker pool ---
	QueueBacklog       prometheus.Gauge     // fleetpulse_queue_backlog
	QueueInFlight      prometheus.Gauge     // fleetpulse_queue_in_flight
	QueueDeadLettered  prometheus.Gauge     // fleetpulse_queue_dead_lettered_total
	EventsProcessed    *prometheus.CounterVec // fleetpulse_worker_events_processed_total{type,outcome}
	ProcessingDuration *prometheus.HistogramVec // fleetpulse_worker_processing_duration_seconds{type}
	WorkerRetries      prometheus.Counter   // fleetpulse_worker_retries_total
}

// NewMetrics registers all collectors against reg and returns the handle
// used across the ingestion, worker, and signals packages.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		HeartbeatFreshness: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fleetpulse_heartbeat_freshness_ratio",
			Help: "Fraction of registered clients in this cohort with a heartbeat in the last 60s (see docs/signals.md).",
		}, cohortLabelNames),
		CaptureCompleteness: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fleetpulse_capture_completeness_ratio",
			Help: "Average fraction of expected capture events actually received per session, by cohort (see docs/signals.md).",
		}, cohortLabelNames),
		MappingAvailability: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fleetpulse_mapping_availability_ratio",
			Help: "Rolling success ratio of calls to the downstream Workflow Mapping service (see docs/signals.md).",
		}),
		EventsAccepted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fleetpulse_ingest_events_accepted_total",
			Help: "Total events accepted by the ingestion endpoint.",
		}, []string{"type", "version", "os", "region"}),
		EventsRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fleetpulse_ingest_events_rejected_total",
			Help: "Total events rejected by the ingestion endpoint, by reason.",
		}, []string{"reason"}),
		QueueBacklog: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fleetpulse_queue_backlog",
			Help: "Number of messages enqueued and awaiting delivery to a worker.",
		}),
		QueueInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fleetpulse_queue_in_flight",
			Help: "Number of messages delivered to a worker but not yet acked or nacked.",
		}),
		QueueDeadLettered: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fleetpulse_queue_dead_lettered_total",
			Help: "Number of messages that exceeded max delivery attempts and were dead-lettered.",
		}),
		EventsProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fleetpulse_worker_events_processed_total",
			Help: "Total events processed by the worker pool, by event type and outcome (success|retry|dead_letter).",
		}, []string{"type", "outcome"}),
		ProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fleetpulse_worker_processing_duration_seconds",
			Help:    "Time to process one event in the worker pool (including the enrichment call), by event type.",
			Buckets: prometheus.DefBuckets,
		}, []string{"type"}),
		WorkerRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fleetpulse_worker_retries_total",
			Help: "Total number of transient-failure retries (nacks) issued by the worker pool.",
		}),
	}

	reg.MustRegister(
		m.HeartbeatFreshness,
		m.CaptureCompleteness,
		m.MappingAvailability,
		m.EventsAccepted,
		m.EventsRejected,
		m.QueueBacklog,
		m.QueueInFlight,
		m.QueueDeadLettered,
		m.EventsProcessed,
		m.ProcessingDuration,
		m.WorkerRetries,
	)

	return m
}

// IncAccepted implements ingest.Metrics.
func (m *Metrics) IncAccepted(evt ingest.Event) {
	m.EventsAccepted.WithLabelValues(string(evt.Type), evt.Cohort.Version, evt.Cohort.OS, evt.Cohort.Region).Inc()
}

// IncRejected implements ingest.Metrics.
func (m *Metrics) IncRejected(reason string) {
	m.EventsRejected.WithLabelValues(reason).Inc()
}
