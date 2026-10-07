// Command downstream is a tiny standalone HTTP service standing in for the
// real "Workflow Mapping" service that a production FleetPulse's enrichment
// step would depend on. It exists so the mapping-availability signal
// (docs/signals.md) has a genuine, independently-failable dependency to
// measure instead of an in-process stub that can never actually go down.
//
// It exposes:
//
//	POST /v1/workflow-mapping   the "real" endpoint the worker pool calls
//	GET  /healthz               liveness probe
//	POST /admin/config          adjust failure-injection behavior on demand
//	GET  /admin/config          view current failure-injection behavior
//
// Example failure injection for the mapping-availability demo:
//
//	curl -X POST localhost:8090/admin/config \
//	  -d '{"failure_rate": 0.6, "timeout_ms": 3000, "down": false}'
package main

import (
	"encoding/json"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

// adminConfig is the mutable failure-injection state, guarded by mu.
type adminConfig struct {
	mu sync.RWMutex

	// FailureRate is the fraction of requests that should be answered with
	// a 5xx error, independent of Down/TimeoutMS.
	FailureRate float64 `json:"failure_rate"`
	// TimeoutMS, if > 0, is how long to sleep before responding (or before
	// the caller's own client-side timeout fires, whichever is first) --
	// simulates the dependency being slow rather than outright down.
	TimeoutMS int `json:"timeout_ms"`
	// Down, if true, fails every request immediately regardless of
	// FailureRate -- simulates a hard outage.
	Down bool `json:"down"`
}

func (c *adminConfig) snapshot() adminConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return adminConfig{FailureRate: c.FailureRate, TimeoutMS: c.TimeoutMS, Down: c.Down}
}

func (c *adminConfig) set(update adminConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.FailureRate = update.FailureRate
	c.TimeoutMS = update.TimeoutMS
	c.Down = update.Down
}

type server struct {
	cfg    *adminConfig
	logger *slog.Logger
	rng    *rand.Rand
	rngMu  sync.Mutex
}

func main() {
	addr := envString("FLEETPULSE_DOWNSTREAM_ADDR", ":8090")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	s := &server{
		cfg:    &adminConfig{},
		logger: logger,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/workflow-mapping", s.handleMap)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/admin/config", s.handleAdminConfig)

	logger.Info("downstream workflow-mapping stub listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("server exited", "error", err)
		os.Exit(1)
	}
}

type mapRequest struct {
	ClientID  string `json:"client_id"`
	SessionID string `json:"session_id"`
	Sequence  int    `json:"sequence_number"`
}

type mapResponse struct {
	MappingID string `json:"mapping_id"`
	Status    string `json:"status"`
}

func (s *server) handleMap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cfg := s.cfg.snapshot()

	if cfg.TimeoutMS > 0 {
		select {
		case <-time.After(time.Duration(cfg.TimeoutMS) * time.Millisecond):
		case <-r.Context().Done():
			return // caller gave up; nothing more to do
		}
	}

	if cfg.Down {
		http.Error(w, "workflow mapping service is down (admin-forced)", http.StatusServiceUnavailable)
		return
	}

	if cfg.FailureRate > 0 && s.roll() < cfg.FailureRate {
		http.Error(w, "simulated workflow mapping failure", http.StatusInternalServerError)
		return
	}

	var req mapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "malformed request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	resp := mapResponse{
		MappingID: uuid.NewString(),
		Status:    "mapped",
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *server) handleAdminConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.cfg.snapshot())
	case http.MethodPost:
		var update adminConfig
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
			http.Error(w, "malformed request body: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		if update.FailureRate < 0 || update.FailureRate > 1 {
			http.Error(w, "failure_rate must be between 0 and 1", http.StatusBadRequest)
			return
		}
		s.cfg.set(update)
		s.logger.Warn("downstream failure-injection config updated",
			"failure_rate", update.FailureRate, "timeout_ms", update.TimeoutMS, "down", update.Down)
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *server) roll() float64 {
	s.rngMu.Lock()
	defer s.rngMu.Unlock()
	return s.rng.Float64()
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
