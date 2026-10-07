package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// Publisher is the minimal dependency the ingest handler needs from the
// processing pipeline -- it is satisfied by queue.Queue without ingest
// needing to import the queue package's full interface (keeps this package
// dependency-light and independently testable).
type Publisher interface {
	Publish(ctx context.Context, evt Event) error
}

// Metrics is the minimal set of ingest-side counters the handler updates.
// A concrete implementation lives in internal/signals so that all
// Prometheus registration stays in one place.
type Metrics interface {
	IncAccepted(evt Event)
	IncRejected(reason string)
}

// Handler is the HTTP handler for the fleet ingestion endpoint.
type Handler struct {
	publisher Publisher
	metrics   Metrics
	logger    *slog.Logger
	now       func() time.Time
}

func NewHandler(publisher Publisher, metrics Metrics, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{publisher: publisher, metrics: metrics, logger: logger, now: time.Now}
}

// batchLimit bounds how many events a single request may carry. Real
// clients batch small numbers of events per flush; an unbounded batch size
// would let one malicious or buggy client submit an arbitrarily large
// payload.
const batchLimit = 500

// ServeHTTP handles POST /v1/events. The body may be either a single
// RawEvent object or a JSON array of RawEvent objects (batched flush) --
// real client SDKs batch heartbeats/captures to amortize HTTP overhead, so
// the endpoint accepts both shapes rather than forcing one event per
// request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	raws, err := decodeEvents(r)
	if err != nil {
		h.metrics.IncRejected("decode_error")
		http.Error(w, "malformed request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(raws) == 0 {
		h.metrics.IncRejected("empty_batch")
		http.Error(w, "request body must contain at least one event", http.StatusBadRequest)
		return
	}
	if len(raws) > batchLimit {
		h.metrics.IncRejected("batch_too_large")
		http.Error(w, "batch exceeds maximum size", http.StatusBadRequest)
		return
	}

	receivedAt := h.now().UTC()
	accepted := 0
	var rejections []string

	for _, raw := range raws {
		evt, err := Normalize(raw, receivedAt)
		if err != nil {
			h.metrics.IncRejected("validation_error")
			rejections = append(rejections, err.Error())
			continue
		}
		if err := h.publisher.Publish(r.Context(), evt); err != nil {
			h.logger.Error("failed to publish event to queue", "error", err, "client_id", evt.ClientID)
			http.Error(w, "internal error enqueueing event", http.StatusInternalServerError)
			return
		}
		h.metrics.IncAccepted(evt)
		accepted++
	}

	resp := ingestResponse{Accepted: accepted, Rejected: len(rejections), Errors: rejections}
	w.Header().Set("Content-Type", "application/json")
	if accepted == 0 {
		w.WriteHeader(http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusAccepted)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

type ingestResponse struct {
	Accepted int      `json:"accepted"`
	Rejected int      `json:"rejected"`
	Errors   []string `json:"errors,omitempty"`
}

// decodeEvents accepts either a single JSON object or a JSON array. It reads
// the whole body into a json.RawMessage first so we can cheaply sniff
// whether it's an array or a single object before committing to a shape.
func decodeEvents(r *http.Request) ([]RawEvent, error) {
	defer r.Body.Close()

	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		return nil, err
	}
	trimmed := trimLeadingSpace(raw)
	if len(trimmed) == 0 {
		return nil, errEmptyBody
	}

	if trimmed[0] == '[' {
		var events []RawEvent
		if err := json.Unmarshal(raw, &events); err != nil {
			return nil, err
		}
		return events, nil
	}

	var single RawEvent
	if err := json.Unmarshal(raw, &single); err != nil {
		return nil, err
	}
	return []RawEvent{single}, nil
}

func trimLeadingSpace(b []byte) []byte {
	i := 0
	for i < len(b) {
		switch b[i] {
		case ' ', '\t', '\n', '\r':
			i++
			continue
		}
		break
	}
	return b[i:]
}

var errEmptyBody = &ValidationError{Field: "body", Reason: "empty request body"}
