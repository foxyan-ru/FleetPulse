package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/mstone/fleetpulse/internal/ingest"
)

// sampleErrors is a small pool of realistic-looking client error messages
// used for simulated EventError traffic.
var sampleErrors = []string{
	"capture buffer overflow: dropped 3 frames",
	"failed to acquire screen capture permission",
	"websocket connection reset by peer",
	"local cache corruption detected, resetting session",
	"disk write failed: ENOSPC",
}

// client is one simulated fleet member: a goroutine with a fixed cohort for
// its whole lifetime, periodically emitting heartbeats and occasionally
// capture sessions or errors over HTTP to the ingestion endpoint.
type client struct {
	id       string
	cohort   ingest.Cohort
	cfg      Config
	silenced *SilencedCohorts
	http     *http.Client
	rng      *rand.Rand
	logger   *slog.Logger
}

func newClient(id string, cohort ingest.Cohort, cfg Config, silenced *SilencedCohorts, logger *slog.Logger) *client {
	return &client{
		id:       id,
		cohort:   cohort,
		cfg:      cfg,
		silenced: silenced,
		http:     &http.Client{Timeout: 5 * time.Second},
		// Seed per-client so goroutines don't contend on a shared rand
		// source; the exact seed doesn't need to be cryptographically
		// unpredictable, just decorrelated between clients.
		rng:    rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(len(id))*2654435761)),
		logger: logger,
	}
}

// run is the client's main loop. It ticks on cfg.HeartbeatInterval (with
// jitter, so N clients don't all hammer the endpoint in lockstep) and, on
// each tick, decides whether to also emit a capture session and/or an error
// event.
func (c *client) run(ctx context.Context) {
	// Stagger initial start so a large fleet doesn't all boot on the same
	// tick either.
	initialDelay := time.Duration(c.rng.Int63n(int64(c.cfg.HeartbeatInterval)))
	select {
	case <-ctx.Done():
		return
	case <-time.After(initialDelay):
	}

	ticker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if c.silenced.IsSilenced(c.cohort.Version) {
				// This is the silent-cohort-failure scenario: a real client
				// whose networking broke, or whose process hung, doesn't
				// send a "I'm going dark now" event -- it just stops. We
				// model that faithfully by doing nothing at all, rather
				// than sending some kind of explicit shutdown signal.
				continue
			}
			c.tick(ctx)
		}
	}
}

func (c *client) tick(ctx context.Context) {
	if err := c.sendHeartbeat(ctx); err != nil {
		c.logger.Warn("heartbeat send failed", "client_id", c.id, "error", err)
	}

	if c.rng.Float64() < c.cfg.CaptureProbability {
		if err := c.sendCaptureSession(ctx); err != nil {
			c.logger.Warn("capture session send failed", "client_id", c.id, "error", err)
		}
	}

	if c.rng.Float64() < c.cfg.ErrorProbability {
		if err := c.sendError(ctx); err != nil {
			c.logger.Warn("error event send failed", "client_id", c.id, "error", err)
		}
	}
}

func (c *client) sendHeartbeat(ctx context.Context) error {
	evt := ingest.RawEvent{
		ClientID:  c.id,
		Type:      string(ingest.EventHeartbeat),
		Version:   c.cohort.Version,
		OS:        c.cohort.OS,
		Region:    c.cohort.Region,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	return c.post(ctx, []ingest.RawEvent{evt})
}

// sendCaptureSession simulates one "recording session" worth of capture
// events. Expected/received counts deliberately don't always match --
// captureLossRate models a fraction of fragments never making it to the
// server (dropped connection, client crash mid-session), which is exactly
// what the capture-completeness signal exists to detect.
func (c *client) sendCaptureSession(ctx context.Context) error {
	sessionID := uuid.NewString()
	expected := 3 + c.rng.Intn(6) // 3..8 fragments per session

	events := make([]ingest.RawEvent, 0, expected)
	for seq := 0; seq < expected; seq++ {
		if c.rng.Float64() < c.cfg.CaptureLossRate {
			continue // simulate a dropped fragment
		}
		events = append(events, ingest.RawEvent{
			ClientID:            c.id,
			Type:                string(ingest.EventCapture),
			Version:             c.cohort.Version,
			OS:                  c.cohort.OS,
			Region:              c.cohort.Region,
			Timestamp:           time.Now().UTC().Format(time.RFC3339),
			SessionID:           sessionID,
			ExpectedEventCount:  expected,
			SequenceNumber:      seq,
		})
	}
	if len(events) == 0 {
		return nil // whole session was "lost"; nothing to send
	}
	return c.post(ctx, events)
}

func (c *client) sendError(ctx context.Context) error {
	msg := sampleErrors[c.rng.Intn(len(sampleErrors))]
	evt := ingest.RawEvent{
		ClientID:     c.id,
		Type:         string(ingest.EventError),
		Version:      c.cohort.Version,
		OS:           c.cohort.OS,
		Region:       c.cohort.Region,
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
		ErrorMessage: msg,
	}
	return c.post(ctx, []ingest.RawEvent{evt})
}

func (c *client) post(ctx context.Context, events []ingest.RawEvent) error {
	body, err := json.Marshal(events)
	if err != nil {
		return fmt.Errorf("marshal events: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.IngestURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("post to ingestion endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("ingestion endpoint returned status %d", resp.StatusCode)
	}
	return nil
}
