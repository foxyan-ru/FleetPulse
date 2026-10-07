package ingest

import (
	"fmt"
	"strings"
	"time"
)

// knownOS / knownRegions are the sets we normalize free-text client fields
// into. Anything outside the set is coerced to "unknown" rather than
// rejected outright -- a client sending a garbled OS string shouldn't be
// dropped entirely, it should show up as a visible "unknown" bucket so the
// signals layer can surface it instead of hiding it.
var knownOS = map[string]string{
	"windows": "windows",
	"win32":   "windows",
	"macos":   "macos",
	"darwin":  "macos",
	"osx":     "macos",
	"linux":   "linux",
}

var knownRegions = map[string]string{
	"us-east": "us-east",
	"us-west": "us-west",
	"eu":      "eu",
	"eu-west": "eu",
	"apac":    "apac",
}

// ValidationError distinguishes a malformed-payload rejection (client bug,
// corrupted retry, garbage on the wire) from an internal error. The ingest
// HTTP handler maps this to a 400; anything else becomes a 500.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid field %q: %s", e.Field, e.Reason)
}

// Normalize validates a RawEvent and converts it into the canonical Event
// type. It is intentionally strict about the fields that matter for
// correctness (client_id, type, timestamp) and lenient/normalizing about the
// fields that are cosmetic-but-useful (os, region, version casing).
func Normalize(raw RawEvent, receivedAt time.Time) (Event, error) {
	clientID := strings.TrimSpace(raw.ClientID)
	if clientID == "" {
		return Event{}, &ValidationError{Field: "client_id", Reason: "must not be empty"}
	}

	evtType := EventType(strings.ToLower(strings.TrimSpace(raw.Type)))
	if !evtType.Valid() {
		return Event{}, &ValidationError{Field: "type", Reason: fmt.Sprintf("unrecognized event type %q", raw.Type)}
	}

	if strings.TrimSpace(raw.Timestamp) == "" {
		return Event{}, &ValidationError{Field: "timestamp", Reason: "must not be empty"}
	}
	ts, err := time.Parse(time.RFC3339, raw.Timestamp)
	if err != nil {
		return Event{}, &ValidationError{Field: "timestamp", Reason: "must be RFC3339, got: " + raw.Timestamp}
	}
	// Reject timestamps that are wildly in the future -- a common signal of
	// a client with a broken clock, which would otherwise poison freshness
	// calculations by looking "more current" than everything else.
	if ts.After(receivedAt.Add(5 * time.Minute)) {
		return Event{}, &ValidationError{Field: "timestamp", Reason: "more than 5m in the future"}
	}

	version := normalizeVersion(raw.Version)
	if version == "" {
		return Event{}, &ValidationError{Field: "version", Reason: "must not be empty"}
	}

	osName := normalizeOS(raw.OS)
	region := normalizeRegion(raw.Region)

	evt := Event{
		ClientID:   clientID,
		Type:       evtType,
		Cohort:     Cohort{Version: version, OS: osName, Region: region},
		Timestamp:  ts,
		ReceivedAt: receivedAt,
		SessionID:  strings.TrimSpace(raw.SessionID),
	}

	switch evtType {
	case EventCapture:
		if evt.SessionID == "" {
			return Event{}, &ValidationError{Field: "session_id", Reason: "required for capture events"}
		}
		if raw.ExpectedEventCount <= 0 {
			return Event{}, &ValidationError{Field: "expected_event_count", Reason: "must be > 0 for capture events"}
		}
		if raw.SequenceNumber < 0 || raw.SequenceNumber >= raw.ExpectedEventCount {
			return Event{}, &ValidationError{Field: "sequence_number", Reason: "must be in [0, expected_event_count)"}
		}
		evt.ExpectedEventCount = raw.ExpectedEventCount
		evt.SequenceNumber = raw.SequenceNumber
	case EventError:
		msg := strings.TrimSpace(raw.ErrorMessage)
		if msg == "" {
			return Event{}, &ValidationError{Field: "error_message", Reason: "required for error events"}
		}
		// Cap length -- clients are not trusted to bound their own payloads.
		const maxErrLen = 2048
		if len(msg) > maxErrLen {
			msg = msg[:maxErrLen]
		}
		evt.ErrorMessage = msg
	}

	return evt, nil
}

// normalizeVersion strips a leading "v" and surrounding whitespace so
// "v2.5.1", "2.5.1 ", and "2.5.1" all collapse to the same cohort label.
// This matters a lot in practice: a version-label typo or inconsistency
// between the installer and the app itself is a classic source of a signal
// silently fragmenting into two near-identical cohorts.
func normalizeVersion(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "v")
	v = strings.TrimPrefix(v, "V")
	return v
}

func normalizeOS(v string) string {
	key := strings.ToLower(strings.TrimSpace(v))
	if norm, ok := knownOS[key]; ok {
		return norm
	}
	return "unknown"
}

func normalizeRegion(v string) string {
	key := strings.ToLower(strings.TrimSpace(v))
	if norm, ok := knownRegions[key]; ok {
		return norm
	}
	return "unknown"
}
