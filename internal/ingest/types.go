// Package ingest defines the wire format that simulated fleet clients speak,
// plus the normalization/validation logic that turns an untrusted HTTP
// payload into the canonical Event type the rest of the system operates on.
//
// This is the "schema boundary" of FleetPulse: everything upstream of
// normalization is untrusted input from a fleet we don't control the
// internals of (real desktop apps / browser extensions would be exactly
// this untrustworthy — wrong clocks, stale versions, malformed retries).
package ingest

import "time"

// EventType enumerates the kinds of signals a client can phone home with.
type EventType string

const (
	EventHeartbeat EventType = "heartbeat"
	EventCapture   EventType = "capture"
	EventError     EventType = "error"
)

// Valid reports whether t is one of the known event types.
func (t EventType) Valid() bool {
	switch t {
	case EventHeartbeat, EventCapture, EventError:
		return true
	default:
		return false
	}
}

// Cohort identifies the slice of the fleet a client belongs to. This is the
// label set the "silent cohort" demo scenario keys off of: version is the
// dimension we kill in the simulator, but os/region are carried through so
// signals can be sliced along any of them.
type Cohort struct {
	Version string `json:"version"`
	OS      string `json:"os"`
	Region  string `json:"region"`
}

// Key returns a stable string key for use as a map key / metric label
// combination, e.g. "2.5.1|windows|us-east".
func (c Cohort) Key() string {
	return c.Version + "|" + c.OS + "|" + c.Region
}

// RawEvent is the JSON shape accepted over the wire from a client. Every
// field is a string/plain type deliberately -- we do NOT trust the client to
// send a well-typed timestamp, a recognized OS name, or a canonical version
// string, and normalization is where we reconcile that.
type RawEvent struct {
	ClientID  string `json:"client_id"`
	Type      string `json:"type"`
	Version   string `json:"version"`
	OS        string `json:"os"`
	Region    string `json:"region"`
	Timestamp string `json:"timestamp"` // RFC3339
	SessionID string `json:"session_id,omitempty"`

	// ExpectedEventCount / SequenceNumber are only meaningful for
	// EventCapture: they let capture-completeness be computed as
	// "distinct sequence numbers received for a session" / "expected".
	ExpectedEventCount int `json:"expected_event_count,omitempty"`
	SequenceNumber      int `json:"sequence_number,omitempty"`

	// ErrorMessage is only meaningful for EventError.
	ErrorMessage string `json:"error_message,omitempty"`
}

// Event is the normalized, validated form of a RawEvent. This is what
// crosses the ingest -> queue -> worker boundary.
type Event struct {
	ClientID  string
	Type      EventType
	Cohort    Cohort
	Timestamp time.Time // client-asserted event time, after parsing
	ReceivedAt time.Time // server-observed receive time

	SessionID           string
	ExpectedEventCount  int
	SequenceNumber      int
	ErrorMessage        string
}
