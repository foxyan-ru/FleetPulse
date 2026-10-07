// Package state defines the "last-seen per client" store that backs the
// heartbeat-freshness signal. Like internal/queue, it has two
// implementations behind one interface:
//   - InMemoryStore (this package): a mutex-guarded map, fine for a single
//     server process handling the simulated fleet.
//   - internal/aws.DynamoDBStore: a real (structured, not run) DynamoDB
//     implementation for the AWS-shaped deployment, keyed by client_id, with
//     cohort attributes as a GSI-able projection for future per-cohort
//     queries.
package state

import (
	"context"
	"sync"
	"time"

	"github.com/mstone/fleetpulse/internal/ingest"
)

// ClientInfo is the durable "what do we know about this client right now"
// record.
type ClientInfo struct {
	ClientID          string
	Cohort            ingest.Cohort
	LastHeartbeatAt   time.Time
	LastEventAt       time.Time // most recent event of any type
	LastSessionID     string
	RegisteredAt      time.Time
}

// Store is the interface the signals package and worker pool depend on.
type Store interface {
	// Upsert records that a client was observed, creating the record if it
	// doesn't already exist. isHeartbeat distinguishes a heartbeat-driven
	// update (which advances LastHeartbeatAt) from a capture/error event
	// (which only advances LastEventAt) -- heartbeat freshness is defined
	// specifically in terms of heartbeats, not "any traffic from the
	// client", so the two need to stay distinguishable.
	Upsert(ctx context.Context, clientID string, cohort ingest.Cohort, at time.Time, isHeartbeat bool, sessionID string) error

	// All returns every known client record. Used by the signals computer
	// to calculate heartbeat freshness per cohort. For a real fleet at
	// scale this would need to be a paginated/streaming scan (see the
	// DynamoDB implementation's comments); it's a fine tradeoff for the
	// simulated fleet sizes this project targets.
	All(ctx context.Context) ([]ClientInfo, error)

	// Get returns a single client's record.
	Get(ctx context.Context, clientID string) (ClientInfo, bool, error)
}

// InMemoryStore is a mutex-guarded map implementation of Store.
type InMemoryStore struct {
	mu      sync.RWMutex
	clients map[string]ClientInfo
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{clients: make(map[string]ClientInfo)}
}

func (s *InMemoryStore) Upsert(ctx context.Context, clientID string, cohort ingest.Cohort, at time.Time, isHeartbeat bool, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, exists := s.clients[clientID]
	if !exists {
		info = ClientInfo{ClientID: clientID, RegisteredAt: at}
	}
	info.Cohort = cohort
	if at.After(info.LastEventAt) {
		info.LastEventAt = at
	}
	if isHeartbeat && at.After(info.LastHeartbeatAt) {
		info.LastHeartbeatAt = at
	}
	if sessionID != "" {
		info.LastSessionID = sessionID
	}
	s.clients[clientID] = info
	return nil
}

func (s *InMemoryStore) All(ctx context.Context) ([]ClientInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ClientInfo, 0, len(s.clients))
	for _, info := range s.clients {
		out = append(out, info)
	}
	return out, nil
}

func (s *InMemoryStore) Get(ctx context.Context, clientID string) (ClientInfo, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.clients[clientID]
	return info, ok, nil
}
