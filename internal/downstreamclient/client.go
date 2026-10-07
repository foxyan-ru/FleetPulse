// Package downstreamclient is the client side of the call FleetPulse's
// worker pool makes to the "Workflow Mapping" stand-in service
// (cmd/downstream) during enrichment. Its success/failure/latency feeds the
// mapping-availability signal defined in docs/signals.md -- mapping
// availability is meant to measure a *downstream dependency* health, not
// FleetPulse's own uptime, so it's modeled as a genuine outbound HTTP call
// rather than an in-process stub.
package downstreamclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client calls the downstream workflow-mapping stub service.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

func New(baseURL string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: timeout},
	}
}

// MapRequest is the payload sent to the downstream stub for a capture event
// that needs its workflow mapped to a canonical action.
type MapRequest struct {
	ClientID  string `json:"client_id"`
	SessionID string `json:"session_id"`
	Sequence  int    `json:"sequence_number"`
}

// MapResponse is the downstream stub's response on success.
type MapResponse struct {
	MappingID string `json:"mapping_id"`
	Status    string `json:"status"`
}

// ErrDownstreamUnavailable is returned (wrapped) for any failure calling the
// downstream service, whether that's a non-2xx response, a timeout, or a
// connection error. The worker/signals layers only need to know
// "available" vs "not", matching how mapping availability is defined in
// docs/signals.md.
var ErrDownstreamUnavailable = fmt.Errorf("downstream workflow-mapping service unavailable")

// Map calls the downstream service's /v1/workflow-mapping endpoint. The
// caller is expected to record the outcome (success/failure + latency)
// against the mapping-availability signal regardless of how this returns.
func (c *Client) Map(ctx context.Context, req MapRequest) (MapResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return MapResponse{}, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/workflow-mapping", bytes.NewReader(body))
	if err != nil {
		return MapResponse{}, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return MapResponse{}, fmt.Errorf("%w: %v", ErrDownstreamUnavailable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return MapResponse{}, fmt.Errorf("%w: status %d: %s", ErrDownstreamUnavailable, resp.StatusCode, string(respBody))
	}

	var out MapResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return MapResponse{}, fmt.Errorf("decode response: %w", err)
	}
	return out, nil
}
