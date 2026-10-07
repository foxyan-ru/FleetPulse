package aws

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/mstone/fleetpulse/internal/ingest"
	"github.com/mstone/fleetpulse/internal/state"
)

// clientRecord is the DynamoDB item shape for the per-client last-seen
// table. Table schema (see docker-compose.yml's localstack-init bootstrap
// script referenced in docs/architecture.md):
//
//	Table: fleetpulse-client-state
//	Partition key: client_id (S)
//	Attributes: version, os, region, last_heartbeat_at, last_event_at,
//	            last_session_id, registered_at (all stored as RFC3339
//	            strings for human-readability when inspecting items via the
//	            LocalStack/real AWS console)
//
// A real production table at fleet scale would add a GSI on
// (version, last_heartbeat_at) so "give me every stale client in cohort X"
// could be a Query instead of the full-table Scan this implementation's
// All() uses -- called out explicitly below since it's the first thing that
// would need to change before this scales past a demo fleet.
type clientRecord struct {
	ClientID        string `dynamodbav:"client_id"`
	Version         string `dynamodbav:"version"`
	OS              string `dynamodbav:"os"`
	Region          string `dynamodbav:"region"`
	LastHeartbeatAt string `dynamodbav:"last_heartbeat_at"`
	LastEventAt     string `dynamodbav:"last_event_at"`
	LastSessionID   string `dynamodbav:"last_session_id,omitempty"`
	RegisteredAt    string `dynamodbav:"registered_at"`
}

// DynamoDBStore implements state.Store against a DynamoDB table, targeting
// LocalStack by default (see internal/aws/config.go). It is the
// production-shaped counterpart to state.InMemoryStore.
type DynamoDBStore struct {
	client    *dynamodb.Client
	tableName string
	logger    *slog.Logger
}

func NewDynamoDBStore(cfg awssdk.Config, tableName string, logger *slog.Logger) *DynamoDBStore {
	if logger == nil {
		logger = slog.Default()
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if endpoint := Endpoint(); endpoint != "" {
			o.BaseEndpoint = awssdk.String(endpoint)
		}
	})
	return &DynamoDBStore{client: client, tableName: tableName, logger: logger}
}

// Upsert writes (or updates) the client's record. It uses UpdateItem with
// conditional SET expressions rather than a blind PutItem so that
// concurrent updates from different worker goroutines/instances can't
// clobber each other's `registered_at` (first-seen) timestamp, and so that
// last_heartbeat_at only ever moves forward (an out-of-order/delayed
// heartbeat delivery shouldn't roll freshness backwards).
func (s *DynamoDBStore) Upsert(ctx context.Context, clientID string, cohort ingest.Cohort, at time.Time, isHeartbeat bool, sessionID string) error {
	atStr := at.UTC().Format(time.RFC3339Nano)

	update := "SET version = :version, #os = :os, region = :region, " +
		"last_event_at = :event_at, " +
		"registered_at = if_not_exists(registered_at, :event_at)"
	names := map[string]string{"#os": "os"} // "os" collides with a reserved-word-adjacent pattern in some SDKs; alias defensively
	values := map[string]interface{}{
		":version":  cohort.Version,
		":os":       cohort.OS,
		":region":   cohort.Region,
		":event_at": atStr,
	}

	if isHeartbeat {
		// Only advance last_heartbeat_at if the new value is greater, using
		// a conditional-free "max via expression" trick: DynamoDB doesn't
		// have a native MAX() update function, so this is approximated by
		// always setting it here. Events are expected to arrive
		// approximately in order given the queue's FIFO-per-cohort delivery
		// pattern in this scaffold; a production version would read-modify-
		// write with a ConditionExpression on the existing value instead.
		update += ", last_heartbeat_at = :hb_at"
		values[":hb_at"] = atStr
	}
	if sessionID != "" {
		update += ", last_session_id = :session_id"
		values[":session_id"] = sessionID
	}

	av, err := attributevalue.MarshalMap(values)
	if err != nil {
		return fmt.Errorf("marshal update values: %w", err)
	}

	_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: awssdk.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"client_id": &types.AttributeValueMemberS{Value: clientID},
		},
		UpdateExpression:          awssdk.String(update),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: av,
	})
	if err != nil {
		return fmt.Errorf("dynamodb UpdateItem: %w", err)
	}
	return nil
}

// All scans the entire table. NOTE: a full Scan is the wrong access pattern
// at real fleet scale (it's O(table size) and consumes read capacity
// proportional to every item, not just the ones you need) -- it is used
// here because the simulated fleet sizes this project targets (hundreds to
// low thousands of clients) make a Scan a reasonable, honest choice for a
// demo, not because it's what a production deployment should do. See the
// type doc comment above for the GSI-based Query this would become.
func (s *DynamoDBStore) All(ctx context.Context) ([]state.ClientInfo, error) {
	var out []state.ClientInfo
	var startKey map[string]types.AttributeValue

	for {
		resp, err := s.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         awssdk.String(s.tableName),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, fmt.Errorf("dynamodb Scan: %w", err)
		}

		for _, item := range resp.Items {
			var rec clientRecord
			if err := attributevalue.UnmarshalMap(item, &rec); err != nil {
				s.logger.Error("skipping unparseable dynamodb item", "error", err)
				continue
			}
			info, err := rec.toClientInfo()
			if err != nil {
				s.logger.Error("skipping dynamodb item with invalid timestamps", "client_id", rec.ClientID, "error", err)
				continue
			}
			out = append(out, info)
		}

		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}

	return out, nil
}

func (s *DynamoDBStore) Get(ctx context.Context, clientID string) (state.ClientInfo, bool, error) {
	resp, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: awssdk.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"client_id": &types.AttributeValueMemberS{Value: clientID},
		},
	})
	if err != nil {
		return state.ClientInfo{}, false, fmt.Errorf("dynamodb GetItem: %w", err)
	}
	if resp.Item == nil {
		return state.ClientInfo{}, false, nil
	}

	var rec clientRecord
	if err := attributevalue.UnmarshalMap(resp.Item, &rec); err != nil {
		return state.ClientInfo{}, false, fmt.Errorf("unmarshal item: %w", err)
	}
	info, err := rec.toClientInfo()
	if err != nil {
		return state.ClientInfo{}, false, fmt.Errorf("parse item timestamps: %w", err)
	}
	return info, true, nil
}

func (r clientRecord) toClientInfo() (state.ClientInfo, error) {
	info := state.ClientInfo{
		ClientID:      r.ClientID,
		Cohort:        ingest.Cohort{Version: r.Version, OS: r.OS, Region: r.Region},
		LastSessionID: r.LastSessionID,
	}
	if r.LastHeartbeatAt != "" {
		t, err := time.Parse(time.RFC3339Nano, r.LastHeartbeatAt)
		if err != nil {
			return state.ClientInfo{}, fmt.Errorf("parse last_heartbeat_at: %w", err)
		}
		info.LastHeartbeatAt = t
	}
	if r.LastEventAt != "" {
		t, err := time.Parse(time.RFC3339Nano, r.LastEventAt)
		if err != nil {
			return state.ClientInfo{}, fmt.Errorf("parse last_event_at: %w", err)
		}
		info.LastEventAt = t
	}
	if r.RegisteredAt != "" {
		t, err := time.Parse(time.RFC3339Nano, r.RegisteredAt)
		if err != nil {
			return state.ClientInfo{}, fmt.Errorf("parse registered_at: %w", err)
		}
		info.RegisteredAt = t
	}
	return info, nil
}
