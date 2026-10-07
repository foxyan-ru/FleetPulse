package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/mstone/fleetpulse/internal/ingest"
	"github.com/mstone/fleetpulse/internal/queue"
)

// wireEvent is the JSON envelope stored as an SQS message body. It mirrors
// ingest.Event but with string timestamps, since ingest.Event isn't
// (de)serialized elsewhere and we'd rather keep an explicit wire type here
// than rely on encoding/json's default time.Time handling matching
// whatever a future refactor of ingest.Event assumes.
type wireEvent struct {
	ClientID            string `json:"client_id"`
	Type                string `json:"type"`
	Version             string `json:"version"`
	OS                  string `json:"os"`
	Region              string `json:"region"`
	Timestamp           string `json:"timestamp"`
	ReceivedAt          string `json:"received_at"`
	SessionID           string `json:"session_id,omitempty"`
	ExpectedEventCount  int    `json:"expected_event_count,omitempty"`
	SequenceNumber      int    `json:"sequence_number,omitempty"`
	ErrorMessage        string `json:"error_message,omitempty"`
}

func toWireEvent(evt ingest.Event) wireEvent {
	return wireEvent{
		ClientID:           evt.ClientID,
		Type:               string(evt.Type),
		Version:            evt.Cohort.Version,
		OS:                 evt.Cohort.OS,
		Region:             evt.Cohort.Region,
		Timestamp:          evt.Timestamp.Format(time.RFC3339Nano),
		ReceivedAt:         evt.ReceivedAt.Format(time.RFC3339Nano),
		SessionID:          evt.SessionID,
		ExpectedEventCount: evt.ExpectedEventCount,
		SequenceNumber:     evt.SequenceNumber,
		ErrorMessage:       evt.ErrorMessage,
	}
}

func fromWireEvent(w wireEvent) (ingest.Event, error) {
	ts, err := time.Parse(time.RFC3339Nano, w.Timestamp)
	if err != nil {
		return ingest.Event{}, fmt.Errorf("parse timestamp: %w", err)
	}
	receivedAt, err := time.Parse(time.RFC3339Nano, w.ReceivedAt)
	if err != nil {
		return ingest.Event{}, fmt.Errorf("parse received_at: %w", err)
	}
	return ingest.Event{
		ClientID:           w.ClientID,
		Type:               ingest.EventType(w.Type),
		Cohort:             ingest.Cohort{Version: w.Version, OS: w.OS, Region: w.Region},
		Timestamp:          ts,
		ReceivedAt:         receivedAt,
		SessionID:          w.SessionID,
		ExpectedEventCount: w.ExpectedEventCount,
		SequenceNumber:     w.SequenceNumber,
		ErrorMessage:       w.ErrorMessage,
	}, nil
}

// SQSQueue implements queue.Queue against a real (or LocalStack-emulated)
// Amazon SQS standard queue. It is the production-shaped counterpart to
// queue.InMemoryQueue -- see the package doc on internal/queue for the
// tradeoffs.
//
// Redelivery/retry semantics come directly from SQS's own visibility
// timeout and redrive policy: Nack sets the message's visibility timeout to
// zero so it's immediately eligible for redelivery, and a plain "never ack"
// (worker crash) lets the queue's configured VisibilityTimeout expire
// naturally. Messages that exceed maxReceiveCount are expected to be routed
// to a dead-letter queue by the queue's own redrive policy (configured at
// queue-creation time, not in this client) -- see docs/architecture.md for
// the LocalStack bootstrap commands that would set this up.
type SQSQueue struct {
	client            *sqs.Client
	queueURL          string
	visibilityTimeout int32
	logger            *slog.Logger
}

// NewSQSQueue constructs an SQSQueue. cfg should come from LoadLocalConfig.
// queueURL is the full SQS queue URL (from CreateQueue or GetQueueUrl) --
// bootstrapping the queue itself is out of scope for this client and is
// expected to be done by an init script (see docs/architecture.md) or
// Terraform against LocalStack.
func NewSQSQueue(cfg awssdk.Config, queueURL string, visibilityTimeout time.Duration, logger *slog.Logger) *SQSQueue {
	if logger == nil {
		logger = slog.Default()
	}
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if endpoint := Endpoint(); endpoint != "" {
			o.BaseEndpoint = awssdk.String(endpoint)
		}
	})
	return &SQSQueue{
		client:            client,
		queueURL:          queueURL,
		visibilityTimeout: int32(visibilityTimeout.Seconds()),
		logger:            logger,
	}
}

func (q *SQSQueue) Publish(ctx context.Context, evt ingest.Event) error {
	body, err := json.Marshal(toWireEvent(evt))
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	_, err = q.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    awssdk.String(q.queueURL),
		MessageBody: awssdk.String(string(body)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"event_type": {DataType: awssdk.String("String"), StringValue: awssdk.String(string(evt.Type))},
			"version":    {DataType: awssdk.String("String"), StringValue: awssdk.String(evt.Cohort.Version)},
		},
	})
	if err != nil {
		return fmt.Errorf("sqs SendMessage: %w", err)
	}
	return nil
}

func (q *SQSQueue) Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]queue.Delivery, error) {
	// SQS caps MaxNumberOfMessages at 10 per ReceiveMessage call.
	if maxMessages > 10 {
		maxMessages = 10
	}
	waitSeconds := int32(waitTime.Seconds())
	if waitSeconds > 20 {
		waitSeconds = 20 // SQS long-poll max
	}

	out, err := q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              awssdk.String(q.queueURL),
		MaxNumberOfMessages:   int32(maxMessages),
		WaitTimeSeconds:       waitSeconds,
		VisibilityTimeout:     q.visibilityTimeout,
		MessageAttributeNames: []string{"All"},
		AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameApproximateReceiveCount},
	})
	if err != nil {
		return nil, fmt.Errorf("sqs ReceiveMessage: %w", err)
	}

	deliveries := make([]queue.Delivery, 0, len(out.Messages))
	for _, m := range out.Messages {
		var w wireEvent
		if err := json.Unmarshal([]byte(awssdk.ToString(m.Body)), &w); err != nil {
			q.logger.Error("dropping malformed SQS message body", "message_id", awssdk.ToString(m.MessageId), "error", err)
			// Best-effort delete so a poison-pill message doesn't loop
			// forever; a production system would route this to a DLQ
			// instead of silently dropping it.
			_, _ = q.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      awssdk.String(q.queueURL),
				ReceiptHandle: m.ReceiptHandle,
			})
			continue
		}
		evt, err := fromWireEvent(w)
		if err != nil {
			q.logger.Error("dropping SQS message with invalid event payload", "message_id", awssdk.ToString(m.MessageId), "error", err)
			continue
		}

		deliveryCount := 1
		if raw, ok := m.Attributes[string(types.QueueAttributeNameApproximateReceiveCount)]; ok {
			if n, err := strconv.Atoi(raw); err == nil {
				deliveryCount = n
			}
		}

		deliveries = append(deliveries, &sqsDelivery{
			client:        q.client,
			queueURL:      q.queueURL,
			receiptHandle: awssdk.ToString(m.ReceiptHandle),
			msg: queue.Message{
				ID:            awssdk.ToString(m.MessageId),
				Event:         evt,
				DeliveryCount: deliveryCount,
			},
		})
	}
	return deliveries, nil
}

func (q *SQSQueue) Metrics(ctx context.Context) (queue.Metrics, error) {
	out, err := q.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: awssdk.String(q.queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		return queue.Metrics{}, fmt.Errorf("sqs GetQueueAttributes: %w", err)
	}

	backlog, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
	inFlight, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	return queue.Metrics{
		Backlog:  backlog,
		InFlight: inFlight,
		// DeadLettered would come from GetQueueAttributes on the configured
		// dead-letter queue's URL, not this queue -- omitted here since
		// DLQ provisioning is left to infra bootstrapping (see
		// docs/architecture.md).
	}, nil
}

func (q *SQSQueue) Close() error { return nil }

// sqsDelivery implements queue.Delivery for SQSQueue.
type sqsDelivery struct {
	client        *sqs.Client
	queueURL      string
	receiptHandle string
	msg           queue.Message
}

func (d *sqsDelivery) Message() queue.Message { return d.msg }

func (d *sqsDelivery) Ack(ctx context.Context) error {
	_, err := d.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      awssdk.String(d.queueURL),
		ReceiptHandle: awssdk.String(d.receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("sqs DeleteMessage: %w", err)
	}
	return nil
}

// Nack sets the message's visibility timeout to 0, making it immediately
// eligible for redelivery instead of waiting out the remainder of the
// original visibility window.
func (d *sqsDelivery) Nack(ctx context.Context) error {
	_, err := d.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          awssdk.String(d.queueURL),
		ReceiptHandle:     awssdk.String(d.receiptHandle),
		VisibilityTimeout: 0,
	})
	if err != nil {
		return fmt.Errorf("sqs ChangeMessageVisibility: %w", err)
	}
	return nil
}
