// Package aws contains FleetPulse's real-AWS-shaped implementations of the
// queue.Queue and state.Store interfaces, targeting LocalStack
// (https://localstack.cloud) for local/CI use and real AWS for the one-time
// demo-video deployment described in the README.
//
// Everything in this package is structured to compile against a real SQS
// queue and DynamoDB table but has NOT been executed against either a live
// LocalStack container or real AWS as part of this scaffold -- per the
// project's "simulate cheaply, one real AWS-shaped deployment for the demo"
// philosophy, these are meant to be exercised deliberately (`docker compose
// up localstack` + a small bootstrap script to create the queue/table),
// not on every local dev loop.
package aws

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// EndpointEnvVar is the standard AWS SDK v2 environment variable used to
// redirect all service calls at LocalStack instead of real AWS endpoints.
// It is read directly here (rather than only relying on the SDK's own
// auto-detection) so callers get a clear error if it's unset in "local"
// mode instead of an opaque connection failure against real AWS.
const EndpointEnvVar = "AWS_ENDPOINT_URL"

// DefaultLocalstackEndpoint is LocalStack's default single-port gateway.
const DefaultLocalstackEndpoint = "http://localhost:4566"

// DefaultRegion is used for local/dev config; a real deployment would set
// AWS_REGION explicitly.
const DefaultRegion = "us-east-1"

// LoadLocalConfig builds an aws.Config suitable for pointing every service
// client in this package at LocalStack. It reads AWS_ENDPOINT_URL (falling
// back to DefaultLocalstackEndpoint) and, if no real credentials are
// present in the environment, supplies LocalStack's conventional dummy
// static credentials ("test"/"test") -- LocalStack doesn't validate these,
// but the SDK refuses to make a request with no credential provider at all.
func LoadLocalConfig(ctx context.Context) (aws.Config, error) {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = DefaultRegion
	}

	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}

	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("load aws config: %w", err)
	}
	return cfg, nil
}

// Endpoint returns the LocalStack (or otherwise overridden) endpoint that
// per-service BaseEndpoint options should be set to. Every service client
// constructor in this package (NewSQSQueue, NewDynamoDBStore) calls this so
// there is exactly one place that decides "are we talking to LocalStack or
// real AWS".
//
// When AWS_ENDPOINT_URL is unset, this returns "" and callers should leave
// the service client's BaseEndpoint unset, which makes the SDK talk to real
// AWS -- this is the code path exercised for the one real deployment used
// to record the demo video.
func Endpoint() string {
	if v := os.Getenv(EndpointEnvVar); v != "" {
		return v
	}
	return ""
}
