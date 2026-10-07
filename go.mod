module github.com/mstone/fleetpulse

go 1.24

// NOTE: this module was authored by hand as part of a portfolio scaffold.
// `go mod tidy` / `go mod download` have NOT been run against it, so the
// require block below reflects plausible, recent-as-of-authoring versions
// rather than a resolved, verified dependency graph. Running `go mod tidy`
// is the first thing to do before this module will actually build.

require (
	github.com/aws/aws-sdk-go-v2 v1.32.6
	github.com/aws/aws-sdk-go-v2/config v1.28.6
	github.com/aws/aws-sdk-go-v2/credentials v1.17.47
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.17.6
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.37.1
	github.com/aws/aws-sdk-go-v2/service/sqs v1.37.2
	github.com/google/uuid v1.6.0
	github.com/prometheus/client_golang v1.20.5
)

require (
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.10.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.24.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.28.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.2 // indirect
	github.com/aws/smithy-go v1.22.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)
