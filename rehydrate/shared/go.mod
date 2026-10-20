module github.com/pennsieve/rehydration-service/shared

// shared is a module for code that is shared between the service lambda and the fargate task
// It lives here so that it can be referenced in the Dockerfile for the fargate task

go 1.27.2

require (
	github.com/aws/aws-lambda-go v1.46.0
	github.com/aws/aws-sdk-go-v2 v1.47.3
	github.com/aws/aws-sdk-go-v2/config v1.33.9
	github.com/aws/aws-sdk-go-v2/credentials v1.20.9
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.21.13
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.10.1
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.71.0
	github.com/aws/aws-sdk-go-v2/service/ecs v1.101.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.114.3
	github.com/aws/aws-sdk-go-v2/service/ses v1.42.4
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.5
	github.com/aws/smithy-go v1.28.5
	github.com/google/uuid v1.6.0
	github.com/pennsieve/email-service v1.0.0
	github.com/pennsieve/pennsieve-go v1.10.0
	github.com/stretchr/testify v1.10.0
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.22 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.20.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.46.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.13.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.10.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.38.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.43.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.51.4 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
