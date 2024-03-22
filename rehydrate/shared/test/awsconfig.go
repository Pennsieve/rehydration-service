package test

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	awslogging "github.com/aws/smithy-go/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log"
	"os"
)

type AWSEndpoints struct {
	testingT            require.TestingT
	serviceIDToEndpoint map[string]aws.Endpoint
}

func NewAWSEndpoints(t require.TestingT) *AWSEndpoints {
	return &AWSEndpoints{
		testingT:            t,
		serviceIDToEndpoint: map[string]aws.Endpoint{},
	}
}

func (e *AWSEndpoints) WithMinIO() *AWSEndpoints {
	minioURL := e.requiredEnvVar("MINIO_URL")
	e.serviceIDToEndpoint[s3.ServiceID] = aws.Endpoint{URL: minioURL, HostnameImmutable: true}
	return e
}

func (e *AWSEndpoints) WithDynamoDB() *AWSEndpoints {
	dynamodbURL := e.requiredEnvVar("DYNAMODB_URL")
	e.serviceIDToEndpoint[dynamodb.ServiceID] = aws.Endpoint{URL: dynamodbURL}
	return e
}

func (e *AWSEndpoints) WithSES(sesURL string) *AWSEndpoints {
	e.serviceIDToEndpoint[ses.ServiceID] = aws.Endpoint{URL: sesURL}
	return e
}

func (e *AWSEndpoints) WithECS(ecsURL string) *AWSEndpoints {
	e.serviceIDToEndpoint[ecs.ServiceID] = aws.Endpoint{URL: ecsURL}
	return e
}

func (e *AWSEndpoints) requiredEnvVar(key string) string {
	value, ok := os.LookupEnv(key)
	require.Truef(e.testingT, ok, "required environment variable %q is not set", key)
	require.NotEmptyf(e.testingT, value, "required environment variable %q is empty", key)
	return value
}

func (e *AWSEndpoints) Config(ctx context.Context, logRequests bool) aws.Config {
	awsKey := os.Getenv("TEST_AWS_KEY")
	awsSecret := os.Getenv("TEST_AWS_SECRET")
	optFns := []func(options *config.LoadOptions) error{
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(awsKey, awsSecret, "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if endpoint, ok := e.serviceIDToEndpoint[service]; ok {
				return endpoint, nil
			}
			return aws.Endpoint{}, fmt.Errorf("no test endpoint has been set for AWS serviceID: %s", service)
		})),
	}
	if logRequests {
		awsLogger := awslogging.NewStandardLogger(log.Writer())
		optFns = append(optFns, config.WithLogger(awsLogger), config.WithClientLogMode(aws.LogRequestWithBody))
	}
	awsConfig, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		assert.FailNow(e.testingT, "error creating AWS config", err)
	}
	return awsConfig
}
