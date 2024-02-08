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
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	awslogging "github.com/aws/smithy-go/logging"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

type AWSEndpointMap map[string]aws.Endpoint

func NewAwsEndpointMap() AWSEndpointMap {
	minioURL := os.Getenv("MINIO_URL")
	dynamodbURL := os.Getenv("DYNAMODB_URL")
	return map[string]aws.Endpoint{
		s3.ServiceID:       {URL: minioURL, HostnameImmutable: true},
		dynamodb.ServiceID: {URL: dynamodbURL},
	}
}

func (m AWSEndpointMap) WithSQS(sqsURL string) AWSEndpointMap {
	m[sqs.ServiceID] = aws.Endpoint{URL: sqsURL}
	return m
}

func (m AWSEndpointMap) WithECS(ecsURL string) AWSEndpointMap {
	m[ecs.ServiceID] = aws.Endpoint{URL: ecsURL}
	return m
}
func GetTestAWSConfig(t *testing.T, testEndpoints AWSEndpointMap, logRequests bool) aws.Config {
	awsKey := os.Getenv("TEST_AWS_KEY")
	awsSecret := os.Getenv("TEST_AWS_SECRET")
	optFns := []func(options *config.LoadOptions) error{
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(awsKey, awsSecret, "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if endpoint, ok := testEndpoints[service]; ok {
				return endpoint, nil
			}
			return aws.Endpoint{}, fmt.Errorf("unknown test endpoint requested for service: %s", service)
		})),
	}
	if logRequests {
		awsLogger := awslogging.NewStandardLogger(log.Writer())
		optFns = append(optFns, config.WithLogger(awsLogger), config.WithClientLogMode(aws.LogRequestWithBody))
	}
	awsConfig, err := config.LoadDefaultConfig(context.Background(), optFns...)
	if err != nil {
		assert.FailNow(t, "error creating AWS config", err)
	}
	return awsConfig
}
