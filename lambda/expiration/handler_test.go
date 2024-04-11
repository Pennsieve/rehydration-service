package main

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

var testIdempotencyTableName = "test-rehydration-idempotency-table"
var testEnvVars = test.NewEnvironmentVariables().With(idempotency.TableNameKey, testIdempotencyTableName)

func TestExpirationHandler(t *testing.T) {
	// Just a smoke test to make sure everything is connected up correctly
	// Most testing should be done in the expiration pacakge of the shared module
	testEnvVars.Setenv(t)

	ctx := context.Background()

	awsConfig := test.NewAWSEndpoints(t).WithMinIO().WithDynamoDB().Config(ctx, false)
	awsConfigFactory.Set(&awsConfig)
	defer awsConfigFactory.Set(nil)

	dyDBFixture := test.NewDynamoDBFixture(t, awsConfig, test.IdempotencyCreateTableInput(testIdempotencyTableName))
	defer dyDBFixture.Teardown()

	rehydrationBucket := "test-rehydration-bucket"
	s3Fixture := test.NewS3Fixture(t, s3.NewFromConfig(awsConfig), &s3.CreateBucketInput{Bucket: aws.String(rehydrationBucket)})
	defer s3Fixture.Teardown()

	resp, err := ExpirationHandler(ctx, events.APIGatewayV2HTTPRequest{})
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Empty(t, resp.Body)

	allRecordItems := dyDBFixture.Scan(ctx, testIdempotencyTableName)
	assert.Empty(t, allRecordItems)

	s3Fixture.AssertBucketEmpty(rehydrationBucket)
}
