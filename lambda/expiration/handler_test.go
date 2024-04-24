package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
	"time"
)

var testIdempotencyTableName = "test-rehydration-idempotency-table"
var testEnvVars = test.NewEnvironmentVariables().With(idempotency.TableNameKey, testIdempotencyTableName)

func TestExpirationHandler(t *testing.T) {
	testEnvVars.Setenv(t)

	bucket := "rehydration-test-bucket"
	prefixToExpire := "43/1/"
	prefixToKeep := "43/11/"
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithMinIO().WithDynamoDB().Config(ctx, false)
	awsConfigFactory.Set(&awsConfig)
	defer awsConfigFactory.Set(nil)

	objectsToExpire := test.GeneratePutObjectInputs(bucket, prefixToExpire, 101)
	objectsToKeep := test.GeneratePutObjectInputs(bucket, prefixToKeep, 10)

	putObjectInputs := append(objectsToExpire, objectsToKeep...)

	s3Fixture, _ := test.NewS3Fixture(t, s3.NewFromConfig(awsConfig), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}).WithObjects(putObjectInputs...)
	defer s3Fixture.Teardown()

	now := time.Now()

	toExpireExpirationDate := now.Add(-time.Hour * 24)
	toExpireRecord := idempotency.NewRecord(prefixToExpire, idempotency.Completed).
		WithFargateTaskARN(uuid.NewString()).
		WithRehydrationLocation(fmt.Sprintf("s3://%s/%s", bucket, prefixToExpire)).
		WithExpirationDate(&toExpireExpirationDate)

	expectedRecordsPostExpiration := map[string]*idempotency.Record{}
	toKeepExpirationDate := now.Add(time.Hour * time.Duration(24*2))
	toKeepRecord := idempotency.NewRecord(prefixToKeep, idempotency.Completed).
		WithFargateTaskARN(uuid.NewString()).
		WithRehydrationLocation(fmt.Sprintf("s3://%s/%s", bucket, prefixToKeep)).
		WithExpirationDate(&toKeepExpirationDate)
	expectedRecordsPostExpiration[toKeepRecord.ID] = toKeepRecord

	inProgressRecord := idempotency.NewRecord("43/17/", idempotency.InProgress).WithFargateTaskARN(uuid.NewString())
	expectedRecordsPostExpiration[inProgressRecord.ID] = inProgressRecord

	dyDBFixture := test.NewDynamoDBFixture(t, awsConfig, test.IdempotencyCreateTableInput(testIdempotencyTableName)).
		WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, toKeepRecord, toExpireRecord, inProgressRecord)...)
	defer dyDBFixture.Teardown()

	// First Run should find a rehydration to expire
	resp, err := ExpirationHandler(ctx, events.APIGatewayV2HTTPRequest{})
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Empty(t, resp.Body)

	handlerAfterFirst := handler

	s3Fixture.AssertPrefixEmpty(bucket, prefixToExpire)
	for _, expectedKept := range objectsToKeep {
		key := aws.ToString(expectedKept.Key)
		assert.True(t, s3Fixture.ObjectExists(bucket, key))
	}

	// The expired record should be gone, leaving those that were not expired unchanged
	allRecordItems := dyDBFixture.Scan(ctx, testIdempotencyTableName)
	require.Len(t, allRecordItems, len(expectedRecordsPostExpiration))
	for _, item := range allRecordItems {
		actual, err := idempotency.FromItem(item)
		require.NoError(t, err)
		if assert.Contains(t, expectedRecordsPostExpiration, actual.ID) {
			expected := expectedRecordsPostExpiration[actual.ID]
			if expected.ExpirationDate == nil {
				assert.Nil(t, actual.ExpirationDate)
			} else {
				assert.True(t, expected.ExpirationDate.Equal(*actual.ExpirationDate))
			}
			assert.Equal(t, expected.ID, actual.ID)
			assert.Equal(t, expected.RehydrationLocation, actual.RehydrationLocation)
			assert.Equal(t, expected.FargateTaskARN, actual.FargateTaskARN)
			assert.Equal(t, expected.Status, actual.Status)

		}
	}

	// Second Run should not find any rehydration to expire
	resp2, err := ExpirationHandler(ctx, events.APIGatewayV2HTTPRequest{})
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp2.StatusCode)
	assert.Empty(t, resp2.Body)

	for _, expectedKept := range objectsToKeep {
		key := aws.ToString(expectedKept.Key)
		assert.True(t, s3Fixture.ObjectExists(bucket, key))
	}

	// The non-expired records should be unchanged
	allRecordItems2 := dyDBFixture.Scan(ctx, testIdempotencyTableName)
	require.Len(t, allRecordItems2, len(expectedRecordsPostExpiration))
	for _, item := range allRecordItems2 {
		actual, err := idempotency.FromItem(item)
		require.NoError(t, err)
		if assert.Contains(t, expectedRecordsPostExpiration, actual.ID) {
			expected := expectedRecordsPostExpiration[actual.ID]
			if expected.ExpirationDate == nil {
				assert.Nil(t, actual.ExpirationDate)
			} else {
				assert.True(t, expected.ExpirationDate.Equal(*actual.ExpirationDate))
			}
			assert.Equal(t, expected.ID, actual.ID)
			assert.Equal(t, expected.RehydrationLocation, actual.RehydrationLocation)
			assert.Equal(t, expected.FargateTaskARN, actual.FargateTaskARN)
			assert.Equal(t, expected.Status, actual.Status)

		}
	}

	// Expiration will only run once per day, so the same Lambda won't really be reused, but check that we reused the
	// same expiration.Handler just to see if the initializeHandler() function is working as intended.
	assert.Same(t, handlerAfterFirst, handler)

}
