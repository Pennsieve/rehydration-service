package expiration

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"github.com/pennsieve/rehydration-service/shared/s3cleaner"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestHandler_Handle(t *testing.T) {
	idempotencyTable := "idempotency-test-table"
	bucket := "rehydration-test-bucket"
	prefixToExpire := "43/1/"
	prefixToKeep := "43/11/"
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithMinIO().WithDynamoDB().Config(ctx, false)
	s3Client := s3.NewFromConfig(awsConfig)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)

	objectsToExpire := test.GeneratePutObjectInputs(bucket, prefixToExpire, 101)
	objectsToKeep := test.GeneratePutObjectInputs(bucket, prefixToKeep, 10)

	putObjectInputs := append(objectsToExpire, objectsToKeep...)

	s3Fixture, _ := test.NewS3Fixture(t, s3Client, &s3.CreateBucketInput{
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

	dyDBFixture := test.NewDynamoDBFixture(t, awsConfig, test.IdempotencyCreateTableInput(idempotencyTable)).
		WithItems(test.ItemersToPutItemInputs(t, idempotencyTable, toKeepRecord, toExpireRecord, inProgressRecord)...)
	defer dyDBFixture.Teardown()

	logger := logging.Default
	cleaner, err := s3cleaner.NewCleaner(s3Client, s3cleaner.MaxCleanBatch)
	require.NoError(t, err)
	handler := Handler{
		idempotencyStore: idempotency.NewStore(dyDBClient, logger, idempotencyTable),
		cleaner:          cleaner,
		logger:           logger,
	}
	err = handler.Handle(ctx)
	require.NoError(t, err)

	s3Fixture.AssertPrefixEmpty(bucket, prefixToExpire)
	for _, expectedKept := range objectsToKeep {
		key := aws.ToString(expectedKept.Key)
		assert.True(t, s3Fixture.ObjectExists(bucket, key))
	}

	// The expired record should be gone, leaving those that were not expired unchanged
	allRecordItems := dyDBFixture.Scan(ctx, idempotencyTable)
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

}

func TestParseRehydrationLocation(t *testing.T) {
	expectedBucket := "test-rehydration-bucket"
	expectedPrefix := "14/7/"
	rehydrationLocation := fmt.Sprintf("s3://%s/%s", expectedBucket, expectedPrefix)
	parsed, err := parseRehydrationLocation(rehydrationLocation)
	require.NoError(t, err)
	assert.Equal(t, expectedBucket, parsed.bucket)
	assert.Equal(t, expectedPrefix, parsed.prefix)
}
