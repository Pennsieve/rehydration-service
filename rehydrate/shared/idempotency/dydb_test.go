package idempotency

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var testIdempotencyTableName = "test-idempotency-table"

func TestStore_PutRecord(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := NewStore(dyDBClient, logging.Default, testIdempotencyTableName)
	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName))
	defer dyDB.Teardown()

	record := Record{
		ID:                  "1/2/",
		RehydrationLocation: "bucket/1/2/",
		Status:              InProgress,
	}
	err := store.PutRecord(ctx, record)
	require.NoError(t, err)

	err = store.PutRecord(ctx, record)
	require.Error(t, err)
	var alreadyExistsError *RecordAlreadyExistsError
	require.ErrorAs(t, err, &alreadyExistsError)
	require.Nil(t, alreadyExistsError.UnmarshallingError)
	require.Equal(t, record, *alreadyExistsError.Existing)
}

func TestStore_GetRecord(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

	record := Record{
		ID:                  "1/2/",
		RehydrationLocation: "bucket/1/2/",
		Status:              Completed,
	}

	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName)).WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, &record)...)
	defer dyDB.Teardown()

	actual, err := store.GetRecord(ctx, record.ID)
	require.NoError(t, err)
	require.NotNil(t, actual)
	require.Equal(t, record, *actual)

	actual, err = store.GetRecord(ctx, "non-existent")
	require.NoError(t, err)
	require.Nil(t, actual)
}

func TestStore_UpdateRecord(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

	record := Record{
		ID:     "1/2/",
		Status: InProgress,
	}

	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName)).WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, &record)...)
	defer dyDB.Teardown()

	updatedLocation := "bucket/1/2/"
	updatedStatus := Completed
	record.RehydrationLocation = updatedLocation
	record.Status = updatedStatus

	err := store.UpdateRecord(ctx, record)
	require.NoError(t, err)

	scanAll := dyDB.Scan(ctx, testIdempotencyTableName)
	require.Len(t, scanAll, 1)
	scanned, err := FromItem(scanAll[0])
	require.NoError(t, err)
	require.Equal(t, record.ID, scanned.ID)
	require.Equal(t, updatedLocation, scanned.RehydrationLocation)
	require.Equal(t, updatedStatus, scanned.Status)
}

func TestStore_SetTaskARN(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

	record := Record{
		ID:     "1/2/",
		Status: InProgress,
	}

	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName)).WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, &record)...)
	defer dyDB.Teardown()

	taskARN := "arn:aws:ecs:test:test:test"
	err := store.SetTaskARN(ctx, record.ID, taskARN)
	require.NoError(t, err)

	scanAll := dyDB.Scan(ctx, testIdempotencyTableName)
	require.Len(t, scanAll, 1)
	scanned, err := FromItem(scanAll[0])
	require.NoError(t, err)
	require.Equal(t, record.ID, scanned.ID)
	require.Equal(t, record.RehydrationLocation, scanned.RehydrationLocation)
	require.Equal(t, record.Status, scanned.Status)
	require.Equal(t, taskARN, scanned.FargateTaskARN)
}

func TestStore_DeleteRecord(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

	recordToDelete := Record{
		ID:                  "1/2/",
		RehydrationLocation: "bucket/1/2/",
		Status:              Expired,
	}

	recordToKeep := Record{
		ID:                  "4/9/",
		RehydrationLocation: "bucket/4/9/",
		Status:              Completed,
	}

	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName)).WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, &recordToDelete, &recordToKeep)...)
	defer dyDB.Teardown()

	err := store.DeleteRecord(ctx, recordToDelete.ID)
	require.NoError(t, err)

	scanAll := dyDB.Scan(ctx, testIdempotencyTableName)
	require.Len(t, scanAll, 1)
	scanned, err := FromItem(scanAll[0])
	require.NoError(t, err)
	require.Equal(t, recordToKeep, *scanned)
}

func TestStore_ExpireRecord(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

	recordToExpire := Record{
		ID:                  "1/2/",
		RehydrationLocation: "bucket/1/2/",
		Status:              InProgress,
	}

	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName)).WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, &recordToExpire)...)
	defer dyDB.Teardown()

	err := store.ExpireRecord(ctx, recordToExpire.ID)
	require.NoError(t, err)

	scanAll := dyDB.Scan(ctx, testIdempotencyTableName)
	require.Len(t, scanAll, 1)
	scanned, err := FromItem(scanAll[0])
	require.NoError(t, err)
	assert.Equal(t, Expired, scanned.Status)
	assert.Equal(t, recordToExpire.ID, scanned.ID)
	assert.Equal(t, recordToExpire.RehydrationLocation, scanned.RehydrationLocation)
	assert.Equal(t, recordToExpire.FargateTaskARN, scanned.FargateTaskARN)

	nonExistentRecordID := "999/9/"
	err = store.ExpireRecord(ctx, nonExistentRecordID)
	var recordNotFound *RecordDoesNotExistsError
	if assert.ErrorAs(t, err, &recordNotFound) {
		assert.Equal(t, nonExistentRecordID, recordNotFound.RecordID)
	}
}

func createIdempotencyTableInput(tableName string) *dynamodb.CreateTableInput {
	return test.IdempotencyCreateTableInput(tableName, KeyAttrName)
}
