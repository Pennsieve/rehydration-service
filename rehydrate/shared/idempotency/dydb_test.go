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

func TestDyDBStore_LockRecordForExpiration(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

	recordToLock := Record{
		ID:                  "1/2/",
		RehydrationLocation: "bucket/1/2/",
		Status:              Completed,
	}

	conditionFailRecord := Record{
		ID:                  "13/2/",
		RehydrationLocation: "bucket/13/2/",
		Status:              InProgress,
	}

	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName)).WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, &recordToLock, &conditionFailRecord)...)
	defer dyDB.Teardown()

	err := store.LockRecordForExpiration(ctx, recordToLock.ID)
	require.NoError(t, err)

	err = store.LockRecordForExpiration(ctx, conditionFailRecord.ID)
	// Error message should tell caller expected status and the actual status
	require.ErrorContains(t, err, string(conditionFailRecord.Status))
	require.ErrorContains(t, err, string(Completed))

	scanAll := dyDB.Scan(ctx, testIdempotencyTableName)
	require.Len(t, scanAll, 2)
	for _, item := range scanAll {
		actual, err := FromItem(item)
		require.NoError(t, err)
		assert.True(t, actual.ID == recordToLock.ID || actual.ID == conditionFailRecord.ID)
		if actual.ID == recordToLock.ID {
			assert.Equal(t, Expired, actual.Status)
			assert.Equal(t, recordToLock.RehydrationLocation, actual.RehydrationLocation)
			assert.Equal(t, recordToLock.FargateTaskARN, actual.FargateTaskARN)
		} else if actual.ID == conditionFailRecord.ID {
			assert.Equal(t, conditionFailRecord.Status, actual.Status)
			assert.Equal(t, conditionFailRecord.RehydrationLocation, actual.RehydrationLocation)
			assert.Equal(t, conditionFailRecord.FargateTaskARN, actual.FargateTaskARN)
		}

	}

	nonExistentRecordID := "999/9/"
	err = store.LockRecordForExpiration(ctx, nonExistentRecordID)
	var recordNotFound *RecordDoesNotExistsError
	if assert.ErrorAs(t, err, &recordNotFound) {
		assert.Equal(t, nonExistentRecordID, recordNotFound.RecordID)
	}
}

func TestDyDBStore_UnlockRecordForExpiration(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

	recordToUnlock := Record{
		ID:                  "1/2/",
		RehydrationLocation: "bucket/1/2/",
		Status:              Expired,
	}

	conditionFailRecord := Record{
		ID:                  "13/2/",
		RehydrationLocation: "bucket/13/2/",
		Status:              InProgress,
	}

	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName)).WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, &recordToUnlock, &conditionFailRecord)...)
	defer dyDB.Teardown()

	err := store.UnlockRecordForExpiration(ctx, recordToUnlock.ID)
	require.NoError(t, err)

	err = store.UnlockRecordForExpiration(ctx, conditionFailRecord.ID)
	// Error message should tell caller expected status and the actual status
	require.ErrorContains(t, err, string(conditionFailRecord.Status))
	require.ErrorContains(t, err, string(Expired))

	scanAll := dyDB.Scan(ctx, testIdempotencyTableName)
	require.Len(t, scanAll, 2)
	for _, item := range scanAll {
		actual, err := FromItem(item)
		require.NoError(t, err)
		assert.True(t, actual.ID == recordToUnlock.ID || actual.ID == conditionFailRecord.ID)
		if actual.ID == recordToUnlock.ID {
			assert.Equal(t, Completed, actual.Status)
			assert.Equal(t, recordToUnlock.RehydrationLocation, actual.RehydrationLocation)
			assert.Equal(t, recordToUnlock.FargateTaskARN, actual.FargateTaskARN)
		} else if actual.ID == conditionFailRecord.ID {
			assert.Equal(t, conditionFailRecord.Status, actual.Status)
			assert.Equal(t, conditionFailRecord.RehydrationLocation, actual.RehydrationLocation)
			assert.Equal(t, conditionFailRecord.FargateTaskARN, actual.FargateTaskARN)
		}

	}

	nonExistentRecordID := "999/9/"
	err = store.LockRecordForExpiration(ctx, nonExistentRecordID)
	var recordNotFound *RecordDoesNotExistsError
	if assert.ErrorAs(t, err, &recordNotFound) {
		assert.Equal(t, nonExistentRecordID, recordNotFound.RecordID)
	}
}

func createIdempotencyTableInput(tableName string) *dynamodb.CreateTableInput {
	return test.IdempotencyCreateTableInput(tableName, KeyAttrName)
}
