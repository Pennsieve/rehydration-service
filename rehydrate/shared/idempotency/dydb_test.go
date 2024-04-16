package idempotency_test

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var testIdempotencyTableName = "test-idempotency-table"

func TestStore_PutRecord(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := idempotency.NewStore(dyDBClient, logging.Default, testIdempotencyTableName)
	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName))
	defer dyDB.Teardown()

	record := idempotency.NewRecord("1/2/", idempotency.InProgress).WithRehydrationLocation("bucket/1/2/")

	err := store.PutRecord(ctx, *record)
	require.NoError(t, err)

	err = store.PutRecord(ctx, *record)
	require.Error(t, err)
	var alreadyExistsError *idempotency.RecordAlreadyExistsError
	require.ErrorAs(t, err, &alreadyExistsError)
	require.Nil(t, alreadyExistsError.UnmarshallingError)
	require.Equal(t, record, alreadyExistsError.Existing)
}

func TestStore_GetRecord(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := idempotency.NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

	record := idempotency.NewRecord("1/2/", idempotency.Completed).
		WithRehydrationLocation("bucket/1/2/")

	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName)).WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, record)...)
	defer dyDB.Teardown()

	actual, err := store.GetRecord(ctx, record.ID)
	require.NoError(t, err)
	require.NotNil(t, actual)
	require.Equal(t, record, actual)

	actual, err = store.GetRecord(ctx, "non-existent")
	require.NoError(t, err)
	require.Nil(t, actual)
}

func TestStore_UpdateRecord(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := idempotency.NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

	record := idempotency.NewRecord("1/2/", idempotency.InProgress)

	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName)).WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, record)...)
	defer dyDB.Teardown()

	updatedLocation := "bucket/1/2/"
	updatedStatus := idempotency.Completed
	updatedExpirationDate := time.Now().Add(time.Hour * time.Duration(24*14))
	record.RehydrationLocation = updatedLocation
	record.Status = updatedStatus
	record.ExpirationDate = &updatedExpirationDate

	err := store.UpdateRecord(ctx, *record)
	require.NoError(t, err)

	scanAll := dyDB.Scan(ctx, testIdempotencyTableName)
	require.Len(t, scanAll, 1)
	scanned, err := idempotency.FromItem(scanAll[0])
	require.NoError(t, err)
	assert.Equal(t, record.ID, scanned.ID)
	assert.Equal(t, updatedLocation, scanned.RehydrationLocation)
	assert.Equal(t, updatedStatus, scanned.Status)
	assert.True(t, updatedExpirationDate.Equal(*scanned.ExpirationDate))
}

func TestStore_SetTaskARN(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := idempotency.NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

	record := idempotency.NewRecord("1/2/", idempotency.InProgress)

	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName)).WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, record)...)
	defer dyDB.Teardown()

	taskARN := "arn:aws:ecs:test:test:test"
	err := store.SetTaskARN(ctx, record.ID, taskARN)
	require.NoError(t, err)

	scanAll := dyDB.Scan(ctx, testIdempotencyTableName)
	require.Len(t, scanAll, 1)
	scanned, err := idempotency.FromItem(scanAll[0])
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
	store := idempotency.NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

	recordToDelete := idempotency.NewRecord("1/2/", idempotency.Expired).
		WithRehydrationLocation("bucket/1/2/")

	recordToKeep := idempotency.NewRecord("4/9/", idempotency.Completed).
		WithRehydrationLocation("bucket/4/9/")

	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName)).WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, recordToDelete, recordToKeep)...)
	defer dyDB.Teardown()

	err := store.DeleteRecord(ctx, recordToDelete.ID)
	require.NoError(t, err)

	scanAll := dyDB.Scan(ctx, testIdempotencyTableName)
	require.Len(t, scanAll, 1)
	scanned, err := idempotency.FromItem(scanAll[0])
	require.NoError(t, err)
	require.Equal(t, recordToKeep, scanned)
}

func TestStore_ExpireRecord(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := idempotency.NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

	recordToExpire := idempotency.NewRecord("1/2/", idempotency.InProgress).
		WithRehydrationLocation("bucket/1/2/")

	dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName)).WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, recordToExpire)...)
	defer dyDB.Teardown()

	err := store.ExpireRecord(ctx, recordToExpire.ID)
	require.NoError(t, err)

	scanAll := dyDB.Scan(ctx, testIdempotencyTableName)
	require.Len(t, scanAll, 1)
	scanned, err := idempotency.FromItem(scanAll[0])
	require.NoError(t, err)
	assert.Equal(t, idempotency.Expired, scanned.Status)
	assert.Equal(t, recordToExpire.ID, scanned.ID)
	assert.Equal(t, recordToExpire.RehydrationLocation, scanned.RehydrationLocation)
	assert.Equal(t, recordToExpire.FargateTaskARN, scanned.FargateTaskARN)

	nonExistentRecordID := "999/9/"
	err = store.ExpireRecord(ctx, nonExistentRecordID)
	var recordNotFound *idempotency.RecordDoesNotExistsError
	if assert.ErrorAs(t, err, &recordNotFound) {
		assert.Equal(t, nonExistentRecordID, recordNotFound.RecordID)
	}
}

func TestDyDBStore_SetExpirationDate_ConditionErrors(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := idempotency.NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

	nonExistentRecordID := "999/9/"

	expirationDate := time.Now().Add(time.Hour * time.Duration(14*24))

	wrongStatusFailRecord := idempotency.NewRecord("54/8", idempotency.InProgress)
	laterExpirationDate := expirationDate.Add(time.Hour * 24)
	laterExpirationFailRecord := idempotency.NewRecord("13/2/", idempotency.Completed).
		WithRehydrationLocation("bucket/13/2/").
		WithExpirationDate(&laterExpirationDate)
	wrongStatusAndLaterExpirationFailRecord := idempotency.NewRecord("894/1", idempotency.Expired).WithExpirationDate(&laterExpirationDate)

	for _, tst := range []struct {
		name   string
		record *idempotency.Record
	}{
		{"no such record", nil},
		{"wrong status", wrongStatusFailRecord},
		{"later expiration date", laterExpirationFailRecord},
		{"wrong status and later expiration date", wrongStatusAndLaterExpirationFailRecord},
	} {
		t.Run(tst.name, func(t *testing.T) {
			dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName))
			if tst.record != nil {
				dyDB = dyDB.WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, tst.record)...)
			}
			defer dyDB.Teardown()

			if tst.record == nil {
				err := store.SetExpirationDate(ctx, nonExistentRecordID, expirationDate)
				var recordNotFound *idempotency.RecordDoesNotExistsError
				if assert.ErrorAs(t, err, &recordNotFound) {
					assert.Equal(t, nonExistentRecordID, recordNotFound.RecordID)
				}
				scanAll := dyDB.Scan(ctx, testIdempotencyTableName)
				assert.Empty(t, scanAll)
			} else {
				err := store.SetExpirationDate(ctx, tst.record.ID, expirationDate)
				var conditionFailedError *idempotency.ConditionFailedError
				if assert.ErrorAs(t, err, &conditionFailedError) {
					assert.Contains(t, conditionFailedError.Error(), tst.record.Status)
					if tst.record.ExpirationDate != nil {
						assert.Contains(t, conditionFailedError.Error(), tst.record.ExpirationDate.Format(time.RFC3339Nano))
					}
					assert.Contains(t, conditionFailedError.Error(), tst.record.ID)
					assert.Contains(t, conditionFailedError.Error(), expirationDate.Format(time.RFC3339Nano))
				}

				scanAll := dyDB.Scan(ctx, testIdempotencyTableName)
				require.Len(t, scanAll, 1)
				item := scanAll[0]
				actual, err := idempotency.FromItem(item)
				require.NoError(t, err)
				if tst.record.ExpirationDate == nil {
					assert.Nil(t, actual.ExpirationDate)
				} else {
					assert.True(t, laterExpirationDate.Equal(*actual.ExpirationDate))
				}
				assert.Equal(t, tst.record.Status, actual.Status)
				assert.Equal(t, tst.record.RehydrationLocation, actual.RehydrationLocation)
				assert.Equal(t, tst.record.FargateTaskARN, actual.FargateTaskARN)
			}
		})
	}

}

func TestDyDBStore_SetExpirationDate(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)

	expirationDate := time.Now().Add(time.Hour * time.Duration(14*24))

	for _, tst := range []struct {
		name                      string
		preexistingExpirationDate *time.Time
	}{
		{"no pre-existing expiration date", nil},
		{"pre-existing expiration date", &expirationDate},
	} {
		t.Run(tst.name, func(t *testing.T) {
			record := idempotency.NewRecord("1/2/", idempotency.Completed).
				WithRehydrationLocation("bucket/1/2/").
				WithExpirationDate(tst.preexistingExpirationDate)

			dyDB := test.NewDynamoDBFixture(t, awsConfig, createIdempotencyTableInput(testIdempotencyTableName)).WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, record)...)
			defer dyDB.Teardown()

			laterExpirationDate := expirationDate.Add(time.Hour * 24)

			store := idempotency.NewStore(dyDBClient, logging.Default, testIdempotencyTableName)

			err := store.SetExpirationDate(ctx, record.ID, laterExpirationDate)
			require.NoError(t, err)

			scanAll := dyDB.Scan(ctx, testIdempotencyTableName)
			require.Len(t, scanAll, 1)
			item := scanAll[0]
			actual, err := idempotency.FromItem(item)
			require.NoError(t, err)
			assert.True(t, laterExpirationDate.Equal(*actual.ExpirationDate))
			assert.Equal(t, record.Status, actual.Status)
			assert.Equal(t, record.RehydrationLocation, actual.RehydrationLocation)
			assert.Equal(t, record.FargateTaskARN, actual.FargateTaskARN)

		})
	}

}

func TestDyDBStore_QueryExpirationIndex(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := idempotency.NewStore(dyDBClient, logging.Default, testIdempotencyTableName)
	now := time.Now()

	toExpireExpDate := now.Add(-time.Hour * 24)
	toExpire := idempotency.NewRecord("12/1/", idempotency.Completed).
		WithRehydrationLocation("s3://bucket/12/1/").
		WithExpirationDate(&toExpireExpDate)
	expiredExpDate := now.Add(-time.Hour * time.Duration(24*3))
	expired := idempotency.NewRecord("34/1/", idempotency.Expired).
		WithRehydrationLocation("s3://bucket/34/1/").
		WithExpirationDate(&expiredExpDate)
	inProgress := idempotency.NewRecord("56/7/", idempotency.InProgress)
	dyBFixture := test.NewDynamoDBFixture(t, awsConfig, test.IdempotencyCreateTableInput(testIdempotencyTableName)).
		WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, toExpire, expired, inProgress)...)
	defer dyBFixture.Teardown()

	queryResults, err := store.QueryExpirationIndex(ctx, now, 10)
	require.NoError(t, err)
	assert.Len(t, queryResults, 1)
	actual := queryResults[0]
	assert.Equal(t, toExpire.ID, actual.ID)
	assert.Equal(t, toExpire.Status, actual.Status)
	assert.Equal(t, toExpire.RehydrationLocation, actual.RehydrationLocation)
	assert.True(t, toExpire.ExpirationDate.Equal(*actual.ExpirationDate))
}

func TestDyDBStore_ExpireByIndex(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := idempotency.NewStore(dyDBClient, logging.Default, testIdempotencyTableName)
	now := time.Now()

	toExpireExpDate := now.Add(-time.Hour * 24)
	toExpire := idempotency.NewRecord("12/1/", idempotency.Completed).
		WithRehydrationLocation("s3://bucket/12/1/").
		WithFargateTaskARN(uuid.NewString()).
		WithExpirationDate(&toExpireExpDate)

	dyBFixture := test.NewDynamoDBFixture(t, awsConfig, test.IdempotencyCreateTableInput(testIdempotencyTableName)).
		WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, toExpire)...)
	defer dyBFixture.Teardown()

	result, err := store.ExpireByIndex(ctx, toExpire.ExpirationIndex)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, toExpire.ID, result.ID)
	assert.Equal(t, idempotency.Expired, result.Status)
	assert.Equal(t, toExpire.RehydrationLocation, result.RehydrationLocation)
	assert.True(t, toExpire.ExpirationDate.Equal(*result.ExpirationDate))

	allItems := dyBFixture.Scan(ctx, testIdempotencyTableName)
	require.Len(t, allItems, 1)
	item := allItems[0]
	actualRecord, err := idempotency.FromItem(item)
	require.NoError(t, err)
	assert.Equal(t, toExpire.ID, actualRecord.ID)
	assert.Equal(t, idempotency.Expired, actualRecord.Status)
	assert.Equal(t, toExpire.RehydrationLocation, actualRecord.RehydrationLocation)
	assert.True(t, toExpire.ExpirationDate.Equal(*actualRecord.ExpirationDate))
	assert.Equal(t, toExpire.FargateTaskARN, actualRecord.FargateTaskARN)

}

func TestDyDBStore_ExpireByIndex_ConditionCheckFailure(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := idempotency.NewStore(dyDBClient, logging.Default, testIdempotencyTableName)
	now := time.Now()

	toExpireExpDate := now.Add(-time.Hour * 24)
	record := idempotency.NewRecord("12/1/", idempotency.Completed).
		WithRehydrationLocation("s3://bucket/12/1/").
		WithFargateTaskARN(uuid.NewString()).
		WithExpirationDate(&toExpireExpDate)

	dyBFixture := test.NewDynamoDBFixture(t, awsConfig, test.IdempotencyCreateTableInput(testIdempotencyTableName)).
		WithItems(test.ItemersToPutItemInputs(t, testIdempotencyTableName, record)...)
	defer dyBFixture.Teardown()

	outdatedExpirationDate := record.ExpirationDate.Add(-time.Hour * time.Duration(24*14))
	outdatedIndex := idempotency.ExpirationIndex{
		ID:                  record.ID,
		RehydrationLocation: record.RehydrationLocation,
		Status:              record.Status,
		ExpirationDate:      &outdatedExpirationDate,
	}

	_, err := store.ExpireByIndex(ctx, outdatedIndex)
	require.Error(t, err)
	var conditionCheckError *idempotency.ConditionFailedError
	if assert.ErrorAs(t, err, &conditionCheckError) {
		assert.Contains(t, conditionCheckError.Error(), outdatedIndex.Status)
		assert.Contains(t, conditionCheckError.Error(), outdatedIndex.ID)
		assert.Contains(t, conditionCheckError.Error(), outdatedIndex.ExpirationDate.String())
		assert.Contains(t, conditionCheckError.Error(), record.ExpirationDate.Format(time.RFC3339Nano))
	}

	allItems := dyBFixture.Scan(ctx, testIdempotencyTableName)
	require.Len(t, allItems, 1)
	item := allItems[0]
	actualRecord, err := idempotency.FromItem(item)
	require.NoError(t, err)
	assert.Equal(t, record.ID, actualRecord.ID)
	assert.Equal(t, record.Status, actualRecord.Status)
	assert.Equal(t, record.RehydrationLocation, actualRecord.RehydrationLocation)
	assert.True(t, record.ExpirationDate.Equal(*actualRecord.ExpirationDate))
	assert.Equal(t, record.FargateTaskARN, actualRecord.FargateTaskARN)
}

func createIdempotencyTableInput(tableName string) *dynamodb.CreateTableInput {
	return test.IdempotencyCreateTableInput(tableName)
}
