package idempotency

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/require"
	"testing"
)

var testIdempotencyTableName = "test-idempotency-table"

func TestStore_PutRecord(t *testing.T) {
	awsConfig := test.GetTestAWSConfig(t, test.NewAwsEndpointMap(), false)
	store := newDyDBStore(awsConfig, logging.Default, testIdempotencyTableName)
	dyDB := test.NewDynamoDBFixture(t, store.client, createIdempotencyTableInput(store.table))
	defer dyDB.Teardown()

	record := Record{
		ID:                  "1/2/",
		RehydrationLocation: "bucket/1/2/",
		Status:              InProgress,
	}
	ctx := context.Background()
	err := store.PutRecord(ctx, record)
	require.NoError(t, err)

	err = store.PutRecord(ctx, record)
	require.Error(t, err)
	var alreadyExistsError RecordAlreadyExistsError
	require.ErrorAs(t, err, &alreadyExistsError)
	require.Nil(t, alreadyExistsError.UnmarshallingError)
	require.Equal(t, record, *alreadyExistsError.Existing)
}

func TestStore_GetRecord(t *testing.T) {
	awsConfig := test.GetTestAWSConfig(t, test.NewAwsEndpointMap(), false)
	store := newDyDBStore(awsConfig, logging.Default, testIdempotencyTableName)

	record := Record{
		ID:                  "1/2/",
		RehydrationLocation: "bucket/1/2/",
		Status:              Completed,
	}

	dyDB := test.NewDynamoDBFixture(t, store.client, createIdempotencyTableInput(store.table)).WithItems(recordsToPutItemInputs(t, store.table, record)...)
	defer dyDB.Teardown()

	ctx := context.Background()
	actual, err := store.GetRecord(ctx, record.ID)
	require.NoError(t, err)
	require.NotNil(t, actual)
	require.Equal(t, record, *actual)

	actual, err = store.GetRecord(ctx, "non-existent")
	require.NoError(t, err)
	require.Nil(t, actual)
}

func TestStore_UpdateRecord(t *testing.T) {
	awsConfig := test.GetTestAWSConfig(t, test.NewAwsEndpointMap(), false)
	store := newDyDBStore(awsConfig, logging.Default, testIdempotencyTableName)

	record := Record{
		ID:     "1/2/",
		Status: InProgress,
	}

	dyDB := test.NewDynamoDBFixture(t, store.client, createIdempotencyTableInput(store.table)).WithItems(recordsToPutItemInputs(t, store.table, record)...)
	defer dyDB.Teardown()

	updatedLocation := "bucket/1/2/"
	updatedStatus := Completed
	record.RehydrationLocation = updatedLocation
	record.Status = updatedStatus

	ctx := context.Background()
	err := store.UpdateRecord(ctx, record)
	require.NoError(t, err)

	scanAll := dyDB.Scan(ctx, store.table)
	require.Len(t, scanAll, 1)
	scanned, err := FromItem(scanAll[0])
	require.NoError(t, err)
	require.Equal(t, record.ID, scanned.ID)
	require.Equal(t, updatedLocation, scanned.RehydrationLocation)
	require.Equal(t, updatedStatus, scanned.Status)
}

func TestStore_SetTaskARN(t *testing.T) {
	awsConfig := test.GetTestAWSConfig(t, test.NewAwsEndpointMap(), false)
	store := newDyDBStore(awsConfig, logging.Default, testIdempotencyTableName)

	record := Record{
		ID:     "1/2/",
		Status: InProgress,
	}

	dyDB := test.NewDynamoDBFixture(t, store.client, createIdempotencyTableInput(store.table)).WithItems(recordsToPutItemInputs(t, store.table, record)...)
	defer dyDB.Teardown()

	ctx := context.Background()
	taskARN := "arn:aws:ecs:test:test:test"
	err := store.SetTaskARN(ctx, record.ID, taskARN)
	require.NoError(t, err)

	scanAll := dyDB.Scan(ctx, store.table)
	require.Len(t, scanAll, 1)
	scanned, err := FromItem(scanAll[0])
	require.NoError(t, err)
	require.Equal(t, record.ID, scanned.ID)
	require.Equal(t, record.RehydrationLocation, scanned.RehydrationLocation)
	require.Equal(t, record.Status, scanned.Status)
	require.Equal(t, taskARN, scanned.FargateTaskARN)
}

func TestStore_DeleteRecord(t *testing.T) {
	awsConfig := test.GetTestAWSConfig(t, test.NewAwsEndpointMap(), false)
	store := newDyDBStore(awsConfig, logging.Default, testIdempotencyTableName)

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

	dyDB := test.NewDynamoDBFixture(t, store.client, createIdempotencyTableInput(store.table)).WithItems(recordsToPutItemInputs(t, store.table, recordToDelete, recordToKeep)...)
	defer dyDB.Teardown()

	ctx := context.Background()
	err := store.DeleteRecord(ctx, recordToDelete.ID)
	require.NoError(t, err)

	scanAll := dyDB.Scan(ctx, store.table)
	require.Len(t, scanAll, 1)
	scanned, err := FromItem(scanAll[0])
	require.NoError(t, err)
	require.Equal(t, recordToKeep, *scanned)
}

func createIdempotencyTableInput(tableName string) *dynamodb.CreateTableInput {
	return test.IdempotencyCreateTableInput(tableName, KeyAttrName)
}

func recordsToPutItemInputs(t *testing.T, tableName string, records ...Record) []*dynamodb.PutItemInput {
	var inputs []*dynamodb.PutItemInput
	for _, record := range records {
		item, err := record.Item()
		require.NoError(t, err)
		inputs = append(inputs, &dynamodb.PutItemInput{
			Item:      item,
			TableName: aws.String(tableName),
		})
	}
	return inputs
}
