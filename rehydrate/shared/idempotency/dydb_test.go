package idempotency

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStore_PutRecord(t *testing.T) {
	awsConfig := test.GetTestAWSConfig(t, test.NewAwsEndpointMap(), false)
	store, err := NewStore(awsConfig, logging.Default)
	require.NoError(t, err)
	dyDB := test.NewDynamoDBFixture(t, store.client, createIdempotencyTableInput(store.table))
	defer dyDB.Teardown()

	record := Record{
		ID:                  "1/2/",
		RehydrationLocation: "bucket/1/2/",
		Status:              InProgress,
		ExpiryTimestamp:     time.Now(),
	}
	ctx := context.Background()
	err = store.PutRecord(ctx, record)
	require.NoError(t, err)

	err = store.PutRecord(ctx, record)
	require.Error(t, err)
	var alreadyExistsError RecordAlreadyExistsError
	require.ErrorAs(t, err, &alreadyExistsError)
	require.Nil(t, alreadyExistsError.UnmarshallingError)
	require.True(t, record.Equal(*alreadyExistsError.Existing))
}

func TestStore_GetRecord(t *testing.T) {
	awsConfig := test.GetTestAWSConfig(t, test.NewAwsEndpointMap(), false)
	store, err := NewStore(awsConfig, logging.Default)
	require.NoError(t, err)

	record := Record{
		ID:                  "1/2/",
		RehydrationLocation: "bucket/1/2/",
		Status:              Completed,
		ExpiryTimestamp:     time.Now(),
	}

	dyDB := test.NewDynamoDBFixture(t, store.client, createIdempotencyTableInput(store.table)).WithItems(recordsToPutItemInputs(t, store.table, record)...)
	defer dyDB.Teardown()

	ctx := context.Background()
	actual, err := store.GetRecord(ctx, record.ID)
	require.NoError(t, err)
	require.NotNil(t, actual)
	require.True(t, actual.Equal(record))

	actual, err = store.GetRecord(ctx, "non-existent")
	require.NoError(t, err)
	require.Nil(t, actual)
}

func TestStore_UpdateRecord(t *testing.T) {
	awsConfig := test.GetTestAWSConfig(t, test.NewAwsEndpointMap(), false)
	store, err := NewStore(awsConfig, logging.Default)
	require.NoError(t, err)

	record := Record{
		ID:              "1/2/",
		Status:          InProgress,
		ExpiryTimestamp: time.Now(),
	}

	dyDB := test.NewDynamoDBFixture(t, store.client, createIdempotencyTableInput(store.table)).WithItems(recordsToPutItemInputs(t, store.table, record)...)
	defer dyDB.Teardown()

	updatedLocation := "bucket/1/2/"
	updatedStatus := Completed
	record.RehydrationLocation = updatedLocation
	record.Status = updatedStatus

	ctx := context.Background()
	err = store.UpdateRecord(ctx, record)
	require.NoError(t, err)

	updated, err := store.GetRecord(ctx, record.ID)
	require.NoError(t, err)
	require.Equal(t, record.ID, updated.ID)
	require.True(t, record.ExpiryTimestamp.Equal(updated.ExpiryTimestamp))
	require.Equal(t, updatedLocation, updated.RehydrationLocation)
	require.Equal(t, updatedStatus, updated.Status)
}

func createIdempotencyTableInput(tableName string) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(idempotencyKeyAttrName),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(idempotencyKeyAttrName),
				KeyType:       types.KeyTypeHash,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	}
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
