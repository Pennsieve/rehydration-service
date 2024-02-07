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
	t.Skip("need to finish create table input")
	awsConfig := test.GetTestAWSConfig(t, test.NewAwsEndpointMap())
	store, err := NewStore(awsConfig, logging.Default)
	require.NoError(t, err)
	createTable := dynamodb.CreateTableInput{
		TableName: aws.String(store.table),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("key"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("key"),
				KeyType:       types.KeyTypeHash,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	}
	dyDB := test.NewDynamoDBFixture(t, store.client, &createTable)
	defer dyDB.Teardown()

	record := Record{
		Key:                 "1/2/",
		RehydrationLocation: "bucket/1/2/",
		Status:              InProgress,
		ExpiryTimestamp:     time.Now().UTC(),
	}
	ctx := context.Background()
	err = store.PutRecord(ctx, record)
	require.NoError(t, err)
}
