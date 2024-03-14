package test

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func IdempotencyCreateTableInput(tableName string, idempotencyKeyAttrName string) *dynamodb.CreateTableInput {
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

type Itemer interface {
	Item() (map[string]types.AttributeValue, error)
}

func RecordsToPutItemInputs(t *testing.T, tableName string, records ...Itemer) []*dynamodb.PutItemInput {
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
