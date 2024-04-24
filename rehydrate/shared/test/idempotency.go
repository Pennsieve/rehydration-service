package test

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
)

func IdempotencyCreateTableInput(tableName string) *dynamodb.CreateTableInput {
	globalIndices := []types.GlobalSecondaryIndex{{
		IndexName: aws.String(idempotency.ExpirationIndexName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(idempotency.StatusAttrName), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(idempotency.ExpirationDateAttrName), KeyType: types.KeyTypeRange},
		},
		Projection: &types.Projection{
			NonKeyAttributes: []string{
				idempotency.KeyAttrName,
				idempotency.RehydrationLocationAttrName,
			},
			ProjectionType: types.ProjectionTypeInclude,
		},
	}}
	return &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(idempotency.KeyAttrName),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(idempotency.StatusAttrName),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(idempotency.ExpirationDateAttrName),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(idempotency.KeyAttrName),
				KeyType:       types.KeyTypeHash,
			},
		},
		GlobalSecondaryIndexes: globalIndices,
		BillingMode:            types.BillingModePayPerRequest,
	}
}
