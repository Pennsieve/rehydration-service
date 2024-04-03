package test

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/rehydration-service/shared/tracking"
)

func TrackingCreateTableInput(tableName string) *dynamodb.CreateTableInput {
	globalIndices := []types.GlobalSecondaryIndex{{
		IndexName: aws.String(tracking.DatasetVersionIndexName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(tracking.DatasetVersionAttrName), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(tracking.RehydrationStatusAttrName), KeyType: types.KeyTypeRange},
		},
		Projection: &types.Projection{
			NonKeyAttributes: []string{
				tracking.IDAttrName,
				tracking.UserNameAttrName,
				tracking.UserEmailAttrName,
				tracking.EmailSentDateAttrName,
			},
			ProjectionType: types.ProjectionTypeInclude,
		},
	}}
	return &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(tracking.IDAttrName),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(tracking.DatasetVersionAttrName),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(tracking.RehydrationStatusAttrName),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(tracking.IDAttrName),
				KeyType:       types.KeyTypeHash,
			},
		},
		GlobalSecondaryIndexes: globalIndices,
		BillingMode:            types.BillingModePayPerRequest,
	}
}
