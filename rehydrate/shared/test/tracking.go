package test

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/pennsieve/rehydration-service/shared/models"
	"github.com/pennsieve/rehydration-service/shared/tracking"
	"time"
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
	},
		{
			IndexName: aws.String(tracking.ExpirationIndexName),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(tracking.RehydrationStatusAttrName), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(tracking.EmailSentDateAttrName), KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{
				NonKeyAttributes: []string{
					tracking.DatasetVersionAttrName,
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
			{
				AttributeName: aws.String(tracking.EmailSentDateAttrName),
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

func NewTestEntry(dataset models.Dataset, user models.User) *tracking.Entry {
	return &tracking.Entry{
		DatasetVersionIndex: tracking.DatasetVersionIndex{
			ID:                uuid.NewString(),
			DatasetVersion:    dataset.DatasetVersion(),
			UserName:          user.Name,
			UserEmail:         user.Email,
			RehydrationStatus: tracking.InProgress,
		},
		LambdaLogStream: uuid.NewString(),
		AWSRequestID:    uuid.NewString(),
		RequestDate:     time.Now(),
	}
}
