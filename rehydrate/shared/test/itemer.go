package test

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"testing"
)

type Itemer interface {
	Item() (map[string]types.AttributeValue, error)
}

func ItemersToPutItemInputs(t *testing.T, tableName string, itemers ...Itemer) []*dynamodb.PutItemInput {
	var inputs []*dynamodb.PutItemInput
	for _, itemer := range itemers {
		item, err := itemer.Item()
		require.NoError(t, err)
		inputs = append(inputs, &dynamodb.PutItemInput{
			Item:      item,
			TableName: aws.String(tableName),
		})
	}
	return inputs
}

func ItemerMapToPutItemInputs(t *testing.T, tableNameToItemers map[string][]Itemer) []*dynamodb.PutItemInput {
	var inputs []*dynamodb.PutItemInput
	for tableName, itemers := range tableNameToItemers {
		inputs = append(inputs, ItemersToPutItemInputs(t, tableName, itemers...)...)
	}
	return inputs
}
