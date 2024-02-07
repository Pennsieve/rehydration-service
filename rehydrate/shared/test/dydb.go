package test

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type DynamoDBFixture struct {
	Fixture
	Client *dynamodb.Client
	// Tables is a set of table names
	Tables  map[string]bool
	context context.Context
}

func NewDynamoDBFixture(t *testing.T, client *dynamodb.Client, inputs ...*dynamodb.CreateTableInput) *DynamoDBFixture {
	f := DynamoDBFixture{
		Fixture: Fixture{T: t},
		Client:  client,
		Tables:  map[string]bool{},
		context: context.Background(),
	}
	var waitInputs []dynamodb.DescribeTableInput
	for _, input := range inputs {
		tableName := aws.ToString(input.TableName)
		if _, err := f.Client.CreateTable(f.context, input); err != nil {
			assert.FailNow(f.T, "error creating test table", "table: %s, error: %v", tableName, err)
		}
		f.Tables[tableName] = true
		waitInputs = append(waitInputs, dynamodb.DescribeTableInput{TableName: input.TableName})
	}
	if err := waitForEverything(waitInputs, func(i dynamodb.DescribeTableInput) error {
		return dynamodb.NewTableExistsWaiter(f.Client).Wait(f.context, &i, time.Minute)
	}); err != nil {
		assert.FailNow(f.T, "test table not created", err)
	}
	return &f
}

func (f *DynamoDBFixture) WithItems(inputs ...*dynamodb.PutItemInput) *DynamoDBFixture {
	for _, input := range inputs {
		if _, err := f.Client.PutItem(f.context, input); err != nil {
			assert.FailNow(f.T, "error adding item test table", "table: %s, item: %v, error: %v", aws.ToString(input.TableName), input.Item, err)
		}
	}
	return f
}

func (f *DynamoDBFixture) Teardown() {
	var waitInputs []dynamodb.DescribeTableInput
	for name := range f.Tables {
		input := dynamodb.DeleteTableInput{TableName: aws.String(name)}
		if _, err := f.Client.DeleteTable(f.context, &input); err != nil {
			assert.FailNow(f.T, "error deleting test table", "table: %s, error: %v", name, err)
		}
		waitInputs = append(waitInputs, dynamodb.DescribeTableInput{TableName: input.TableName})
	}
	if err := waitForEverything(waitInputs, func(i dynamodb.DescribeTableInput) error {
		return dynamodb.NewTableNotExistsWaiter(f.Client).Wait(f.context, &i, time.Minute)
	}); err != nil {
		assert.FailNow(f.T, "test table not deleted", err)
	}

}
