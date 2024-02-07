package idempotency

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log/slog"
	"os"
)

const idempotencyTableNameKey = "FARGATE_IDEMPOTENT_DYNAMODB_TABLE_NAME"

type Store struct {
	client *dynamodb.Client
	table  string
	logger *slog.Logger
}

func NewStore(config aws.Config, logger *slog.Logger) (*Store, error) {

	table, ok := os.LookupEnv(idempotencyTableNameKey)
	if !ok {
		return nil, fmt.Errorf("environment variable %s not set", idempotencyTableNameKey)
	}
	if len(table) == 0 {
		return nil, fmt.Errorf("environment variable %s set to empty string", idempotencyTableNameKey)

	}
	client := dynamodb.NewFromConfig(config)
	return &Store{
		client: client,
		table:  table,
		logger: logger,
	}, nil
}

func (s *Store) PutRecord(ctx context.Context, record Record) error {
	item, err := record.Item()
	if err != nil {
		return err
	}
	putCondition := fmt.Sprintf("attribute_not_exists(%s)", idempotencyKeyAttrName)
	in := dynamodb.PutItemInput{
		Item:                                item,
		TableName:                           aws.String(s.table),
		ConditionExpression:                 aws.String(putCondition),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	}
	out, err := s.client.PutItem(ctx, &in)
	if err != nil {
		return fmt.Errorf("error putting record %+v to %s: %w", record, s.table, err)
	}
	existingRecord, err := FromItem(out.Attributes)
	if err != nil {
		return fmt.Errorf("error unmarshalling condition check failure return values: %w", err)
	}
	s.logger.Info(fmt.Sprintf("condition check failure return value: %+v", existingRecord))
	return err
}
