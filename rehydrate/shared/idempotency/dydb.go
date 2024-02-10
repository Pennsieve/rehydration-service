package idempotency

import (
	"context"
	"errors"
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

func (s *Store) SaveInProgress(ctx context.Context, datasetID, datasetVersionID int) error {
	recordID := recordID(datasetID, datasetVersionID)
	record := Record{
		ID:     recordID,
		Status: InProgress,
	}
	return s.PutRecord(ctx, record)
}

func (s *Store) GetRecord(ctx context.Context, recordID string) (*Record, error) {
	key := itemKeyFromRecordID(recordID)
	in := dynamodb.GetItemInput{
		Key:            key,
		TableName:      aws.String(s.table),
		ConsistentRead: aws.Bool(true),
	}
	out, err := s.client.GetItem(ctx, &in)
	if err != nil {
		return nil, fmt.Errorf("error getting record with ID %s: %w", recordID, err)
	}
	if out.Item == nil || len(out.Item) == 0 {
		return nil, nil
	}
	return FromItem(out.Item)

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
	if _, err = s.client.PutItem(ctx, &in); err == nil {
		return nil
	}
	var conditionFailedError *types.ConditionalCheckFailedException
	if errors.As(err, &conditionFailedError) {
		alreadyExistsError := RecordAlreadyExistsError{}
		if existingRecord, err := FromItem(conditionFailedError.Item); err == nil {
			alreadyExistsError.Existing = existingRecord
		} else {
			alreadyExistsError.UnmarshallingError = err
		}
		return alreadyExistsError
	}
	return fmt.Errorf("error putting record %+v to %s: %w", record, s.table, err)
}

func (s *Store) UpdateRecord(ctx context.Context, record Record) error {
	asItem, err := record.Item()
	if err != nil {
		return fmt.Errorf("error marshalling record for update: %w", err)
	}
	expressionAttrNames := map[string]string{
		"#location": idempotencyRehydrationLocationAttrName,
		"#status":   idempotencyStatusAttrName,
	}
	expressionAttrValues := map[string]types.AttributeValue{
		":location": asItem[idempotencyRehydrationLocationAttrName],
		":status":   asItem[idempotencyStatusAttrName],
	}
	updateExpression := "SET #location = :location, #status = :status"

	in := &dynamodb.UpdateItemInput{
		Key:                       itemKeyFromRecordID(record.ID),
		TableName:                 aws.String(s.table),
		ExpressionAttributeNames:  expressionAttrNames,
		ExpressionAttributeValues: expressionAttrValues,
		UpdateExpression:          aws.String(updateExpression),
	}
	if _, err := s.client.UpdateItem(ctx, in); err != nil {
		return fmt.Errorf("error updating record %s: %w", record.ID, err)
	}
	return nil
}

func (s *Store) SetTaskARN(ctx context.Context, recordID string, taskARN string) error {
	expressionAttrNames := map[string]string{
		"#taskARN": idempotencyTaskARNAttrName,
	}
	expressionAttrValues := map[string]types.AttributeValue{
		":taskARN": &types.AttributeValueMemberS{Value: taskARN},
	}
	updateExpression := "SET #taskARN = :taskARN"

	in := &dynamodb.UpdateItemInput{
		Key:                       itemKeyFromRecordID(recordID),
		TableName:                 aws.String(s.table),
		ExpressionAttributeNames:  expressionAttrNames,
		ExpressionAttributeValues: expressionAttrValues,
		UpdateExpression:          aws.String(updateExpression),
	}
	if _, err := s.client.UpdateItem(ctx, in); err != nil {
		return fmt.Errorf("error setting task ARN %s on record %s: %w", taskARN, recordID, err)
	}
	return nil
}

func (s *Store) DeleteRecord(ctx context.Context, recordID string) error {
	in := &dynamodb.DeleteItemInput{
		Key:       itemKeyFromRecordID(recordID),
		TableName: aws.String(s.table),
	}
	if _, err := s.client.DeleteItem(ctx, in); err != nil {
		return fmt.Errorf("error deleting record %s: %w", recordID, err)
	}
	return nil
}

type RecordAlreadyExistsError struct {
	Existing           *Record
	UnmarshallingError error
}

func (e RecordAlreadyExistsError) Error() string {
	if e.UnmarshallingError == nil {
		return fmt.Sprintf("record with ID %s already exists", e.Existing.ID)
	}
	return fmt.Sprintf("record with ID already exists; there was an error when unmarshalling existing Record: %v", e.UnmarshallingError)
}

func recordID(datasetID, datasetVersionID int) string {
	return fmt.Sprintf("%d/%d/", datasetID, datasetVersionID)
}
func itemKeyFromRecordID(recordID string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{idempotencyKeyAttrName: &types.AttributeValueMemberS{Value: recordID}}
}
