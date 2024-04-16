package idempotency

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/rehydration-service/shared/dydbutils"
	"log/slog"
	"strings"
	"time"
)

const TableNameKey = "FARGATE_IDEMPOTENT_DYNAMODB_TABLE_NAME"

type DyDBStore struct {
	client *dynamodb.Client
	table  string
	logger *slog.Logger
}

func NewStore(dyDBClient *dynamodb.Client, logger *slog.Logger, tableName string) Store {
	return &DyDBStore{
		client: dyDBClient,
		table:  tableName,
		logger: logger,
	}
}

func (s *DyDBStore) SaveInProgress(ctx context.Context, datasetID, datasetVersionID int) error {
	recordID := RecordID(datasetID, datasetVersionID)
	record := NewRecord(recordID, InProgress)
	return s.PutRecord(ctx, *record)
}

func (s *DyDBStore) GetRecord(ctx context.Context, recordID string) (*Record, error) {
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

func (s *DyDBStore) PutRecord(ctx context.Context, record Record) error {
	item, err := record.Item()
	if err != nil {
		return err
	}
	putConditionBuilder := expression.Name(KeyAttrName).AttributeNotExists()
	putExpression, err := expression.NewBuilder().WithCondition(putConditionBuilder).Build()
	if err != nil {
		return fmt.Errorf("error building PutRecord expression: %w", err)
	}
	in := dynamodb.PutItemInput{
		Item:                                item,
		TableName:                           aws.String(s.table),
		ExpressionAttributeNames:            putExpression.Names(),
		ExpressionAttributeValues:           putExpression.Values(),
		ConditionExpression:                 putExpression.Condition(),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	}
	if _, err = s.client.PutItem(ctx, &in); err == nil {
		return nil
	}
	var conditionFailedError *types.ConditionalCheckFailedException
	if errors.As(err, &conditionFailedError) {
		alreadyExistsError := &RecordAlreadyExistsError{}
		if existingRecord, err := FromItem(conditionFailedError.Item); err == nil {
			alreadyExistsError.Existing = existingRecord
		} else {
			alreadyExistsError.UnmarshallingError = err
		}
		return alreadyExistsError
	}
	return fmt.Errorf("error putting record %+v to %s: %w", record, s.table, err)
}

func (s *DyDBStore) UpdateRecord(ctx context.Context, record Record) error {
	updateExpressionBuilder := expression.Set(
		expression.Name(RehydrationLocationAttrName),
		expression.Value(record.RehydrationLocation),
	).Set(
		expression.Name(StatusAttrName),
		expression.Value(record.Status),
	).Set(
		expression.Name(ExpirationDateAttrName),
		expression.Value(record.ExpirationDate))
	updateExpression, err := expression.NewBuilder().WithUpdate(updateExpressionBuilder).Build()
	if err != nil {
		return fmt.Errorf("error building UpdateRecord expression: %w", err)
	}
	in := &dynamodb.UpdateItemInput{
		Key:                       itemKeyFromRecordID(record.ID),
		TableName:                 aws.String(s.table),
		ExpressionAttributeNames:  updateExpression.Names(),
		ExpressionAttributeValues: updateExpression.Values(),
		UpdateExpression:          updateExpression.Update(),
	}
	if _, err := s.client.UpdateItem(ctx, in); err != nil {
		return fmt.Errorf("error updating record %s: %w", record.ID, err)
	}
	return nil
}

func (s *DyDBStore) SetTaskARN(ctx context.Context, recordID string, taskARN string) error {
	updateExpressionBuilder := expression.Set(expression.Name(TaskARNAttrName), expression.Value(taskARN))
	updateExpression, err := expression.NewBuilder().WithUpdate(updateExpressionBuilder).Build()
	if err != nil {
		return fmt.Errorf("error building SetTaskARN expression: %w", err)
	}
	in := &dynamodb.UpdateItemInput{
		Key:                       itemKeyFromRecordID(recordID),
		TableName:                 aws.String(s.table),
		ExpressionAttributeNames:  updateExpression.Names(),
		ExpressionAttributeValues: updateExpression.Values(),
		UpdateExpression:          updateExpression.Update(),
	}
	if _, err := s.client.UpdateItem(ctx, in); err != nil {
		return fmt.Errorf("error setting task ARN %s on record %s: %w", taskARN, recordID, err)
	}
	return nil
}

func (s *DyDBStore) DeleteRecord(ctx context.Context, recordID string) error {
	in := &dynamodb.DeleteItemInput{
		Key:       itemKeyFromRecordID(recordID),
		TableName: aws.String(s.table),
	}
	if _, err := s.client.DeleteItem(ctx, in); err != nil {
		return fmt.Errorf("error deleting record %s: %w", recordID, err)
	}
	return nil
}

func (s *DyDBStore) ExpireRecord(ctx context.Context, recordID string) error {
	updateExpressionBuilder := expression.Set(expression.Name(StatusAttrName), expression.Value(Expired))
	conditionExpressionBuilder := expression.AttributeExists(expression.Name(KeyAttrName))
	updateExpression, err := expression.NewBuilder().
		WithUpdate(updateExpressionBuilder).
		WithCondition(conditionExpressionBuilder).
		Build()
	if err != nil {
		return fmt.Errorf("error building ExpireRecord expression: %w", err)
	}

	in := &dynamodb.UpdateItemInput{
		Key:                       itemKeyFromRecordID(recordID),
		TableName:                 aws.String(s.table),
		ExpressionAttributeNames:  updateExpression.Names(),
		ExpressionAttributeValues: updateExpression.Values(),
		UpdateExpression:          updateExpression.Update(),
		ConditionExpression:       updateExpression.Condition(),
	}
	if _, err := s.client.UpdateItem(ctx, in); err != nil {
		var conditionFailedError *types.ConditionalCheckFailedException
		if errors.As(err, &conditionFailedError) {
			return &RecordDoesNotExistsError{RecordID: recordID}
		}
		return fmt.Errorf("error expiring record %s: %w", recordID, err)
	}
	return nil
}

func (s *DyDBStore) SetExpirationDate(ctx context.Context, recordID string, expirationDate time.Time) error {
	updateBuilder := expression.Set(expression.Name(ExpirationDateAttrName), expression.Value(expirationDate))
	// only set expiration if record actually exists and if status is COMPLETED, and if current expiration date, if any,
	// is earlier than the new one.
	conditionBuilder := expression.And(
		expression.AttributeExists(expression.Name(KeyAttrName)),
		expression.Equal(expression.Name(StatusAttrName), expression.Value(Completed)),
		expression.Or(
			expression.AttributeNotExists(expression.Name(ExpirationDateAttrName)),
			expression.LessThan(expression.Name(ExpirationDateAttrName), expression.Value(expirationDate))),
	)
	setExpirationExpression, err := expression.NewBuilder().WithUpdate(updateBuilder).WithCondition(conditionBuilder).Build()
	if err != nil {
		return fmt.Errorf("error building SetExpirationDate expression: %w", err)
	}
	in := &dynamodb.UpdateItemInput{
		Key:                                 itemKeyFromRecordID(recordID),
		TableName:                           aws.String(s.table),
		ExpressionAttributeNames:            setExpirationExpression.Names(),
		ExpressionAttributeValues:           setExpirationExpression.Values(),
		UpdateExpression:                    setExpirationExpression.Update(),
		ConditionExpression:                 setExpirationExpression.Condition(),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	}
	if _, err := s.client.UpdateItem(ctx, in); err != nil {
		var conditionFailedError *types.ConditionalCheckFailedException
		if errors.As(err, &conditionFailedError) {
			if len(conditionFailedError.Item) == 0 {
				return &RecordDoesNotExistsError{RecordID: recordID}
			}
			actualStatus := conditionFailedError.Item[StatusAttrName].(*types.AttributeValueMemberS).Value
			var message strings.Builder
			message.WriteString(fmt.Sprintf("unable to set record %s expiration date to %s: current status: %s",
				recordID,
				expirationDate.Format(time.RFC3339Nano),
				actualStatus))
			if actualExpirationDateAV, ok := conditionFailedError.Item[ExpirationDateAttrName]; ok {
				message.WriteString(fmt.Sprintf(", current expiration date: %s", actualExpirationDateAV.(*types.AttributeValueMemberS).Value))
			}
			return &ConditionFailedError{message: message.String()}
		}
		return fmt.Errorf("error setting expiration date of record %s: %w", recordID, err)
	}
	return nil
}

func (s *DyDBStore) QueryExpirationIndex(ctx context.Context, now time.Time, limit int32) ([]ExpirationIndex, error) {
	var indexEntries []ExpirationIndex
	var errs []error

	keyConditionBuilder := expression.KeyAnd(
		expression.Key(StatusAttrName).Equal(expression.Value(Completed)),
		expression.Key(ExpirationDateAttrName).LessThan(expression.Value(now)))
	queryExpression, err := expression.NewBuilder().WithKeyCondition(keyConditionBuilder).Build()
	if err != nil {
		return nil, fmt.Errorf("error building QueryExpirationIndex expression: %w", err)
	}

	queryIn := &dynamodb.QueryInput{
		TableName:                 aws.String(s.table),
		IndexName:                 aws.String(ExpirationIndexName),
		ExpressionAttributeNames:  queryExpression.Names(),
		ExpressionAttributeValues: queryExpression.Values(),
		KeyConditionExpression:    queryExpression.KeyCondition(),
		Limit:                     aws.Int32(limit),
	}
	var lastEvaluatedKey map[string]types.AttributeValue
	for runQuery := true; runQuery; runQuery = len(lastEvaluatedKey) != 0 {
		queryIn.ExclusiveStartKey = lastEvaluatedKey
		queryOut, err := s.client.Query(ctx, queryIn)
		if err != nil {
			return nil, fmt.Errorf("error querying ExpirationIndex: %w", err)
		}
		lastEvaluatedKey = queryOut.LastEvaluatedKey
		for _, i := range queryOut.Items {
			if indexEntry, err := ExpirationIndexFromItem(i); err == nil {
				indexEntries = append(indexEntries, *indexEntry)
			} else {
				errs = append(errs, err)
			}
		}
	}
	return indexEntries, errors.Join(errs...)
}

func (s *DyDBStore) ExpireByIndex(ctx context.Context, index ExpirationIndex) (*Record, error) {
	updateBuilder := expression.Set(expression.Name(StatusAttrName), expression.Value(Expired))
	conditionBuilder := expression.And(
		expression.AttributeExists(expression.Name(KeyAttrName)),
		expression.Name(StatusAttrName).Equal(expression.Value(index.Status)),
		expression.Name(ExpirationDateAttrName).Equal(expression.Value(index.ExpirationDate)),
	)

	expireByIndexExpression, err := expression.NewBuilder().WithUpdate(updateBuilder).WithCondition(conditionBuilder).Build()
	if err != nil {
		return nil, fmt.Errorf("error building ExpireByIndex expression: %w", err)
	}

	in := &dynamodb.UpdateItemInput{
		Key:                                 itemKeyFromRecordID(index.ID),
		TableName:                           aws.String(s.table),
		ExpressionAttributeNames:            expireByIndexExpression.Names(),
		ExpressionAttributeValues:           expireByIndexExpression.Values(),
		UpdateExpression:                    expireByIndexExpression.Update(),
		ReturnValues:                        types.ReturnValueAllNew,
		ConditionExpression:                 expireByIndexExpression.Condition(),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	}
	out, err := s.client.UpdateItem(ctx, in)
	if err != nil {
		var conditionFailedError *types.ConditionalCheckFailedException
		if errors.As(err, &conditionFailedError) {
			if len(conditionFailedError.Item) == 0 {
				return nil, &RecordDoesNotExistsError{RecordID: index.ID}
			}
			actualStatus := conditionFailedError.Item[StatusAttrName].(*types.AttributeValueMemberS).Value
			actualExpirationDate := conditionFailedError.Item[ExpirationDateAttrName].(*types.AttributeValueMemberS).Value
			return nil,
				&ConditionFailedError{fmt.Sprintf("conditional check failed while expiring record %s: expected current status %s, actual status: %s, expected current expiration date %s, actual expiration date: %s",
					index.ID,
					index.Status,
					actualStatus,
					index.ExpirationDate,
					actualExpirationDate)}
		}
		return nil, fmt.Errorf("error updating status of record %s: %w", index.ID, err)
	}
	record, err := FromItem(out.Attributes)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling updated record: %w", err)
	}
	return record, nil
}

type RecordAlreadyExistsError struct {
	Existing           *Record
	UnmarshallingError error
}

func (e *RecordAlreadyExistsError) Error() string {
	if e.UnmarshallingError == nil {
		return fmt.Sprintf("record with ID %s already exists", e.Existing.ID)
	}
	return fmt.Sprintf("record with ID already exists; there was an error when unmarshalling existing Record: %v", e.UnmarshallingError)
}

type RecordDoesNotExistsError struct {
	RecordID string
}

func (e *RecordDoesNotExistsError) Error() string {
	return fmt.Sprintf("record with ID %s does not exist", e.RecordID)
}

type ConditionFailedError struct {
	message string
}

func (e *ConditionFailedError) Error() string {
	return e.message
}

func itemKeyFromRecordID(recordID string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{KeyAttrName: dydbutils.StringAttributeValue(recordID)}
}
