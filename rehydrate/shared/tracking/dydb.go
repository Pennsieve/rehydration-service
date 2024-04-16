package tracking

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/rehydration-service/shared/models"
	"log/slog"
	"time"
)

const TableNameKey = "REQUEST_TRACKING_DYNAMODB_TABLE_NAME"
const DatasetVersionIndexName = "DatasetVersionIndex"
const ExpirationIndexName = "ExpirationIndex"

type DyDBStore struct {
	client *dynamodb.Client
	table  string
	logger *slog.Logger
}

func NewStore(client *dynamodb.Client, logger *slog.Logger, tableName string) Store {
	return &DyDBStore{
		client: client,
		table:  tableName,
		logger: logger,
	}
}

func (s *DyDBStore) EmailSent(ctx context.Context, id string, emailSentDate *time.Time, status RehydrationStatus) error {
	updateBuilder := expression.Set(
		expression.Name(EmailSentDateAttrName),
		expression.Value(emailSentDate),
	).Set(
		expression.Name(RehydrationStatusAttrName),
		expression.Value(status),
	)
	conditionBuilder := expression.AttributeNotExists(expression.Name(EmailSentDateAttrName))
	emailSentExpression, err := expression.NewBuilder().WithUpdate(updateBuilder).WithCondition(conditionBuilder).Build()
	if err != nil {
		return fmt.Errorf("error building EmailSent expression: %w", err)
	}
	updateIn := &dynamodb.UpdateItemInput{
		Key:                                 entryItemKeyFromID(id),
		TableName:                           aws.String(s.table),
		ConditionExpression:                 emailSentExpression.Condition(),
		ExpressionAttributeNames:            emailSentExpression.Names(),
		ExpressionAttributeValues:           emailSentExpression.Values(),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		UpdateExpression:                    emailSentExpression.Update(),
	}
	if _, err = s.client.UpdateItem(ctx, updateIn); err != nil {
		var conditionFailedError *types.ConditionalCheckFailedException
		if errors.As(err, &conditionFailedError) {
			alreadyExistsError := &EntryAlreadyExistsError{}
			if existingEntry, err := FromItem(conditionFailedError.Item); err == nil {
				alreadyExistsError.Existing = existingEntry
			} else {
				alreadyExistsError.UnmarshallingError = err
			}
			return alreadyExistsError
		}
		return fmt.Errorf("error updating entry %s with emailSentDate: %s, rehydrationStatus: %s", id, emailSentDate, status)
	}
	return nil
}

func (s *DyDBStore) QueryDatasetVersionIndexUnhandled(ctx context.Context, dataset models.Dataset, limit int32) ([]DatasetVersionIndex, error) {
	var indexEntries []DatasetVersionIndex
	var errs []error

	keyConditionBuilder := expression.KeyAnd(
		expression.Key(DatasetVersionAttrName).Equal(expression.Value(dataset.DatasetVersion())),
		expression.Key(RehydrationStatusAttrName).Equal(expression.Value(InProgress)),
	)
	filterBuilder := expression.AttributeNotExists(expression.Name(EmailSentDateAttrName))
	queryExpression, err := expression.NewBuilder().WithKeyCondition(keyConditionBuilder).WithFilter(filterBuilder).Build()
	if err != nil {
		return nil, fmt.Errorf("error building QueryDatasetVersionIndexUnhandled expression: %w", err)
	}

	queryIn := &dynamodb.QueryInput{
		TableName:                 aws.String(s.table),
		IndexName:                 aws.String(DatasetVersionIndexName),
		ExpressionAttributeNames:  queryExpression.Names(),
		ExpressionAttributeValues: queryExpression.Values(),
		KeyConditionExpression:    queryExpression.KeyCondition(),
		FilterExpression:          queryExpression.Filter(),
		Limit:                     aws.Int32(limit),
	}
	var lastEvaluatedKey map[string]types.AttributeValue
	for runQuery := true; runQuery; runQuery = len(lastEvaluatedKey) != 0 {
		queryIn.ExclusiveStartKey = lastEvaluatedKey
		queryOut, err := s.client.Query(ctx, queryIn)
		if err != nil {
			return nil, fmt.Errorf("error querying DatasetVersionIndex: %w", err)
		}
		lastEvaluatedKey = queryOut.LastEvaluatedKey
		for _, i := range queryOut.Items {
			if indexEntry, err := DatasetVersionIndexFromItem(i); err == nil {
				indexEntries = append(indexEntries, *indexEntry)
			} else {
				errs = append(errs, err)
			}
		}
	}
	return indexEntries, errors.Join(errs...)
}

func (s *DyDBStore) PutEntry(ctx context.Context, entry *Entry) error {
	item, err := entry.Item()
	if err != nil {
		return err
	}
	in := dynamodb.PutItemInput{
		Item:         item,
		TableName:    aws.String(s.table),
		ReturnValues: types.ReturnValueAllOld,
	}
	out, err := s.client.PutItem(ctx, &in)
	if err != nil {
		return fmt.Errorf("error putting entry %+v to %s: %w", entry, s.table, err)
	}
	if len(out.Attributes) > 0 {
		s.logger.Warn("overwrote existing tracking entry", slog.Any("existingEntry", out.Attributes))
	}
	return nil
}

type EntryAlreadyExistsError struct {
	Existing           *Entry
	UnmarshallingError error
}

func (e *EntryAlreadyExistsError) Error() string {
	if e.UnmarshallingError == nil {
		return fmt.Sprintf("entry with ID %s already exists", e.Existing.ID)
	}
	return fmt.Sprintf("entry with ID already exists; there was an error when unmarshalling existing Entry: %v", e.UnmarshallingError)
}
