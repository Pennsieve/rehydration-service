package tracking

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/rehydration-service/shared/models"
	"log/slog"
	"time"
)

const TableNameKey = "REQUEST_TRACKING_DYNAMODB_TABLE_NAME"
const DatasetVersionIndexName = "DatasetVersionIndex"

type DyDBStore struct {
	client *dynamodb.Client
	table  string
	logger *slog.Logger
}

func NewStore(config aws.Config, logger *slog.Logger, tableName string) Store {
	return newDyDBStore(config, logger, tableName)
}

func newDyDBStore(config aws.Config, logger *slog.Logger, tableName string) *DyDBStore {
	client := dynamodb.NewFromConfig(config)
	return &DyDBStore{
		client: client,
		table:  tableName,
		logger: logger,
	}
}

func (s *DyDBStore) NewInProgressEntry(ctx context.Context, id string, dataset models.Dataset, user models.User, lambdaLogStream, awsRequestID, fargateTaskARN string) error {
	requestDate := time.Now()
	inProgress := &Entry{
		DatasetVersionIndex: DatasetVersionIndex{
			ID:                id,
			DatasetVersion:    dataset.DatasetVersion(),
			UserName:          user.Name,
			UserEmail:         user.Email,
			RehydrationStatus: InProgress,
		},
		LambdaLogStream: lambdaLogStream,
		AWSRequestID:    awsRequestID,
		RequestDate:     requestDate,
		FargateTaskARN:  fargateTaskARN,
	}
	return s.PutEntry(ctx, inProgress)
}

func (s *DyDBStore) NewFailedEntry(ctx context.Context, id string, dataset models.Dataset, user models.User, lambdaLogStream, awsRequestID string) error {
	requestDate := time.Now()
	inProgress := &Entry{
		DatasetVersionIndex: DatasetVersionIndex{
			ID:                id,
			DatasetVersion:    dataset.DatasetVersion(),
			UserName:          user.Name,
			UserEmail:         user.Email,
			RehydrationStatus: Failed,
		},
		LambdaLogStream: lambdaLogStream,
		AWSRequestID:    awsRequestID,
		RequestDate:     requestDate,
	}
	return s.PutEntry(ctx, inProgress)
}

func (s *DyDBStore) NewUnknownEntry(ctx context.Context, id string, dataset models.Dataset, user models.User, lambdaLogStream, awsRequestID string) error {
	requestDate := time.Now()
	inProgress := &Entry{
		DatasetVersionIndex: DatasetVersionIndex{
			ID:                id,
			DatasetVersion:    dataset.DatasetVersion(),
			UserName:          user.Name,
			UserEmail:         user.Email,
			RehydrationStatus: Unknown,
		},
		LambdaLogStream: lambdaLogStream,
		AWSRequestID:    awsRequestID,
		RequestDate:     requestDate,
	}
	return s.PutEntry(ctx, inProgress)
}

func (s *DyDBStore) EmailSent(ctx context.Context, id string, emailSentDate time.Time, status RehydrationStatus) error {
	expressionAttrNames := map[string]string{
		"#emailSentDate": EmailSentDateAttrName,
		"#status":        RehydrationStatusAttrName,
	}
	temp := &Entry{
		DatasetVersionIndex: DatasetVersionIndex{
			RehydrationStatus: status,
		},
		EmailSentDate: &emailSentDate,
	}
	expressionValues, err := temp.Item()
	if err != nil {
		return fmt.Errorf("error marshalling emailSentDate %s and rehydrationStatus %s: %w", emailSentDate, status, err)
	}
	expressionAttrValues := map[string]types.AttributeValue{
		":emailSentDate": expressionValues[EmailSentDateAttrName],
		":status":        expressionValues[RehydrationStatusAttrName],
	}
	updateExpression := "SET #emailSentDate = :emailSentDate, #status = :status"
	conditionExpression := fmt.Sprintf("attribute_not_exists(%s)", EmailSentDateAttrName)

	updateIn := &dynamodb.UpdateItemInput{
		Key:                                 entryItemKeyFromID(id),
		TableName:                           aws.String(s.table),
		ConditionExpression:                 aws.String(conditionExpression),
		ExpressionAttributeNames:            expressionAttrNames,
		ExpressionAttributeValues:           expressionAttrValues,
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		UpdateExpression:                    aws.String(updateExpression),
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

func (s *DyDBStore) QueryDatasetVersionIndex(ctx context.Context, dataset models.Dataset, limit int32) ([]DatasetVersionIndex, error) {
	var indexEntries []DatasetVersionIndex
	var errs []error
	datasetVersionTerm := ":datasetVersion"
	keyCondition := fmt.Sprintf("%s = %s", DatasetVersionAttrName, datasetVersionTerm)
	expressionValues := map[string]types.AttributeValue{datasetVersionTerm: stringAttributeValue(dataset.DatasetVersion())}
	queryIn := &dynamodb.QueryInput{
		TableName:                 aws.String(s.table),
		IndexName:                 aws.String(DatasetVersionIndexName),
		ExpressionAttributeValues: expressionValues,
		KeyConditionExpression:    aws.String(keyCondition),
		FilterExpression:          nil,
		Limit:                     aws.Int32(limit),
		ExclusiveStartKey:         nil,
	}
	queryOut, err := s.client.Query(ctx, queryIn)
	if err != nil {
		return nil, fmt.Errorf("error querying DatasetVersionIndex: %w", err)
	}
	for _, i := range queryOut.Items {
		if indexEntry, err := DatasetVersionIndexFromItem(i); err == nil {
			indexEntries = append(indexEntries, *indexEntry)
		} else {
			errs = append(errs, err)
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
