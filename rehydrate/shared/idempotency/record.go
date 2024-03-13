package idempotency

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/rehydration-service/shared/models"
	"strings"
)

type Status string

const (
	InProgress Status = "IN_PROGRESS"
	Completed  Status = "COMPLETED"
	Expired    Status = "EXPIRED"
)

func StatusFromString(s string) (Status, error) {
	switch strings.ToUpper(s) {
	case string(InProgress):
		return InProgress, nil
	case string(Completed):
		return Completed, nil
	case string(Expired):
		return Expired, nil
	default:
		return "", fmt.Errorf("unknown idempotency status: [%s]", s)
	}
}

// KeyAttrName is the name of the idempotency key attribute in the DynamoDB item representing a Record.
// Must match the struct tag for Record.ID, but there does not seem to be an easy way to enforce this.
const KeyAttrName = "id"
const idempotencyRehydrationLocationAttrName = "RehydrationLocation"
const idempotencyStatusAttrName = "status"
const idempotencyTaskARNAttrName = "fargateTaskARN"

type Record struct {
	ID                  string `dynamodbav:"id"`
	RehydrationLocation string `dynamodbav:"RehydrationLocation"`
	Status              Status `dynamodbav:"status"`
	FargateTaskARN      string `dynamodbav:"fargateTaskARN"`
}

func (r *Record) Item() (map[string]types.AttributeValue, error) {
	item, err := attributevalue.MarshalMap(r)
	if err != nil {
		return nil, fmt.Errorf("error marshalling Record %+v to DynamoDB item: %w", r, err)

	}
	return item, nil
}

func FromItem(item map[string]types.AttributeValue) (*Record, error) {
	var record Record
	if err := attributevalue.UnmarshalMap(item, &record); err != nil {
		return nil, fmt.Errorf("error unmarshalling item to Record: %w", err)
	}
	return &record, nil
}

func RecordID(datasetID, datasetVersionID int) string {
	return models.DatasetVersion(datasetID, datasetVersionID)
}
