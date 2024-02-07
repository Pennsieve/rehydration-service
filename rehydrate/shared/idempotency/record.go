package idempotency

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strings"
	"time"
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

// idempotencyKeyAttrName is the name of the idempotency key attribute in the DynamoDB item representing a Record.
// Must match the struct tag for Record.Key, but there does not seem to be an easy way to enforce this.
const idempotencyKeyAttrName = "key"

type Record struct {
	Key                 string    `dynamodbav:"key"`
	RehydrationLocation string    `dynamodbav:"rehydrationLocation"`
	Status              Status    `dynamodbav:"status"`
	ExpiryTimestamp     time.Time `dynamodbav:"expiryTimestamp,unixtime"`
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
	record.ExpiryTimestamp = record.ExpiryTimestamp.UTC()
	return &record, nil
}
