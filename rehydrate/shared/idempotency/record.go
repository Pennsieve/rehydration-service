package idempotency

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/rehydration-service/shared/dydbutils"
	"github.com/pennsieve/rehydration-service/shared/models"
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

// KeyAttrName is the name of the idempotency key attribute in the DynamoDB item representing a Record.
// Must match the struct tag for Record.ID, but there does not seem to be an easy way to enforce this.
const KeyAttrName = "id"
const RehydrationLocationAttrName = "rehydrationLocation"
const StatusAttrName = "status"
const TaskARNAttrName = "fargateTaskARN"
const ExpirationDateAttrName = "expirationDate"

const ExpirationIndexName = "ExpirationIndex"

type ExpirationIndex struct {
	ID                  string `dynamodbav:"id"`
	RehydrationLocation string `dynamodbav:"rehydrationLocation,omitempty"`
	Status              Status `dynamodbav:"status"`
	// ExpirationDate is a pointer because omitempty does not work with time.Time:
	// https://github.com/aws/aws-sdk-go/issues/2040 (issue is for the V1 SDK, but I saw the same thing with V2)
	// This is the cleanest way to ensure that entries that haven't had their expiration date set result in table items
	// with no expiration date field attribute instead of having the attribute set to the time.Time zero value 0001-01-01T00:00:00Z
	ExpirationDate *time.Time `dynamodbav:"expirationDate,omitempty"`
}
type Record struct {
	ExpirationIndex
	FargateTaskARN string `dynamodbav:"fargateTaskARN,omitempty"`
}

func NewRecord(id string, status Status) *Record {
	return &Record{
		ExpirationIndex: ExpirationIndex{
			ID:     id,
			Status: status,
		}}
}

func (r *Record) WithRehydrationLocation(location string) *Record {
	r.RehydrationLocation = location
	return r
}

func (r *Record) WithFargateTaskARN(taskARN string) *Record {
	r.FargateTaskARN = taskARN
	return r
}

func (r *Record) WithExpirationDate(expirationDate *time.Time) *Record {
	r.ExpirationDate = expirationDate
	return r
}

func (r *Record) Item() (map[string]types.AttributeValue, error) {
	return dydbutils.ItemImpl(r)
}

func (e *ExpirationIndex) Item() (map[string]types.AttributeValue, error) {
	return dydbutils.ItemImpl(e)
}

var FromItem = dydbutils.FromItem[Record]

var ExpirationIndexFromItem = dydbutils.FromItem[ExpirationIndex]

func RecordID(datasetID, datasetVersionID int) string {
	return models.DatasetVersion(datasetID, datasetVersionID)
}
