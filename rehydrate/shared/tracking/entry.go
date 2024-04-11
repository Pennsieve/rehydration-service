package tracking

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/rehydration-service/shared/dydbutils"
	"github.com/pennsieve/rehydration-service/shared/models"
	"strings"
	"time"
)

// RehydrationStatus indicates only the current status of the rehydration Fargate task
type RehydrationStatus string

const (
	// Unknown is only for the situation where the Lambda handling the endpoint fails before it can start the
	// Fargate task or determine if one is already started. It should be an abnormal status
	Unknown    RehydrationStatus = "UNKNOWN"
	InProgress RehydrationStatus = "IN_PROGRESS"
	Completed  RehydrationStatus = "COMPLETED"
	Expired    RehydrationStatus = "EXPIRED"
	Failed     RehydrationStatus = "FAILED"
)

func RehydrationStatusFromString(s string) (RehydrationStatus, error) {
	switch strings.ToUpper(s) {
	case string(InProgress):
		return InProgress, nil
	case string(Completed):
		return Completed, nil
	case string(Failed):
		return Failed, nil
	default:
		return "", fmt.Errorf("unknown rehydration status: [%s]", s)
	}
}

// IDAttrName and other attribute name constants below should match the values in the dynamodbav struct tags in the Entry,
// and DatasetVersionIndex structs.
const IDAttrName = "id"
const DatasetVersionAttrName = "datasetVersion"
const UserNameAttrName = "userName"
const UserEmailAttrName = "userEmail"
const LambdaLogStreamAttrName = "lambdaLogStream"
const AWSRequestIDAttrName = "awsRequestId"
const RequestDateAttrName = "requestDate"
const RehydrationStatusAttrName = "rehydrationStatus"
const EmailSentDateAttrName = "emailSentDate"
const FargateTaskARNAttrName = "fargateTaskARN"

// DatasetVersionIndex represents a Global Secondary Index to the Entry table.
// The partition key of this index is DatasetVersion so that when a rehydration Fargate
// task completes it can look up all the users that requested that DatasetVersion and
// send an email and update the main Entry item with an email sent date and new RehydrationStatus.
// The id and user's name and email are included in this index so that we can do this without first looking up the full
// Entry from the table.
type DatasetVersionIndex struct {
	ID                string            `dynamodbav:"id"`
	DatasetVersion    string            `dynamodbav:"datasetVersion"`
	UserName          string            `dynamodbav:"userName"`
	UserEmail         string            `dynamodbav:"userEmail"`
	RehydrationStatus RehydrationStatus `dynamodbav:"rehydrationStatus"`
	// EmailSentDate is a pointer because omitempty does not work with time.Time:
	// https://github.com/aws/aws-sdk-go/issues/2040 (issue is for the V1 SDK, but I saw the same thing with V2)
	// This is the cleanest way to ensure that entries that haven't had their email sent date result in table items
	// with no email sent date field attribute instead of having the attribute set to the time.Time zero value 0001-01-01T00:00:00Z
	EmailSentDate *time.Time `dynamodbav:"emailSentDate,omitempty"`
}
type Entry struct {
	DatasetVersionIndex
	LambdaLogStream string    `dynamodbav:"lambdaLogStream"`
	AWSRequestID    string    `dynamodbav:"awsRequestId"`
	RequestDate     time.Time `dynamodbav:"requestDate"`
	FargateTaskARN  string    `dynamodbav:"fargateTaskARN,omitempty"`
}

func NewEntry(id string, dataset models.Dataset, user models.User, lambdaLogStream, awsRequestID, fargateTaskARN string) *Entry {
	requestDate := time.Now()
	return &Entry{
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
}

func (r *Entry) Item() (map[string]types.AttributeValue, error) {
	return dydbutils.ItemImpl(r)
}

var FromItem = dydbutils.FromItem[Entry]

var DatasetVersionIndexFromItem = dydbutils.FromItem[DatasetVersionIndex]

func entryItemKeyFromID(id string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{IDAttrName: dydbutils.StringAttributeValue(id)}
}
