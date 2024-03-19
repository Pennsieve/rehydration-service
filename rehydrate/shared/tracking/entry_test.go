package tracking_test

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/pennsieve/rehydration-service/shared/tracking"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestEntry_ItemRoundTrip(t *testing.T) {
	emailSentDate := time.Now()
	requestDate := emailSentDate.Add(-time.Hour * 3)

	entry := &tracking.Entry{
		DatasetVersionIndex: tracking.DatasetVersionIndex{
			ID:                uuid.NewString(),
			DatasetVersion:    "451/3/",
			UserName:          "First Last",
			UserEmail:         "last@example.com",
			RehydrationStatus: tracking.Completed,
			EmailSentDate:     &emailSentDate,
		},
		LambdaLogStream: "/lambda/log/stream/name",
		AWSRequestID:    "REQUEST-1234",
		RequestDate:     requestDate,
		FargateTaskARN:  "arn:ecs:test:test:test",
	}

	item, err := entry.Item()
	require.NoError(t, err)
	AssertEqualEntryItem(t, *entry, item)

	unmarshalled, err := tracking.FromItem(item)
	require.NoError(t, err)

	// Can't compare time.Times that have been through a serialization/deserialization process, so compare fields individually
	assert.Equal(t, entry.ID, unmarshalled.ID)
	assert.Equal(t, entry.DatasetVersion, unmarshalled.DatasetVersion)
	assert.Equal(t, entry.UserName, unmarshalled.UserName)
	assert.Equal(t, entry.UserEmail, unmarshalled.UserEmail)
	assert.Equal(t, entry.LambdaLogStream, unmarshalled.LambdaLogStream)
	assert.Equal(t, entry.AWSRequestID, unmarshalled.AWSRequestID)
	assert.Equal(t, entry.RehydrationStatus, unmarshalled.RehydrationStatus)
	assert.Equal(t, entry.FargateTaskARN, unmarshalled.FargateTaskARN)

	assert.Equal(t, entry.RequestDate.Format(time.RFC3339Nano), unmarshalled.RequestDate.Format(time.RFC3339Nano))
	assert.Equal(t, entry.EmailSentDate.Format(time.RFC3339Nano), entry.EmailSentDate.Format(time.RFC3339Nano))

}

func TestStatusFromString(t *testing.T) {
	_, err := tracking.RehydrationStatusFromString("NotAStatus")
	require.Error(t, err)
	require.Contains(t, err.Error(), "NotAStatus")

	complete, err := tracking.RehydrationStatusFromString("completed")
	require.NoError(t, err)
	require.Equal(t, tracking.Completed, complete)

}

func AssertEqualAttributeValueString(t *testing.T, expectedValue string, attrValue types.AttributeValue) bool {
	return assert.Equal(t, &types.AttributeValueMemberS{Value: expectedValue}, attrValue)
}
func AssertEqualEntryItem(t *testing.T, entry tracking.Entry, item map[string]types.AttributeValue) bool {
	result := AssertEqualAttributeValueString(t, entry.ID, item[tracking.IDAttrName])
	result = result && AssertEqualAttributeValueString(t, entry.DatasetVersion, item[tracking.DatasetVersionAttrName])
	result = result && AssertEqualAttributeValueString(t, entry.UserName, item[tracking.UserNameAttrName])
	result = result && AssertEqualAttributeValueString(t, entry.UserEmail, item[tracking.UserEmailAttrName])
	result = result && AssertEqualAttributeValueString(t, entry.LambdaLogStream, item[tracking.LambdaLogStreamAttrName])
	result = result && AssertEqualAttributeValueString(t, entry.AWSRequestID, item[tracking.AWSRequestIDAttrName])
	result = result && AssertEqualAttributeValueString(t, string(entry.RehydrationStatus), item[tracking.RehydrationStatusAttrName])
	if len(entry.FargateTaskARN) == 0 {
		// testing omitempty
		result = result && assert.NotContains(t, item, tracking.FargateTaskARNAttrName)
	} else {
		result = result && AssertEqualAttributeValueString(t, entry.FargateTaskARN, item[tracking.FargateTaskARNAttrName])
	}
	result = result && AssertEqualAttributeValueString(t, entry.RequestDate.Format(time.RFC3339Nano), item[tracking.RequestDateAttrName])
	if entry.EmailSentDate == nil {
		// testing omitempty
		result = result && assert.NotContains(t, item, tracking.EmailSentDateAttrName)
	} else {
		result = result && AssertEqualAttributeValueString(t, entry.EmailSentDate.Format(time.RFC3339Nano), item[tracking.EmailSentDateAttrName])
	}
	return result
}
