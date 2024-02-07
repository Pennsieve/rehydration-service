package idempotency

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRecord_ItemRoundTrip(t *testing.T) {
	unixEpoch := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	record := Record{
		Key:                 "1/2/",
		RehydrationLocation: "bucket/1/2/",
		Status:              InProgress,
		ExpiryTimestamp:     unixEpoch,
	}

	item, err := record.Item()
	require.NoError(t, err)
	assert.Equal(t, &types.AttributeValueMemberS{Value: record.Key}, item["key"])
	assert.Equal(t, &types.AttributeValueMemberS{Value: record.RehydrationLocation}, item["rehydrationLocation"])
	assert.Equal(t, &types.AttributeValueMemberS{Value: string(InProgress)}, item["status"])
	assert.Equal(t, &types.AttributeValueMemberN{Value: "0"}, item["expiryTimestamp"])

	unmarshalled, err := FromItem(item)
	require.NoError(t, err)
	assert.Equal(t, record, *unmarshalled)

}
