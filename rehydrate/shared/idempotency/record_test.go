package idempotency

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRecord_ItemRoundTrip(t *testing.T) {
	timestamp := time.Now()
	record := Record{
		ID:                  "1/2/",
		RehydrationLocation: "bucket/1/2/",
		Status:              InProgress,
		ExpiryTimestamp:     timestamp,
	}

	item, err := record.Item()
	require.NoError(t, err)
	fmt.Println(item["expiryTimestamp"])
	assert.Equal(t, &types.AttributeValueMemberS{Value: record.ID}, item["id"])
	assert.Equal(t, &types.AttributeValueMemberS{Value: record.RehydrationLocation}, item["rehydrationLocation"])
	assert.Equal(t, &types.AttributeValueMemberS{Value: string(InProgress)}, item["status"])
	assert.Equal(t, &types.AttributeValueMemberS{Value: timestamp.Format(time.RFC3339Nano)}, item["expiryTimestamp"])

	unmarshalled, err := FromItem(item)
	require.NoError(t, err)
	require.True(t, record.Equal(*unmarshalled))
}
