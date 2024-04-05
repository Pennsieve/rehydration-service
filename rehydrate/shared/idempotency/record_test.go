package idempotency

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRecord_ItemRoundTrip(t *testing.T) {
	record := NewRecord("1/2/", InProgress).
		WithRehydrationLocation("bucket/1/2/")

	item, err := record.Item()
	require.NoError(t, err)
	assert.Equal(t, &types.AttributeValueMemberS{Value: record.ID}, item["id"])
	assert.Equal(t, &types.AttributeValueMemberS{Value: record.RehydrationLocation}, item["RehydrationLocation"])
	assert.Equal(t, &types.AttributeValueMemberS{Value: string(InProgress)}, item["status"])

	unmarshalled, err := FromItem(item)
	require.NoError(t, err)
	require.Equal(t, record, unmarshalled)
}

func TestStatusFromString(t *testing.T) {
	_, err := StatusFromString("NotAStatus")
	require.Error(t, err)
	require.Contains(t, err.Error(), "NotAStatus")

	complete, err := StatusFromString("completed")
	require.NoError(t, err)
	require.Equal(t, Completed, complete)

}
