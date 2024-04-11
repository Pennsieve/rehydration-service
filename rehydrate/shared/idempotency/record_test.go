package idempotency

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRecord_ItemRoundTrip_NoExpirationDate(t *testing.T) {
	record := NewRecord("1/2/", InProgress).
		WithRehydrationLocation("bucket/1/2/")

	item, err := record.Item()
	require.NoError(t, err)
	assert.Equal(t, &types.AttributeValueMemberS{Value: record.ID}, item[KeyAttrName])
	assert.Equal(t, &types.AttributeValueMemberS{Value: record.RehydrationLocation}, item[RehydrationLocationAttrName])
	assert.Equal(t, &types.AttributeValueMemberS{Value: string(record.Status)}, item[StatusAttrName])
	assert.NotContains(t, item, ExpirationDateAttrName)

	unmarshalled, err := FromItem(item)
	require.NoError(t, err)
	assert.Equal(t, record, unmarshalled)
}

func TestRecord_ItemRoundTrip_ExpirationDate(t *testing.T) {
	expDate := time.Now().Add(time.Hour * 24)
	record := NewRecord("1/2/", Completed).
		WithRehydrationLocation("bucket/1/2/").
		WithFargateTaskARN(uuid.NewString()).
		WithExpirationDate(&expDate)

	item, err := record.Item()
	require.NoError(t, err)
	assert.Equal(t, &types.AttributeValueMemberS{Value: record.ID}, item[KeyAttrName])
	assert.Equal(t, &types.AttributeValueMemberS{Value: record.RehydrationLocation}, item[RehydrationLocationAttrName])
	assert.Equal(t, &types.AttributeValueMemberS{Value: string(record.Status)}, item[StatusAttrName])
	assert.Equal(t, &types.AttributeValueMemberS{Value: record.FargateTaskARN}, item[TaskARNAttrName])
	assert.Equal(t, &types.AttributeValueMemberS{Value: record.ExpirationDate.Format(time.RFC3339Nano)}, item[ExpirationDateAttrName])

	unmarshalled, err := FromItem(item)
	require.NoError(t, err)
	assert.Equal(t, record.ID, unmarshalled.ID)
	assert.Equal(t, record.Status, unmarshalled.Status)
	assert.Equal(t, record.RehydrationLocation, unmarshalled.RehydrationLocation)
	assert.Equal(t, record.FargateTaskARN, unmarshalled.FargateTaskARN)
	assert.True(t, record.ExpirationDate.Equal(*unmarshalled.ExpirationDate))
}

func TestStatusFromString(t *testing.T) {
	_, err := StatusFromString("NotAStatus")
	require.Error(t, err)
	require.Contains(t, err.Error(), "NotAStatus")

	complete, err := StatusFromString("completed")
	require.NoError(t, err)
	require.Equal(t, Completed, complete)

}
