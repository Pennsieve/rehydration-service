package dydbutils

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestFromItem(t *testing.T) {
	structTag := "timestamp"
	type ItemStruct struct {
		Timestamp time.Time `dynamodbav:"timestamp"`
	}
	fromItem := FromItem[ItemStruct]

	goodTime := time.Now().Format(time.RFC3339Nano)
	good := map[string]types.AttributeValue{structTag: StringAttributeValue(goodTime)}
	goodStruct, err := fromItem(good)
	require.NoError(t, err)
	assert.Equal(t, goodTime, goodStruct.Timestamp.Format(time.RFC3339Nano))

	badTime := time.Now().Format(time.TimeOnly)
	bad := map[string]types.AttributeValue{structTag: StringAttributeValue(badTime)}
	_, err = fromItem(bad)
	assert.ErrorContains(t, err, fmt.Sprintf("%T", ItemStruct{}))
}

func TestItemImpl(t *testing.T) {
	structTag := "timestamp"
	type ItemStruct struct {
		Timestamp time.Time `dynamodbav:"timestamp"`
	}

	value := ItemStruct{Timestamp: time.Now()}
	asItem, err := ItemImpl(value)
	require.NoError(t, err)
	assert.Len(t, asItem, 1)
	assert.Contains(t, asItem, structTag)
	stringAttribute, ok := asItem[structTag].(*types.AttributeValueMemberS)
	if assert.True(t, ok) {
		assert.Equal(t, value.Timestamp.Format(time.RFC3339Nano), stringAttribute.Value)
	}
}
