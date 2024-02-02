package test

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAWSEndpointMap_WithSQS(t *testing.T) {
	mockSQSURL := "http://mock-sqs-endpoint"
	m := NewAwsEndpointMap().WithSQS(mockSQSURL)
	require.Contains(t, m, sqs.ServiceID)
	require.Equal(t, mockSQSURL, m[sqs.ServiceID].URL)
}
