package test

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAWSEndpoints_WithSQS(t *testing.T) {
	mockSQSURL := "http://mock-sqs-endpoint"
	m := NewAWSEndpoints(t).WithSQS(mockSQSURL)
	require.Contains(t, m.serviceIDToEndpoint, sqs.ServiceID)
	require.Equal(t, mockSQSURL, m.serviceIDToEndpoint[sqs.ServiceID].URL)
}
