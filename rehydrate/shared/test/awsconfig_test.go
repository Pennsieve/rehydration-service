package test

import (
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAWSEndpoints_WithSES(t *testing.T) {
	mockSESURL := "http://mock-ses-endpoint"
	m := NewAWSEndpoints(t).WithSES(mockSESURL)
	require.Contains(t, m.serviceIDToEndpoint, ses.ServiceID)
	require.Equal(t, mockSESURL, m.serviceIDToEndpoint[ses.ServiceID].URL)
}
