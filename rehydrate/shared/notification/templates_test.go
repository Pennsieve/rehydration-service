package notification

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLoadTemplates(t *testing.T) {
	require.NoError(t, LoadTemplates())
	assert.NotNil(t, rehydrationCompleteTemplate)
	assert.NotNil(t, rehydrationFailedTemplate)
}

func TestRehydrationCompleteEmailBody(t *testing.T) {
	require.NoError(t, LoadTemplates())
	datasetID := 1234
	datasetVersionID := 2
	rehydrationLocation := fmt.Sprintf("s3://bucket/rehydrated/%d/%d", datasetID, datasetVersionID)

	body, err := RehydrationCompleteEmailBody(datasetID, datasetVersionID, rehydrationLocation)
	require.NoError(t, err)
	assert.Contains(t, body, "Rehydration Complete")
	assert.Contains(t, body, rehydrationLocation)
	assert.Contains(t, body, fmt.Sprintf("Dataset %d version %d", datasetID, datasetVersionID))

}

func TestRehydrationFailedEmailBody(t *testing.T) {
	require.NoError(t, LoadTemplates())
	datasetID := 6803
	datasetVersionID := 1
	requestID := uuid.NewString()

	body, err := RehydrationFailedEmailBody(datasetID, datasetVersionID, requestID)
	require.NoError(t, err)
	assert.Contains(t, body, "Rehydration Failed")
	assert.Contains(t, body, requestID)
	assert.Contains(t, body, fmt.Sprintf("Dataset %d version %d", datasetID, datasetVersionID))
}
