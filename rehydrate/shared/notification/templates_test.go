package notification

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestLoadTemplates(t *testing.T) {
	require.NoError(t, LoadTemplates())

	var emailBuilder strings.Builder

	emailData := RehydrationComplete{
		DatasetID:           1569,
		DatasetVersionID:    4,
		RehydrationLocation: "s3://bucket/rehydrated/1569/4",
	}
	require.NoError(t, RehydrationCompleteTemplate.Execute(&emailBuilder, emailData))

	emailBody := emailBuilder.String()
	assert.Contains(t, emailBody, emailData.RehydrationLocation)
	assert.Contains(t, emailBody, fmt.Sprintf("Dataset %d version %d", emailData.DatasetID, emailData.DatasetVersionID))
}
