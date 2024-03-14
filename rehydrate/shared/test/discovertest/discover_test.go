package discovertest

import (
	"github.com/pennsieve/rehydration-service/shared/models"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewTestDatasetFiles(t *testing.T) {
	testDatasetFiles := NewTestDatasetFiles(models.Dataset{
		ID:        1,
		VersionID: 2,
	}, 1).WithFakeS3VersionsIDs()

	files := testDatasetFiles.Files

	for i := 0; i < len(files); i++ {
		byPath := testDatasetFiles.ByPath[files[i].Path]
		assert.Same(t, &files[i], byPath)
		assert.NotEmpty(t, files[i].S3VersionID)
		assert.Equal(t, files[i].S3VersionID, byPath.S3VersionID)
	}
}
