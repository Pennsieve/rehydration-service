package utils_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pennsieve/rehydration-service/fargate/utils"
)

func TestCreateDestinationKey(t *testing.T) {
	datasetId := 5070
	versionId := 2
	path := "files/testfile.txt"
	destinationKey := utils.DestinationKey(datasetId, versionId, path)
	expectedDestinationKey := "5070/2/files/testfile.txt"
	if destinationKey != expectedDestinationKey {
		t.Errorf("expected %s, got %s", expectedDestinationKey, destinationKey)
	}
}

func TestCreateVersionedSource(t *testing.T) {
	datasetUri := "s3://pennsieve-dev-discover-publish50-use1/5069/metadata/schema.json"
	version := "48iKzZl_XnOKz4M8XgEq1IhkzEItv5eU"
	result, err := utils.VersionedCopySource(datasetUri, version)
	require.NoError(t, err)
	expectedResult := "pennsieve-dev-discover-publish50-use1/5069/metadata/schema.json?versionId=48iKzZl_XnOKz4M8XgEq1IhkzEItv5eU"
	if result != expectedResult {
		t.Errorf("got %s, expected %s", result, expectedResult)
	}

	// escaped path with spaces
	datasetUri = "s3://dev-discover50-use1/85/files/primary/sub-P333/P777 Embedding Schematic.pptx"
	version = "48iKzZl_XnOKz4M8XgEq1IhkzEItv5eU"
	result, err = utils.VersionedCopySource(datasetUri, version)
	require.NoError(t, err)
	expectedResult = "dev-discover50-use1/85/files/primary/sub-P333/P777%20Embedding%20Schematic.pptx?versionId=48iKzZl_XnOKz4M8XgEq1IhkzEItv5eU"
	if result != expectedResult {
		t.Errorf("got %s, expected %s", result, expectedResult)
	}
}

func TestRehydrationLocation(t *testing.T) {
	destinationBucket := "destination-bucket"
	datasetId := 5070
	versionId := 2
	expectedLocation := fmt.Sprintf("s3://%s/%d/%d/", destinationBucket, datasetId, versionId)
	location := utils.RehydrationLocation(destinationBucket, datasetId, versionId)
	require.Equal(t, expectedLocation, location)
}

func TestCreateURLEscapedPath(t *testing.T) {
	path := "files/primary/sub-P786/P786 Embedding Schematic.pptx"
	result := utils.CreateURLEscapedPath(path)
	assert.Equal(t, result, "files%2Fprimary%2Fsub-P786%2FP786+Embedding+Schematic.pptx")
	// paths not requiring url encoding are left as-is
	path = "P786EmbeddingSchematic.pptx"
	result = utils.CreateURLEscapedPath(path)
	assert.Equal(t, result, "P786EmbeddingSchematic.pptx")
}

func TestCreateAWSEscapedPath(t *testing.T) {
	path := "files/primary/sub-P786/P786 Embedding Schematic.pptx"
	result := utils.CreateAWSEscapedPath(path)
	assert.Equal(t, *result, "files/primary/sub-P786/P786%20Embedding%20Schematic.pptx")

	// paths not requiring url encoding are left as-is
	path = "files/primary/sub-P786/P786EmbeddingSchematic.pptx"
	result = utils.CreateAWSEscapedPath(path)
	assert.Equal(t, *result, "files/primary/sub-P786/P786EmbeddingSchematic.pptx")
}
