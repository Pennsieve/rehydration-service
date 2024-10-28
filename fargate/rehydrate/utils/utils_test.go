package utils_test

import (
	"testing"

	"github.com/pennsieve/rehydration-service/rehydrate/utils"
	"github.com/stretchr/testify/assert"
)

func TestCreateDestinationKey(t *testing.T) {
	datasetId := int32(5070)
	versionId := int32(2)
	path := "files/testfile.txt"
	destinationKey := utils.CreateDestinationKey(datasetId, versionId, path)
	expectedDestinationKey := "rehydrated/5070/2/files/testfile.txt"
	if destinationKey != expectedDestinationKey {
		t.Errorf("expected %s, got %s", expectedDestinationKey, destinationKey)
	}
}

func TestCreateDestinationBucket(t *testing.T) {
	datasetUri := "s3://pennsieve-dev-discover-publish50-use1/5069/"
	result, err := utils.CreateDestinationBucket(datasetUri)
	expectedResult := "pennsieve-dev-discover-publish50-use1"
	if err != nil {
		t.Errorf("expected a nil error got %s", err.Error())
	}
	if result != expectedResult {
		t.Errorf("got %s, expected %s", result, expectedResult)
	}
}

func TestCreateVersionedSource(t *testing.T) {
	datasetUri := "s3://pennsieve-dev-discover-publish50-use1/5069/metadata/schema.json"
	version := "48iKzZl_XnOKz4M8XgEq1IhkzEItv5eU"
	result := utils.CreateVersionedSource(datasetUri, version)
	expectedResult := "pennsieve-dev-discover-publish50-use1/5069/metadata/schema.json?versionId=48iKzZl_XnOKz4M8XgEq1IhkzEItv5eU"
	if result != expectedResult {
		t.Errorf("got %s, expected %s", result, expectedResult)
	}
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
