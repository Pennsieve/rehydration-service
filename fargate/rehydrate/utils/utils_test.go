package utils_test

import (
	"testing"

	"github.com/pennsieve/rehydration-service/rehydrate/utils"
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
	datasetUri := "s3://pennsieve-dev-discover-publish50-use1/5069/"
	path := "metadata/schema.json"
	version := "48iKzZl_XnOKz4M8XgEq1IhkzEItv5eU"
	result := utils.CreateVersionedSource(datasetUri, path, version)
	expectedResult := "pennsieve-dev-discover-publish50-use1/5069/metadata/schema.json?versionId=48iKzZl_XnOKz4M8XgEq1IhkzEItv5eU"
	if result != expectedResult {
		t.Errorf("got %s, expected %s", result, expectedResult)
	}
}
