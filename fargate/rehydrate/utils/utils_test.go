package utils_test

import (
	"testing"

	"github.com/pennsieve/rehydration-service/rehydrate/utils"
)

func TestCreateDestinationBucketUri(t *testing.T) {
	datasetUri := "s3://pennsieve-dev-discover-publish50-use1/5069/"
	datasetId := int32(5069)
	result, err := utils.CreateDestinationBucketUri(datasetId, datasetUri)
	expectedResult := "s3://pennsieve-dev-discover-publish50-use1/"
	if err != nil {
		t.Errorf("expected a nil error got %s", err.Error())
	}
	if result != expectedResult {
		t.Errorf("got %s, expected %s", result, expectedResult)
	}
}

func TestCreateDestinationBucketUriFailure(t *testing.T) {
	datasetUri := "s3://pennsieve-dev-discover-publish50-use1/5069/"
	datasetId := int32(5070)
	_, err := utils.CreateDestinationBucketUri(datasetId, datasetUri)
	if err.Error() != "error creating destinationBucketUri" {
		t.Errorf("expected an error to be thrown")
	}
}

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
