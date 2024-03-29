package utils_test

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/pennsieve/rehydration-service/fargate/utils"
)

func TestCreateDestinationKey(t *testing.T) {
	datasetId := 5070
	versionId := 2
	path := "files/testfile.txt"
	destinationKey := utils.CreateDestinationKey(datasetId, versionId, path)
	expectedDestinationKey := "5070/2/files/testfile.txt"
	if destinationKey != expectedDestinationKey {
		t.Errorf("expected %s, got %s", expectedDestinationKey, destinationKey)
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

func TestRehydrationLocation(t *testing.T) {
	destinationBucket := "destination-bucket"
	datasetId := 5070
	versionId := 2
	expectedLocation := fmt.Sprintf("s3://%s/%d/%d", destinationBucket, datasetId, versionId)
	location := utils.RehydrationLocation(destinationBucket, datasetId, versionId)
	require.Equal(t, expectedLocation, location)
}
