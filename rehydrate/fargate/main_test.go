package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve/models/discover"
	"github.com/pennsieve/rehydration-service/fargate/config"
	"github.com/pennsieve/rehydration-service/fargate/utils"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/models"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"testing"
)

func TestRehydrationTaskHandler(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().WithMinIO().Config(ctx, false)
	idempotencyTable := "main-test-idempotency-table"
	publishBucket := "discover-bucket"
	dataset := &models.Dataset{
		ID:        1234,
		VersionID: 3,
	}
	user := &models.User{
		Name:  "First Last",
		Email: "last@example.com",
	}

	datasetFiles := []discover.DatasetFile{
		{Name: "file1.txt", Path: "files/dir1/file1.txt", FileType: "Text"},
		{Name: "file2.txt", Path: "files/dir2/file2.txt", FileType: "Text"},
	}

	for testName, testParams := range map[string]struct {
		thresholdSize int64
	}{
		"simple copies":    {thresholdSize: ThresholdSize},
		"multipart copies": {thresholdSize: 10},
	} {

		// Set up S3 for the tests
		datasetFilesByPath := map[string]*discover.DatasetFile{}
		datasetFilesByKey := map[string]*discover.DatasetFile{}
		var putObjectInputs []*s3.PutObjectInput
		for i := range datasetFiles {
			f := &datasetFiles[i]
			body := fmt.Sprintf("content of %s\n", f.Name)
			size := int64(len([]byte(body)))
			key := fmt.Sprintf("%d/%s", dataset.ID, f.Path)
			putObjectInputs = append(putObjectInputs, &s3.PutObjectInput{
				Bucket:        aws.String(publishBucket),
				Key:           aws.String(key),
				Body:          strings.NewReader(body),
				ContentLength: aws.Int64(size),
			})
			f.Size = size
			datasetFilesByPath[f.Path] = f
			datasetFilesByKey[key] = f
		}

		s3Fixture, putObjectOutputs := test.NewS3Fixture(t, s3.NewFromConfig(awsConfig), &s3.CreateBucketInput{
			Bucket: aws.String(publishBucket),
		}).WithVersioning(publishBucket).WithObjects(putObjectInputs...)

		for location, putOutput := range putObjectOutputs {
			datasetFile, ok := datasetFilesByKey[location.Key]
			require.Truef(t, ok, "missing DatasetFile: bucket: %s, key: %s", location.Bucket, location.Key)
			datasetFile.S3VersionID = aws.ToString(putOutput.VersionId)
		}

		// Setup DynamoDB for tests
		initialIdempotencyRecord := &idempotency.Record{
			ID:             idempotency.RecordID(dataset.ID, dataset.VersionID),
			Status:         idempotency.InProgress,
			FargateTaskARN: "arn:aws:dynamoDB:test:test:test",
		}
		dyDB := test.NewDynamoDBFixture(t, awsConfig, test.IdempotencyCreateTableInput(idempotencyTable, idempotency.KeyAttrName)).WithItems(
			test.RecordsToPutItemInputs(t, idempotencyTable, initialIdempotencyRecord)...)

		// Create a mock Discover API server
		mockDiscover := test.NewDiscoverServerFixture(t, nil)
		addDatasetByVersionHandler(mockDiscover, dataset, publishBucket)
		addDatasetMetadataByVersionHandler(mockDiscover, dataset, datasetFiles)
		addDatasetFileByVersionHandler(mockDiscover, dataset, publishBucket, datasetFilesByPath)

		t.Run(testName, func(t *testing.T) {
			env := &config.Env{
				Dataset:          dataset,
				User:             user,
				TaskEnv:          "TEST",
				PennsieveHost:    mockDiscover.Server.URL,
				IdempotencyTable: idempotencyTable,
			}
			taskConfig := config.NewConfig(awsConfig, env)
			rehydrator := NewDatasetRehydrator(taskConfig, testParams.thresholdSize)
			idempotencyStore, err := taskConfig.IdempotencyStore()
			require.NoError(t, err)
			require.NoError(t, RehydrationTaskHandler(ctx, rehydrator, idempotencyStore))
			for _, datasetFile := range datasetFiles {
				expectedRehydratedKey := utils.CreateDestinationKey(dataset.ID, dataset.VersionID, datasetFile.Path)
				s3Fixture.AssertObjectExists(publishBucket, expectedRehydratedKey, datasetFile.Size)
			}
			idempotencyItems := dyDB.Scan(ctx, idempotencyTable)
			require.Len(t, idempotencyItems, 1)
			updatedIdempotencyRecord, err := idempotency.FromItem(idempotencyItems[0])
			require.NoError(t, err)
			assert.Equal(t, initialIdempotencyRecord.ID, updatedIdempotencyRecord.ID)
			assert.Equal(t, initialIdempotencyRecord.FargateTaskARN, updatedIdempotencyRecord.FargateTaskARN)
			assert.Equal(t, idempotency.Completed, updatedIdempotencyRecord.Status)
			assert.Equal(t, utils.RehydrationLocation(publishBucket, dataset.ID, dataset.VersionID), updatedIdempotencyRecord.RehydrationLocation)
		})

		// Clean up for next run
		s3Fixture.Teardown()
		dyDB.Teardown()
		mockDiscover.Teardown()
	}
}

// this is written to Go 1.21 where http.ServeMux patterns do not yet have wildcards. So this function can be made more
// general when we switch to 1.22
func addDatasetByVersionHandler(mockDiscover *test.DiscoverServerFixture, dataset *models.Dataset, expectedBucket string) {
	pattern := fmt.Sprintf("/discover/datasets/%d/versions/%d", dataset.ID, dataset.VersionID)
	respModel := discover.GetDatasetByVersionResponse{
		ID:      int32(dataset.ID),
		Name:    "test dataset",
		Version: int32(dataset.VersionID),
		Uri:     fmt.Sprintf("s3://%s/%d/", expectedBucket, dataset.ID),
	}
	mockDiscover.ModelHandlerFunc(http.MethodGet, pattern, respModel)
}

func addDatasetMetadataByVersionHandler(mockDiscover *test.DiscoverServerFixture, dataset *models.Dataset, expectedDatasetFiles []discover.DatasetFile) {
	pattern := fmt.Sprintf("/discover/datasets/%d/versions/%d/metadata", dataset.ID, dataset.VersionID)
	respModel := discover.GetDatasetMetadataByVersionResponse{
		ID:      int32(dataset.ID),
		Name:    "test dataset",
		Version: int32(dataset.VersionID),
		Files:   expectedDatasetFiles,
	}
	mockDiscover.ModelHandlerFunc(http.MethodGet, pattern, respModel)
}

func addDatasetFileByVersionHandler(mockDiscover *test.DiscoverServerFixture, dataset *models.Dataset, expectedBucket string, expectedDatasetFileByPath map[string]*discover.DatasetFile) {
	pattern := fmt.Sprintf("/discover/datasets/%d/versions/%d/files", dataset.ID, dataset.VersionID)

	mockDiscover.MultiModelHandlerFunction(http.MethodGet, pattern, func(r *http.Request) any {
		pathQueryParam := r.URL.Query().Get("path")
		datasetFile, ok := expectedDatasetFileByPath[pathQueryParam]
		if !ok {
			return nil
		}
		responseModel := discover.GetDatasetFileByVersionResponse{
			Name:        "test dataset",
			Path:        datasetFile.Path,
			Size:        datasetFile.Size,
			FileType:    datasetFile.FileType,
			Uri:         fmt.Sprintf("s3://%s/%d/%s", expectedBucket, dataset.ID, datasetFile.Path),
			S3VersionID: datasetFile.S3VersionID,
		}
		return responseModel
	})
}
