package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/rehydration-service/fargate/config"
	"github.com/pennsieve/rehydration-service/fargate/utils"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/pennsieve/rehydration-service/shared/test/discovertest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log/slog"
	"net/http"
	"testing"
)

func TestRehydrate(t *testing.T) {
	test.SetLogLevel(t, slog.LevelError)
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithMinIO().Config(ctx, false)
	publishBucket := "discover-bucket"
	taskEnv := newTestConfigEnv()
	dataset := taskEnv.Dataset

	datasetFileCount := 50
	testDatasetFiles := discovertest.NewTestDatasetFiles(*dataset, datasetFileCount)

	for testName, testParams := range map[string]struct {
		thresholdSize int64
	}{
		"simple copies":    {thresholdSize: ThresholdSize},
		"multipart copies": {thresholdSize: 10},
	} {

		t.Run(testName, func(t *testing.T) {
			// Set up S3 for the tests
			s3Fixture, putObjectOutputs := test.NewS3Fixture(t, s3.NewFromConfig(awsConfig),
				&s3.CreateBucketInput{Bucket: aws.String(publishBucket)},
				&s3.CreateBucketInput{Bucket: aws.String(taskEnv.RehydrationBucket)},
			).WithVersioning(publishBucket).WithObjects(testDatasetFiles.PutObjectInputs(publishBucket)...)
			defer s3Fixture.Teardown()

			// Set S3 versionIds
			for location, putOutput := range putObjectOutputs {
				testDatasetFiles.SetS3VersionID(t, location, aws.ToString(putOutput.VersionId))
			}

			// Create a mock Discover API server
			mockDiscover := discovertest.NewServerFixture(t, nil,
				discovertest.GetDatasetMetadataByVersionHandlerBuilder(*dataset, testDatasetFiles.DatasetFiles()),
				discovertest.GetDatasetFileByVersionHandlerBuilder(*dataset, publishBucket, testDatasetFiles.ByPath),
			)
			defer mockDiscover.Teardown()

			taskEnv.PennsieveHost = mockDiscover.Server.URL
			taskConfig := config.NewConfig(awsConfig, taskEnv)
			rehydrator := NewDatasetRehydrator(taskConfig, testParams.thresholdSize)
			rehydrationResult, err := rehydrator.rehydrate(ctx)
			require.NoError(t, err)

			assert.Equal(t, utils.RehydrationLocation(taskEnv.RehydrationBucket, dataset.ID, dataset.VersionID), rehydrationResult.Location)
			assert.Len(t, rehydrationResult.FileResults, datasetFileCount)
			for _, fileResult := range rehydrationResult.FileResults {
				assert.NoError(t, fileResult.Error)
				if assert.NotNil(t, fileResult.Rehydration) {
					sourcePath := fileResult.Rehydration.Src.GetPath()
					require.Contains(t, testDatasetFiles.ByPath, sourcePath)
					expectedRehydratedKey := utils.DestinationKey(dataset.ID, dataset.VersionID, sourcePath)
					assert.Equal(t, expectedRehydratedKey, fileResult.Rehydration.Dest.GetKey())
				}
			}

			for _, datasetFile := range testDatasetFiles.Files {
				expectedRehydratedKey := utils.DestinationKey(dataset.ID, dataset.VersionID, datasetFile.Path)
				s3Fixture.AssertObjectExists(taskEnv.RehydrationBucket, expectedRehydratedKey, datasetFile.Size)
			}
		})
	}
}

func TestRehydrate_S3Errors(t *testing.T) {
	test.SetLogLevel(t, slog.LevelError)
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithMinIO().Config(ctx, false)
	publishBucket := "discover-bucket"
	taskEnv := newTestConfigEnv()
	dataset := taskEnv.Dataset

	testDatasetFileCount := 101
	testDatasetFiles := discovertest.NewTestDatasetFiles(*dataset, testDatasetFileCount).WithFakeS3VersionsIDs()
	var copyFailPaths []string
	for i, file := range testDatasetFiles.Files {
		// S3 Failure every 10th file
		if i%10 == 0 {
			copyFailPaths = append(copyFailPaths, file.Path)
		}
	}

	// Set up S3 for the tests
	s3Client := s3.NewFromConfig(awsConfig)
	s3Fixture, putObjectOutputs := test.NewS3Fixture(t, s3Client,
		&s3.CreateBucketInput{Bucket: aws.String(publishBucket)},
		&s3.CreateBucketInput{Bucket: aws.String(taskEnv.RehydrationBucket)},
	).WithVersioning(publishBucket).WithObjects(testDatasetFiles.PutObjectInputs(publishBucket)...)
	defer s3Fixture.Teardown()

	// Set S3 versionIds
	for location, putOutput := range putObjectOutputs {
		testDatasetFiles.SetS3VersionID(t, location, aws.ToString(putOutput.VersionId))
	}

	// Create a mock Discover API server
	mockDiscover := discovertest.NewServerFixture(t, nil,
		discovertest.GetDatasetMetadataByVersionHandlerBuilder(*dataset, testDatasetFiles.DatasetFiles()),
		discovertest.GetDatasetFileByVersionHandlerBuilder(*dataset, publishBucket, testDatasetFiles.ByPath),
	)

	defer mockDiscover.Teardown()

	taskEnv.PennsieveHost = mockDiscover.Server.URL

	taskConfig := config.NewConfig(awsConfig, taskEnv)
	mockProcessor := NewMockFailingObjectProcessor(s3Client, copyFailPaths...)
	taskConfig.SetObjectProcessor(mockProcessor)

	rehydrator := NewDatasetRehydrator(taskConfig, ThresholdSize)

	result, err := rehydrator.rehydrate(ctx)
	require.NoError(t, err)
	assert.Equal(t, utils.RehydrationLocation(taskEnv.RehydrationBucket, dataset.ID, dataset.VersionID), result.Location)
	assert.Len(t, result.FileResults, testDatasetFileCount)
	for _, fileResult := range result.FileResults {
		require.NotNil(t, fileResult.Rehydration)
		sourcePath := fileResult.Rehydration.Src.GetPath()
		if _, failed := mockProcessor.FailOnPaths[sourcePath]; failed {
			assert.Error(t, fileResult.Error)
		} else {
			assert.NoError(t, fileResult.Error)
		}
	}

}

func TestRehydrate_DiscoverErrors(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).Config(ctx, false)
	publishBucket := "discover-bucket"
	taskEnv := newTestConfigEnv()
	dataset := taskEnv.Dataset

	testDatasetFiles := discovertest.NewTestDatasetFiles(*dataset, 50).WithFakeS3VersionsIDs()
	pathsToFail := map[string]bool{testDatasetFiles.Files[24].Path: true}

	for testName, testParams := range map[string]struct {
		discoverBuilders []*test.HandlerFuncBuilder
	}{
		"get dataset metadata error": {discoverBuilders: []*test.HandlerFuncBuilder{
			discovertest.ErrorGetDatasetMetadataByVersionHandlerBuilder(*dataset, "internal service error", http.StatusInternalServerError)},
		},
		"get dataset file error": {discoverBuilders: []*test.HandlerFuncBuilder{
			discovertest.GetDatasetMetadataByVersionHandlerBuilder(*dataset, testDatasetFiles.DatasetFiles()),
			discovertest.ErrorGetDatasetFileByVersionHandlerBuilder(*dataset, publishBucket, testDatasetFiles.DatasetFilesByPath(), pathsToFail),
		}},
	} {

		t.Run(testName, func(t *testing.T) {
			// Create a mock Discover API server
			mockDiscover := discovertest.NewServerFixture(t, nil, testParams.discoverBuilders...)
			defer mockDiscover.Teardown()

			taskEnv.PennsieveHost = mockDiscover.Server.URL
			taskConfig := config.NewConfig(awsConfig, taskEnv)
			// No calls should be made to S3
			taskConfig.SetObjectProcessor(NewNoCallsObjectProcessor(t))

			rehydrator := NewDatasetRehydrator(taskConfig, ThresholdSize)

			_, err := rehydrator.rehydrate(ctx)
			require.Error(t, err)
		})

	}
}
