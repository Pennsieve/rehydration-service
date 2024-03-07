package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve/models/discover"
	"github.com/pennsieve/rehydration-service/fargate/config"
	"github.com/pennsieve/rehydration-service/fargate/objects"
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
	taskEnv := newTestConfigEnv(idempotencyTable)
	dataset := taskEnv.Dataset

	testDatasetFiles := NewTestDatasetFiles(*dataset, 50)

	for testName, testParams := range map[string]struct {
		thresholdSize int64
	}{
		"simple copies":    {thresholdSize: ThresholdSize},
		"multipart copies": {thresholdSize: 10},
	} {

		// Set up S3 for the tests
		s3Fixture, putObjectOutputs := test.NewS3Fixture(t, s3.NewFromConfig(awsConfig), &s3.CreateBucketInput{
			Bucket: aws.String(publishBucket),
		}).WithVersioning(publishBucket).WithObjects(testDatasetFiles.PutObjectInputs(publishBucket)...)

		// Set S3 versionIds
		for location, putOutput := range putObjectOutputs {
			testDatasetFiles.SetS3VersionID(t, location, aws.ToString(putOutput.VersionId))
		}

		// Setup DynamoDB for tests
		initialIdempotencyRecord := newInProgressRecord(*dataset)
		dyDB := test.NewDynamoDBFixture(t, awsConfig, test.IdempotencyCreateTableInput(idempotencyTable, idempotency.KeyAttrName)).WithItems(
			test.RecordsToPutItemInputs(t, idempotencyTable, initialIdempotencyRecord)...)

		// Create a mock Discover API server
		mockDiscover := test.NewDiscoverServerFixture(t, nil)
		addDatasetByVersionHandler(mockDiscover, dataset, publishBucket)
		addDatasetMetadataByVersionHandler(mockDiscover, dataset, testDatasetFiles.DatasetFiles())
		addDatasetFileByVersionHandler(mockDiscover, dataset, publishBucket, testDatasetFiles.ByPath)

		t.Run(testName, func(t *testing.T) {
			taskEnv.PennsieveHost = mockDiscover.Server.URL
			taskConfig := config.NewConfig(awsConfig, taskEnv)
			rehydrator := NewDatasetRehydrator(taskConfig, testParams.thresholdSize)
			idempotencyStore, err := taskConfig.IdempotencyStore()
			require.NoError(t, err)
			require.NoError(t, RehydrationTaskHandler(ctx, rehydrator, idempotencyStore))
			for _, datasetFile := range testDatasetFiles.Files {
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

func TestRehydrationTaskHandler_S3Errors(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	idempotencyTable := "main-test-idempotency-table"
	publishBucket := "discover-bucket"
	taskEnv := newTestConfigEnv(idempotencyTable)
	dataset := taskEnv.Dataset

	testDatasetFiles := NewTestDatasetFiles(*dataset, 50).WithFakeS3VersionsIDs()
	copyFailPath := testDatasetFiles.Files[17].Path

	// Setup DynamoDB for tests
	initialIdempotencyRecord := newInProgressRecord(*dataset)
	dyDB := test.NewDynamoDBFixture(t, awsConfig, test.IdempotencyCreateTableInput(idempotencyTable, idempotency.KeyAttrName)).WithItems(
		test.RecordsToPutItemInputs(t, idempotencyTable, initialIdempotencyRecord)...)
	defer dyDB.Teardown()

	// Create a mock Discover API server
	mockDiscover := test.NewDiscoverServerFixture(t, nil)
	defer mockDiscover.Teardown()

	taskEnv.PennsieveHost = mockDiscover.Server.URL
	addDatasetByVersionHandler(mockDiscover, dataset, publishBucket)
	addDatasetMetadataByVersionHandler(mockDiscover, dataset, testDatasetFiles.DatasetFiles())
	addDatasetFileByVersionHandler(mockDiscover, dataset, publishBucket, testDatasetFiles.ByPath)

	taskConfig := config.NewConfig(awsConfig, taskEnv)
	taskConfig.SetObjectProcessor(NewMockObjectProcessor([]string{copyFailPath}))

	rehydrator := NewDatasetRehydrator(taskConfig, ThresholdSize)
	idempotencyStore, err := taskConfig.IdempotencyStore()
	require.NoError(t, err)

	err = RehydrationTaskHandler(ctx, rehydrator, idempotencyStore)
	require.Error(t, err)
	require.Contains(t, err.Error(), copyFailPath)

	// Idempotency record should have been deleted so that another attempt can be made
	idempotencyItems := dyDB.Scan(ctx, idempotencyTable)
	require.Len(t, idempotencyItems, 0)
}

// this is written to Go 1.21 where http.ServeMux patterns do not yet have methods or wildcards. So this function can be made more
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

func addDatasetFileByVersionHandler(mockDiscover *test.DiscoverServerFixture, dataset *models.Dataset, expectedBucket string, expectedDatasetFileByPath map[string]*TestDatasetFile) {
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

func newTestConfigEnv(idempotencyTable string) *config.Env {
	dataset := &models.Dataset{
		ID:        1234,
		VersionID: 3,
	}
	user := &models.User{
		Name:  "First Last",
		Email: "last@example.com",
	}

	return &config.Env{
		Dataset:          dataset,
		User:             user,
		TaskEnv:          "TEST",
		IdempotencyTable: idempotencyTable,
	}
}

func newInProgressRecord(dataset models.Dataset) *idempotency.Record {
	return &idempotency.Record{
		ID:             idempotency.RecordID(dataset.ID, dataset.VersionID),
		Status:         idempotency.InProgress,
		FargateTaskARN: "arn:aws:dynamoDB:test:test:test",
	}
}

type TestDatasetFile struct {
	discover.DatasetFile
	content string
	s3key   string
}

type TestDatasetFiles struct {
	Files  []TestDatasetFile
	ByPath map[string]*TestDatasetFile
	ByKey  map[string]*TestDatasetFile
}

func NewTestDatasetFiles(dataset models.Dataset, count int) *TestDatasetFiles {
	datasetFiles := make([]TestDatasetFile, count)
	datasetFilesByPath := map[string]*TestDatasetFile{}
	datasetFilesByKey := map[string]*TestDatasetFile{}
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("file%d.txt", i)
		path := fmt.Sprintf("files/dir%d/%s", i, name)
		content := fmt.Sprintf("content of %s\n", name)
		datasetFile := TestDatasetFile{
			DatasetFile: discover.DatasetFile{
				Name:     name,
				Path:     path,
				FileType: "TEXT",
				Size:     int64(len([]byte(content))),
			},
			content: content,
			s3key:   fmt.Sprintf("%d/%s", dataset.ID, path),
		}
		datasetFiles[i] = datasetFile
		datasetFilesByPath[datasetFile.Path] = &datasetFiles[i]
		datasetFilesByKey[datasetFile.s3key] = &datasetFiles[i]
	}

	return &TestDatasetFiles{
		Files:  datasetFiles,
		ByPath: datasetFilesByPath,
		ByKey:  datasetFilesByKey,
	}
}

func (f *TestDatasetFiles) WithFakeS3VersionsIDs() *TestDatasetFiles {
	for i := range f.Files {
		file := &f.Files[i]
		file.S3VersionID = uuid.NewString()
	}
	return f
}

func (f *TestDatasetFiles) SetS3VersionID(t *testing.T, s3Location test.S3Location, s3VersionId string) {
	datasetFile, ok := f.ByKey[s3Location.Key]
	require.Truef(t, ok, "missing DatasetFile: bucket: %s, key: %s", s3Location.Bucket, s3Location.Key)
	datasetFile.S3VersionID = s3VersionId
}

func (f *TestDatasetFiles) PutObjectInputs(bucket string) []*s3.PutObjectInput {
	var putObjectInputs []*s3.PutObjectInput
	for _, file := range f.Files {
		putObjectInputs = append(putObjectInputs, &s3.PutObjectInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(file.s3key),
			Body:          strings.NewReader(file.content),
			ContentLength: aws.Int64(file.Size),
		})
	}
	return putObjectInputs
}

func (f *TestDatasetFiles) DatasetFiles() []discover.DatasetFile {
	var datasetFiles []discover.DatasetFile
	for _, file := range f.Files {
		datasetFiles = append(datasetFiles, file.DatasetFile)
	}
	return datasetFiles
}

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

type MockObjectProcessor struct {
	FailOnPaths map[string]bool
}

func NewMockObjectProcessor(failOnPaths []string) *MockObjectProcessor {
	mock := MockObjectProcessor{FailOnPaths: map[string]bool{}}
	for _, p := range failOnPaths {
		mock.FailOnPaths[p] = true
	}
	return &mock
}

func (m MockObjectProcessor) Copy(_ context.Context, source objects.Source, _ objects.Destination) error {
	if _, fail := m.FailOnPaths[source.GetPath()]; fail {
		return fmt.Errorf("error copying %s", source.GetVersionedUri())
	}
	return nil
}
