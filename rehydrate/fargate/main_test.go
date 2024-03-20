package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/pennsieve/rehydration-service/fargate/config"
	"github.com/pennsieve/rehydration-service/fargate/objects"
	"github.com/pennsieve/rehydration-service/fargate/utils"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/models"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/pennsieve/rehydration-service/shared/test/discovertest"
	"github.com/pennsieve/rehydration-service/shared/tracking"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log/slog"
	"net/http"
	"testing"
	"time"
)

func TestRehydrationTaskHandler(t *testing.T) {
	test.SetLogLevel(t, slog.LevelError)
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().WithMinIO().Config(ctx, false)
	publishBucket := "discover-bucket"
	taskEnv := newTestConfigEnv()
	dataset := taskEnv.Dataset

	testDatasetFiles := discovertest.NewTestDatasetFiles(*dataset, 50)

	for testName, testParams := range map[string]struct {
		thresholdSize int64
	}{
		"simple copies":    {thresholdSize: ThresholdSize},
		"multipart copies": {thresholdSize: 10},
	} {
		t.Run(testName, func(t *testing.T) {
			// Set up S3 for the tests
			s3Fixture, putObjectOutputs := test.NewS3Fixture(t, s3.NewFromConfig(awsConfig), &s3.CreateBucketInput{
				Bucket: aws.String(publishBucket),
			}).WithVersioning(publishBucket).WithObjects(testDatasetFiles.PutObjectInputs(publishBucket)...)
			defer s3Fixture.Teardown()

			// Set S3 versionIds
			for location, putOutput := range putObjectOutputs {
				testDatasetFiles.SetS3VersionID(t, location, aws.ToString(putOutput.VersionId))
			}

			// Setup DynamoDB for tests
			var putItemInputs []*dynamodb.PutItemInput
			// Idempotency record
			initialIdempotencyRecord := newInProgressRecord(*dataset)
			putItemInputs = append(putItemInputs, test.ItemersToPutItemInputs(t, taskEnv.IdempotencyTable, initialIdempotencyRecord)...)
			expectedTaskARN := initialIdempotencyRecord.FargateTaskARN

			// Some tracking entries
			user2 := models.User{
				Name:  "Guy Sur",
				Email: "sur@example.com",
			}
			alreadyHandledRequestDate := time.Now().Add(-time.Hour * 24)
			alreadyHandledEmailSentData := alreadyHandledRequestDate.Add(time.Hour * 12)
			alreadyHandledEntry := &tracking.Entry{
				DatasetVersionIndex: tracking.DatasetVersionIndex{
					ID:                uuid.NewString(),
					DatasetVersion:    dataset.DatasetVersion(),
					UserName:          "Hal Blaine",
					UserEmail:         "hb@example.com",
					RehydrationStatus: tracking.Completed,
					EmailSentDate:     &alreadyHandledEmailSentData,
				},
				LambdaLogStream: uuid.NewString(),
				AWSRequestID:    uuid.NewString(),
				RequestDate:     alreadyHandledRequestDate,
				FargateTaskARN:  uuid.NewString(),
			}
			unhandledEntries := []test.Itemer{
				tracking.NewEntry(uuid.NewString(), *dataset, *taskEnv.User, uuid.NewString(), uuid.NewString(), expectedTaskARN),
				tracking.NewEntry(uuid.NewString(), *dataset, user2, uuid.NewString(), uuid.NewString(), expectedTaskARN),
				tracking.NewEntry(uuid.NewString(), *dataset, *taskEnv.User, uuid.NewString(), uuid.NewString(), expectedTaskARN),
			}
			unhandledEntriesByID := map[string]*tracking.Entry{}
			for _, e := range unhandledEntries {
				asEntry := e.(*tracking.Entry)
				unhandledEntriesByID[asEntry.ID] = asEntry
			}
			allEntries := append(unhandledEntries, alreadyHandledEntry)
			putItemInputs = append(putItemInputs, test.ItemersToPutItemInputs(t, taskEnv.TrackingTable, allEntries...)...)

			dyDB := test.NewDynamoDBFixture(
				t,
				awsConfig,
				test.IdempotencyCreateTableInput(taskEnv.IdempotencyTable, idempotency.KeyAttrName),
				test.TrackingCreateTableInput(taskEnv.TrackingTable, tracking.IDAttrName)).
				WithItems(putItemInputs...)
			defer dyDB.Teardown()

			// Create a mock Discover API server
			mockDiscover := discovertest.NewServerFixture(t, nil,
				discovertest.GetDatasetByVersionHandlerBuilder(*dataset, publishBucket),
				discovertest.GetDatasetMetadataByVersionHandlerBuilder(*dataset, testDatasetFiles.DatasetFiles()),
				discovertest.GetDatasetFileByVersionHandlerBuilder(*dataset, publishBucket, testDatasetFiles.ByPath),
			)
			defer mockDiscover.Teardown()

			taskEnv.PennsieveHost = mockDiscover.Server.URL
			taskConfig := config.NewConfig(awsConfig, taskEnv)
			trackHandler := NewTaskHandler(taskConfig, testParams.thresholdSize)
			beforeTask := time.Now()
			require.NoError(t, RehydrationTaskHandler(ctx, trackHandler))
			afterTask := time.Now()
			for _, datasetFile := range testDatasetFiles.Files {
				expectedRehydratedKey := utils.CreateDestinationKey(dataset.ID, dataset.VersionID, datasetFile.Path)
				s3Fixture.AssertObjectExists(publishBucket, expectedRehydratedKey, datasetFile.Size)
			}
			idempotencyItems := dyDB.Scan(ctx, taskEnv.IdempotencyTable)
			require.Len(t, idempotencyItems, 1)
			updatedIdempotencyRecord, err := idempotency.FromItem(idempotencyItems[0])
			require.NoError(t, err)
			assert.Equal(t, initialIdempotencyRecord.ID, updatedIdempotencyRecord.ID)
			assert.Equal(t, expectedTaskARN, updatedIdempotencyRecord.FargateTaskARN)
			assert.Equal(t, idempotency.Completed, updatedIdempotencyRecord.Status)
			assert.Equal(t, utils.RehydrationLocation(publishBucket, dataset.ID, dataset.VersionID), updatedIdempotencyRecord.RehydrationLocation)

			trackingItems := dyDB.Scan(ctx, taskEnv.TrackingTable)
			require.Len(t, trackingItems, 4)
			for _, trackingItem := range trackingItems {
				entry, err := tracking.FromItem(trackingItem)
				require.NoError(t, err)
				assert.Equal(t, taskEnv.Dataset.DatasetVersion(), entry.DatasetVersion)
				assert.Equal(t, tracking.Completed, entry.RehydrationStatus)
				assert.NotNil(t, entry.EmailSentDate)
				var expected *tracking.Entry
				updatedEntry, previouslyUnhandled := unhandledEntriesByID[entry.ID]
				if previouslyUnhandled {
					expected = updatedEntry
					assert.False(t, beforeTask.After(*entry.EmailSentDate))
					assert.False(t, afterTask.Before(*entry.EmailSentDate))
				} else {
					expected = alreadyHandledEntry
					assert.Equal(t, expected.EmailSentDate.Format(time.RFC3339Nano), entry.EmailSentDate.Format(time.RFC3339Nano))
				}
				assert.Equal(t, expected.UserName, entry.UserName)
				assert.Equal(t, expected.UserEmail, entry.UserEmail)
				assert.Equal(t, expected.FargateTaskARN, entry.FargateTaskARN)
				assert.Equal(t, expected.LambdaLogStream, entry.LambdaLogStream)
				assert.Equal(t, expected.AWSRequestID, entry.AWSRequestID)
				assert.Equal(t, expected.RequestDate.Format(time.RFC3339Nano), entry.RequestDate.Format(time.RFC3339Nano))
			}
		})
	}
}

func TestRehydrationTaskHandler_S3Errors(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	publishBucket := "discover-bucket"
	taskEnv := newTestConfigEnv()
	idempotencyTable := taskEnv.IdempotencyTable
	dataset := taskEnv.Dataset

	testDatasetFiles := discovertest.NewTestDatasetFiles(*dataset, 50).WithFakeS3VersionsIDs()
	copyFailPath := testDatasetFiles.Files[17].Path

	// Setup DynamoDB for tests
	initialIdempotencyRecord := newInProgressRecord(*dataset)
	initialTrackingEntry := tracking.NewEntry(
		uuid.NewString(),
		*dataset,
		*taskEnv.User,
		uuid.NewString(),
		uuid.NewString(),
		initialIdempotencyRecord.FargateTaskARN)
	dyDB := test.NewDynamoDBFixture(
		t,
		awsConfig,
		test.IdempotencyCreateTableInput(idempotencyTable, idempotency.KeyAttrName),
		test.TrackingCreateTableInput(taskEnv.TrackingTable, tracking.IDAttrName)).
		WithItems(
			test.ItemerMapToPutItemInputs(t, map[string][]test.Itemer{
				idempotencyTable:      {initialIdempotencyRecord},
				taskEnv.TrackingTable: {initialTrackingEntry},
			})...)
	defer dyDB.Teardown()

	// Create a mock Discover API server
	mockDiscover := discovertest.NewServerFixture(t, nil,
		discovertest.GetDatasetByVersionHandlerBuilder(*dataset, publishBucket),
		discovertest.GetDatasetMetadataByVersionHandlerBuilder(*dataset, testDatasetFiles.DatasetFiles()),
		discovertest.GetDatasetFileByVersionHandlerBuilder(*dataset, publishBucket, testDatasetFiles.ByPath),
	)

	defer mockDiscover.Teardown()

	taskEnv.PennsieveHost = mockDiscover.Server.URL

	taskConfig := config.NewConfig(awsConfig, taskEnv)
	taskConfig.SetObjectProcessor(NewMockFailingObjectProcessor(copyFailPath))

	taskHandler := NewTaskHandler(taskConfig, ThresholdSize)
	beforeEmailSent := time.Now()
	err := RehydrationTaskHandler(ctx, taskHandler)
	require.Error(t, err)
	afterEmailSent := time.Now()
	require.Contains(t, err.Error(), copyFailPath)

	// Idempotency record should have been deleted so that another attempt can be made
	idempotencyItems := dyDB.Scan(ctx, idempotencyTable)
	require.Len(t, idempotencyItems, 0)

	// tracking entry should be marked as failed
	trackingItems := dyDB.Scan(ctx, taskEnv.TrackingTable)
	require.Len(t, trackingItems, 1)
	entry, err := tracking.FromItem(trackingItems[0])
	require.NoError(t, err)
	assert.Equal(t, tracking.Failed, entry.RehydrationStatus)
	assert.NotNil(t, entry.EmailSentDate)
	assert.False(t, beforeEmailSent.After(*entry.EmailSentDate))
	assert.False(t, afterEmailSent.Before(*entry.EmailSentDate))

}

func TestRehydrationTaskHandler_DiscoverErrors(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	publishBucket := "discover-bucket"
	taskEnv := newTestConfigEnv()
	idempotencyTable := taskEnv.IdempotencyTable
	dataset := taskEnv.Dataset
	initialIdempotencyRecord := newInProgressRecord(*dataset)
	initialTrackingEntry := tracking.NewEntry(uuid.NewString(), *dataset, *taskEnv.User, uuid.NewString(), uuid.NewString(), initialIdempotencyRecord.FargateTaskARN)

	testDatasetFiles := discovertest.NewTestDatasetFiles(*dataset, 50).WithFakeS3VersionsIDs()
	pathsToFail := map[string]bool{testDatasetFiles.Files[24].Path: true}

	for testName, testParams := range map[string]struct {
		discoverBuilders []*test.HandlerFuncBuilder
	}{
		"get dataset error": {discoverBuilders: []*test.HandlerFuncBuilder{
			discovertest.ErrorGetDatasetByVersionHandlerBuilder(*dataset, "dataset not found", http.StatusNotFound)},
		},
		"get dataset metadata error": {discoverBuilders: []*test.HandlerFuncBuilder{
			discovertest.GetDatasetByVersionHandlerBuilder(*dataset, publishBucket),
			discovertest.ErrorGetDatasetMetadataByVersionHandlerBuilder(*dataset, "internal service error", http.StatusInternalServerError)},
		},
		"get dataset file error": {discoverBuilders: []*test.HandlerFuncBuilder{
			discovertest.GetDatasetByVersionHandlerBuilder(*dataset, publishBucket),
			discovertest.GetDatasetMetadataByVersionHandlerBuilder(*dataset, testDatasetFiles.DatasetFiles()),
			discovertest.ErrorGetDatasetFileByVersionHandlerBuilder(*dataset, publishBucket, testDatasetFiles.DatasetFilesByPath(), pathsToFail),
		}},
	} {

		t.Run(testName, func(t *testing.T) {
			// Setup DynamoDB for tests
			dyDB := test.NewDynamoDBFixture(
				t,
				awsConfig,
				test.IdempotencyCreateTableInput(idempotencyTable, idempotency.KeyAttrName),
				test.TrackingCreateTableInput(taskEnv.TrackingTable, tracking.IDAttrName)).
				WithItems(
					test.ItemerMapToPutItemInputs(t, map[string][]test.Itemer{
						idempotencyTable:      {initialIdempotencyRecord},
						taskEnv.TrackingTable: {initialTrackingEntry},
					})...)
			defer dyDB.Teardown()

			// Create a mock Discover API server
			mockDiscover := discovertest.NewServerFixture(t, nil, testParams.discoverBuilders...)
			defer mockDiscover.Teardown()

			taskEnv.PennsieveHost = mockDiscover.Server.URL
			taskConfig := config.NewConfig(awsConfig, taskEnv)
			// No calls should be made to S3
			taskConfig.SetObjectProcessor(NewNoCallsObjectProcessor(t))

			taskHandler := NewTaskHandler(taskConfig, ThresholdSize)
			beforeEmailSent := time.Now()
			err := RehydrationTaskHandler(ctx, taskHandler)
			require.Error(t, err)
			afterEmailSent := time.Now()

			// idempotency record should have been deleted so that another attempt can be made
			idempotencyItems := dyDB.Scan(ctx, idempotencyTable)
			require.Len(t, idempotencyItems, 0)

			// tracking entry should be marked as failed
			trackingItems := dyDB.Scan(ctx, taskEnv.TrackingTable)
			require.Len(t, trackingItems, 1)
			entry, err := tracking.FromItem(trackingItems[0])
			require.NoError(t, err)
			assert.Equal(t, tracking.Failed, entry.RehydrationStatus)
			assert.NotNil(t, entry.EmailSentDate)
			assert.False(t, beforeEmailSent.After(*entry.EmailSentDate))
			assert.False(t, afterEmailSent.Before(*entry.EmailSentDate))
		})

	}
}

func newTestConfigEnv() *config.Env {
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
		IdempotencyTable: "test-idempotency-table",
		TrackingTable:    "test-tracking-table",
	}
}

func newInProgressRecord(dataset models.Dataset) *idempotency.Record {
	return &idempotency.Record{
		ID:             idempotency.RecordID(dataset.ID, dataset.VersionID),
		Status:         idempotency.InProgress,
		FargateTaskARN: "arn:aws:dynamoDB:test:test:test",
	}
}

type MockFailingObjectProcessor struct {
	FailOnPaths map[string]bool
}

func NewMockFailingObjectProcessor(failOnPaths ...string) *MockFailingObjectProcessor {
	mock := MockFailingObjectProcessor{FailOnPaths: map[string]bool{}}
	for _, p := range failOnPaths {
		mock.FailOnPaths[p] = true
	}
	return &mock
}

func (m MockFailingObjectProcessor) Copy(_ context.Context, source objects.Source, _ objects.Destination) error {
	if _, fail := m.FailOnPaths[source.GetPath()]; fail {
		return fmt.Errorf("error copying %s", source.GetVersionedUri())
	}
	return nil
}

type MockNoCallsObjectProcessor struct {
	testingT require.TestingT
}

func NewNoCallsObjectProcessor(t require.TestingT) *MockNoCallsObjectProcessor {
	return &MockNoCallsObjectProcessor{testingT: t}
}

func (m *MockNoCallsObjectProcessor) Copy(_ context.Context, source objects.Source, destination objects.Destination) error {
	assert.Failf(m.testingT, "unexpected call to S3 Copy", "source: %s, destination: %s", source.GetVersionedUri(), destination.GetKey())
	return nil
}
