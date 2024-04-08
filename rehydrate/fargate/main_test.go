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
	"github.com/pennsieve/rehydration-service/shared/expiration"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/logging"
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
	expectedRehydrationLocation := utils.RehydrationLocation(taskEnv.RehydrationBucket, dataset.ID, dataset.VersionID)

	testDatasetFiles := discovertest.NewTestDatasetFiles(*dataset, 50)

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

			oldEntriesByID := map[string]*tracking.Entry{}

			// An old entry for a previous, completed rehydration of the same dataset
			// Should be left untouched and no email sent.
			oldCompletedRequestDate := time.Now().Add(-time.Hour * 24)
			oldCompletedEmailSentDate := oldCompletedRequestDate.Add(time.Hour * 12)
			oldCompletedEntry := &tracking.Entry{
				DatasetVersionIndex: tracking.DatasetVersionIndex{
					ID:                uuid.NewString(),
					DatasetVersion:    dataset.DatasetVersion(),
					UserName:          "Hal Blaine",
					UserEmail:         "hb@example.com",
					RehydrationStatus: tracking.Completed,
					EmailSentDate:     &oldCompletedEmailSentDate,
				},
				LambdaLogStream: uuid.NewString(),
				AWSRequestID:    uuid.NewString(),
				RequestDate:     oldCompletedRequestDate,
				FargateTaskARN:  uuid.NewString(),
			}
			oldEntriesByID[oldCompletedEntry.ID] = oldCompletedEntry
			// An old entry for a previous, failed rehydration of the same dataset
			// Should be left untouched and no email sent.
			// This situation, where the status is Failed, but there is no emailSentDate
			// should be an exceptional case: The rehydration ended in failure, but there
			// was also an error when sending the failure notification email.
			oldFailedRequestDate := time.Now().Add(-time.Hour * 24)
			oldFailedEntry := &tracking.Entry{
				DatasetVersionIndex: tracking.DatasetVersionIndex{
					ID:                uuid.NewString(),
					DatasetVersion:    dataset.DatasetVersion(),
					UserName:          "Harry Gorman",
					UserEmail:         "gorman@example.com",
					RehydrationStatus: tracking.Failed,
				},
				LambdaLogStream: uuid.NewString(),
				AWSRequestID:    uuid.NewString(),
				RequestDate:     oldFailedRequestDate,
				FargateTaskARN:  uuid.NewString(),
			}
			oldEntriesByID[oldFailedEntry.ID] = oldFailedEntry
			// New entries for this rehydration. Should be updated and emails sent.
			unhandledEntries := []test.Itemer{
				tracking.NewEntry(uuid.NewString(), *dataset, *taskEnv.User, uuid.NewString(), uuid.NewString(), expectedTaskARN),
				tracking.NewEntry(uuid.NewString(), *dataset, user2, uuid.NewString(), uuid.NewString(), expectedTaskARN),
				tracking.NewEntry(uuid.NewString(), *dataset, *taskEnv.User, uuid.NewString(), uuid.NewString(), expectedTaskARN),
			}
			unhandledEntriesByID := map[string]*tracking.Entry{}
			unhandledEntriesByEmail := map[string][]*tracking.Entry{}
			for _, e := range unhandledEntries {
				asEntry := e.(*tracking.Entry)
				unhandledEntriesByID[asEntry.ID] = asEntry
				unhandledEntriesByEmail[asEntry.UserEmail] = append(unhandledEntriesByEmail[asEntry.UserEmail], asEntry)
			}
			allEntries := append(unhandledEntries, oldCompletedEntry, oldFailedEntry)
			putItemInputs = append(putItemInputs, test.ItemersToPutItemInputs(t, taskEnv.TrackingTable, allEntries...)...)

			dyDB := test.NewDynamoDBFixture(
				t,
				awsConfig,
				test.IdempotencyCreateTableInput(taskEnv.IdempotencyTable),
				test.TrackingCreateTableInput(taskEnv.TrackingTable)).
				WithItems(putItemInputs...)
			defer dyDB.Teardown()

			// Create a mock Discover API server
			mockDiscover := discovertest.NewServerFixture(t, nil,
				discovertest.GetDatasetMetadataByVersionHandlerBuilder(*dataset, testDatasetFiles.DatasetFiles()),
				discovertest.GetDatasetFileByVersionHandlerBuilder(*dataset, publishBucket, testDatasetFiles.ByPath),
			)
			defer mockDiscover.Teardown()

			taskEnv.PennsieveHost = mockDiscover.Server.URL
			taskConfig := config.NewConfig(awsConfig, taskEnv)
			mockEmailer := new(MockEmailer)
			taskConfig.SetEmailer(mockEmailer)
			trackHandler, err := NewTaskHandler(taskConfig, testParams.thresholdSize)
			require.NoError(t, err)
			beforeTask := time.Now()
			require.NoError(t, RehydrationTaskHandler(ctx, trackHandler))
			afterTask := time.Now()
			for _, datasetFile := range testDatasetFiles.Files {
				expectedRehydratedKey := utils.CreateDestinationKey(dataset.ID, dataset.VersionID, datasetFile.Path)
				s3Fixture.AssertObjectExists(taskEnv.RehydrationBucket, expectedRehydratedKey, datasetFile.Size)
			}
			idempotencyItems := dyDB.Scan(ctx, taskEnv.IdempotencyTable)
			require.Len(t, idempotencyItems, 1)
			updatedIdempotencyRecord, err := idempotency.FromItem(idempotencyItems[0])
			require.NoError(t, err)
			assert.Equal(t, initialIdempotencyRecord.ID, updatedIdempotencyRecord.ID)
			assert.Equal(t, expectedTaskARN, updatedIdempotencyRecord.FargateTaskARN)
			assert.Equal(t, idempotency.Completed, updatedIdempotencyRecord.Status)
			assert.Equal(t, expectedRehydrationLocation, updatedIdempotencyRecord.RehydrationLocation)
			assert.NotNil(t, updatedIdempotencyRecord.ExpirationDate)
			assert.LessOrEqual(t, expiration.DateFrom(beforeTask, taskEnv.RehydrationTTLDays), *updatedIdempotencyRecord.ExpirationDate)
			assert.GreaterOrEqual(t, expiration.DateFrom(afterTask, taskEnv.RehydrationTTLDays), *updatedIdempotencyRecord.ExpirationDate)

			trackingItems := dyDB.Scan(ctx, taskEnv.TrackingTable)
			require.Len(t, trackingItems, len(allEntries))
			for _, trackingItem := range trackingItems {
				entry, err := tracking.FromItem(trackingItem)
				require.NoError(t, err)
				//DatasetVersion should be the same for all the entries
				assert.Equal(t, taskEnv.Dataset.DatasetVersion(), entry.DatasetVersion)
				var expected *tracking.Entry
				updatedEntry, previouslyUnhandled := unhandledEntriesByID[entry.ID]
				if previouslyUnhandled {
					expected = updatedEntry
					assert.NotNil(t, entry.EmailSentDate)
					assert.False(t, beforeTask.After(*entry.EmailSentDate))
					assert.False(t, afterTask.Before(*entry.EmailSentDate))
					assert.Equal(t, tracking.Completed, entry.RehydrationStatus)
				} else {
					assert.Contains(t, oldEntriesByID, entry.ID)
					expected = oldEntriesByID[entry.ID]
					// old entries should not have there emailSentDate updated
					if expected.RehydrationStatus == tracking.Failed {
						assert.Nil(t, entry.EmailSentDate)
					} else {
						assert.True(t, expected.EmailSentDate.Equal(*entry.EmailSentDate))
					}
					assert.Equal(t, expected.RehydrationStatus, entry.RehydrationStatus)
				}
				assert.Equal(t, expected.UserName, entry.UserName)
				assert.Equal(t, expected.UserEmail, entry.UserEmail)
				assert.Equal(t, expected.FargateTaskARN, entry.FargateTaskARN)
				assert.Equal(t, expected.LambdaLogStream, entry.LambdaLogStream)
				assert.Equal(t, expected.AWSRequestID, entry.AWSRequestID)
				assert.True(t, expected.RequestDate.Equal(entry.RequestDate))
			}

			assert.Empty(t, mockEmailer.failed)
			// should only send one email per encountered address
			assert.Len(t, mockEmailer.complete, len(unhandledEntriesByEmail))
			for _, email := range mockEmailer.complete {
				assert.Equal(t, dataset.ID, email.dataset.ID)
				assert.Equal(t, dataset.VersionID, email.dataset.VersionID)
				assert.Equal(t, expectedRehydrationLocation, email.rehydrationLocation)
				assert.Contains(t, unhandledEntriesByEmail, email.user.Email)
				matchingEntries := unhandledEntriesByEmail[email.user.Email]
				var emailSentDate *time.Time
				for _, matchingEntry := range matchingEntries {
					assert.Equal(t, matchingEntry.UserName, email.user.Name)
					if emailSentDate == nil {
						emailSentDate = matchingEntry.EmailSentDate
					} else {
						// If there is more than one entry with this email address, they
						// should have all been updated with the same email sent date
						assert.True(t, emailSentDate.Equal(*matchingEntry.EmailSentDate))
					}
				}
			}

		})
	}
}

func TestRehydrationTaskHandler_S3CopyErrors(t *testing.T) {
	test.SetLogLevel(t, slog.LevelError)

	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().WithMinIO().Config(ctx, false)
	publishBucket := "discover-bucket"
	taskEnv := newTestConfigEnv()
	idempotencyTable := taskEnv.IdempotencyTable
	dataset := taskEnv.Dataset

	testDatasetFiles := discovertest.NewTestDatasetFiles(*dataset, 50).WithFakeS3VersionsIDs()
	copyFailPath := testDatasetFiles.Files[17].Path

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
		test.IdempotencyCreateTableInput(idempotencyTable),
		test.TrackingCreateTableInput(taskEnv.TrackingTable)).
		WithItems(
			test.ItemerMapToPutItemInputs(t, map[string][]test.Itemer{
				idempotencyTable:      {initialIdempotencyRecord},
				taskEnv.TrackingTable: {initialTrackingEntry},
			})...)
	defer dyDB.Teardown()

	// Create a mock Discover API server
	mockDiscover := discovertest.NewServerFixture(t, nil,
		discovertest.GetDatasetMetadataByVersionHandlerBuilder(*dataset, testDatasetFiles.DatasetFiles()),
		discovertest.GetDatasetFileByVersionHandlerBuilder(*dataset, publishBucket, testDatasetFiles.ByPath),
	)

	defer mockDiscover.Teardown()

	taskEnv.PennsieveHost = mockDiscover.Server.URL

	taskConfig := config.NewConfig(awsConfig, taskEnv)
	mockEmailer := new(MockEmailer)
	taskConfig.SetEmailer(mockEmailer)
	taskConfig.SetObjectProcessor(NewMockFailingObjectProcessor(s3Client, copyFailPath))

	taskHandler, err := NewTaskHandler(taskConfig, ThresholdSize)
	require.NoError(t, err)
	beforeEmailSent := time.Now()
	err = RehydrationTaskHandler(ctx, taskHandler)
	require.Error(t, err)
	joinErr, isJoinErr := err.(interface{ Unwrap() []error })
	if assert.True(t, isJoinErr) {
		errs := joinErr.Unwrap()
		assert.Len(t, errs, 1)
		require.Contains(t, errs[0].Error(), copyFailPath)
	}
	afterEmailSent := time.Now()

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

	// should have sent one failure email
	assert.Empty(t, mockEmailer.complete)
	assert.Len(t, mockEmailer.failed, 1)
	failedEmailCall := mockEmailer.failed[0]
	assert.Equal(t, entry.ID, failedEmailCall.requestID)
	assert.Equal(t, entry.UserName, failedEmailCall.user.Name)
	assert.Equal(t, entry.UserEmail, failedEmailCall.user.Email)
	assert.Equal(t, dataset.ID, failedEmailCall.dataset.ID)
	assert.Equal(t, dataset.VersionID, failedEmailCall.dataset.VersionID)

	// Rehydration bucket should have been cleaned up because of the failure
	s3Fixture.AssertBucketEmpty(taskEnv.RehydrationBucket)

}

func TestRehydrationTaskHandler_DiscoverErrors(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().WithMinIO().Config(ctx, false)
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
		"get dataset metadata error": {discoverBuilders: []*test.HandlerFuncBuilder{
			discovertest.ErrorGetDatasetMetadataByVersionHandlerBuilder(*dataset, "internal service error", http.StatusInternalServerError)},
		},
		"get dataset file error": {discoverBuilders: []*test.HandlerFuncBuilder{
			discovertest.GetDatasetMetadataByVersionHandlerBuilder(*dataset, testDatasetFiles.DatasetFiles()),
			discovertest.ErrorGetDatasetFileByVersionHandlerBuilder(*dataset, publishBucket, testDatasetFiles.DatasetFilesByPath(), pathsToFail),
		}},
	} {

		t.Run(testName, func(t *testing.T) {
			// Set up S3 for the tests just enough so that the S3 clean succeeds when the rehydration fails
			s3Client := s3.NewFromConfig(awsConfig)
			s3Fixture := test.NewS3Fixture(t, s3Client,
				&s3.CreateBucketInput{Bucket: aws.String(taskEnv.RehydrationBucket)},
			)
			defer s3Fixture.Teardown()

			// Setup DynamoDB for tests
			dyDB := test.NewDynamoDBFixture(
				t,
				awsConfig,
				test.IdempotencyCreateTableInput(idempotencyTable),
				test.TrackingCreateTableInput(taskEnv.TrackingTable)).
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
			// capture any emails sent
			mockEmailer := new(MockEmailer)
			taskConfig.SetEmailer(mockEmailer)

			taskHandler, err := NewTaskHandler(taskConfig, ThresholdSize)
			require.NoError(t, err)
			beforeEmailSent := time.Now()
			err = RehydrationTaskHandler(ctx, taskHandler)
			require.Error(t, err)
			joinErr, isJoinErr := err.(interface{ Unwrap() []error })
			if assert.True(t, isJoinErr) {
				errs := joinErr.Unwrap()
				assert.Len(t, errs, 1)
			}
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

			// One failure email should have been sent
			assert.Empty(t, mockEmailer.complete)
			assert.Len(t, mockEmailer.failed, 1)
			// should have sent one failure email
			assert.Empty(t, mockEmailer.complete)
			assert.Len(t, mockEmailer.failed, 1)
			failedEmailCall := mockEmailer.failed[0]
			assert.Equal(t, entry.ID, failedEmailCall.requestID)
			assert.Equal(t, entry.UserName, failedEmailCall.user.Name)
			assert.Equal(t, entry.UserEmail, failedEmailCall.user.Email)
			assert.Equal(t, dataset.ID, failedEmailCall.dataset.ID)
			assert.Equal(t, dataset.VersionID, failedEmailCall.dataset.VersionID)

			// Rehydration bucket should be empty because we never even got to copy
			s3Fixture.AssertBucketEmpty(taskEnv.RehydrationBucket)
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
		Dataset:            dataset,
		User:               user,
		TaskEnv:            "TEST",
		IdempotencyTable:   "test-idempotency-table",
		TrackingTable:      "test-tracking-table",
		PennsieveDomain:    "pennsieve.example.com",
		AWSRegion:          "us-test-1",
		RehydrationBucket:  "test-rehydration-bucket",
		RehydrationTTLDays: 14,
	}
}

func newInProgressRecord(dataset models.Dataset) *idempotency.Record {
	return idempotency.NewRecord(
		idempotency.RecordID(dataset.ID, dataset.VersionID),
		idempotency.InProgress).
		WithFargateTaskARN("arn:aws:dynamoDB:test:test:test")
}

type MockFailingObjectProcessor struct {
	FailOnPaths   map[string]bool
	RealProcessor objects.Processor
}

func NewMockFailingObjectProcessor(s3Client *s3.Client, failOnPaths ...string) *MockFailingObjectProcessor {
	realProcessor := objects.NewRehydrator(s3Client, ThresholdSize, logging.Default)
	mock := MockFailingObjectProcessor{FailOnPaths: map[string]bool{}, RealProcessor: realProcessor}
	for _, p := range failOnPaths {
		mock.FailOnPaths[p] = true
	}
	return &mock
}

func (m MockFailingObjectProcessor) Copy(ctx context.Context, source objects.Source, destination objects.Destination) error {
	if _, fail := m.FailOnPaths[source.GetPath()]; fail {
		return fmt.Errorf("error copying %s", source.GetVersionedUri())
	}
	return m.RealProcessor.Copy(ctx, source, destination)
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

type MockEmailer struct {
	complete []mockCompleteEmailCall
	failed   []mockFailedEmailCall
}

type mockEmailCall struct {
	dataset models.Dataset
	user    models.User
}

type mockCompleteEmailCall struct {
	mockEmailCall
	rehydrationLocation string
}

type mockFailedEmailCall struct {
	mockEmailCall
	requestID string
}

func (m *MockEmailer) SendRehydrationComplete(_ context.Context, dataset models.Dataset, user models.User, rehydrationLocation string) error {
	m.complete = append(m.complete, mockCompleteEmailCall{
		mockEmailCall:       mockEmailCall{dataset: dataset, user: user},
		rehydrationLocation: rehydrationLocation,
	})
	return nil
}

func (m *MockEmailer) SendRehydrationFailed(_ context.Context, dataset models.Dataset, user models.User, requestID string) error {
	m.failed = append(m.failed, mockFailedEmailCall{
		mockEmailCall: mockEmailCall{dataset: dataset, user: user},
		requestID:     requestID,
	})
	return nil
}
