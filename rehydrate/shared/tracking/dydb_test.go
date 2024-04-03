package tracking_test

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"github.com/pennsieve/rehydration-service/shared/models"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/pennsieve/rehydration-service/shared/tracking"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var testTableName = "test-request-tracking-table"

func TestDyDBStore_PutEntry(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := tracking.NewStore(dyDBClient, logging.Default, testTableName)

	dyDB := test.NewDynamoDBFixture(t, awsConfig, test.TrackingCreateTableInput(testTableName))
	defer dyDB.Teardown()

	expectedID := uuid.NewString()
	dataset := models.Dataset{
		ID:        898,
		VersionID: 7,
	}
	user := models.User{
		Name:  "First Last",
		Email: "last@example.com",
	}
	expectedLambdaLog := "/lambda/log/stream"
	awsRequestID := "REQUEST-8765"
	requestDate := time.Now()
	emailSentDate := requestDate.Add(time.Hour + 2)
	entry := &tracking.Entry{
		DatasetVersionIndex: tracking.DatasetVersionIndex{
			ID:                expectedID,
			DatasetVersion:    dataset.DatasetVersion(),
			UserName:          user.Name,
			UserEmail:         user.Email,
			RehydrationStatus: tracking.InProgress,
			EmailSentDate:     &emailSentDate,
		},
		LambdaLogStream: expectedLambdaLog,
		AWSRequestID:    awsRequestID,
		RequestDate:     requestDate,
		FargateTaskARN:  "arn:ecs:test::test",
	}
	err := store.PutEntry(ctx, entry)
	require.NoError(t, err)

	items := dyDB.Scan(ctx, testTableName)
	require.Len(t, items, 1)
	AssertEqualEntryItem(t, *entry, items[0])
}

func TestDyDBStore_EmailSent(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := tracking.NewStore(dyDBClient, logging.Default, testTableName)

	expectedID := uuid.NewString()
	dataset := models.Dataset{
		ID:        898,
		VersionID: 7,
	}
	user := models.User{
		Name:  "First Last",
		Email: "last@example.com",
	}
	expectedLambdaLog := "/lambda/log/stream"
	expectedAWSRequestID := "REQUEST-8765"
	expectedFargateTaskARN := "arn::::test:test"
	origEntry := tracking.NewEntry(expectedID, dataset, user, expectedLambdaLog, expectedAWSRequestID, expectedFargateTaskARN)

	dyDB := test.NewDynamoDBFixture(t, awsConfig, test.TrackingCreateTableInput(testTableName)).WithItems(test.ItemersToPutItemInputs(t, testTableName, origEntry)...)
	defer dyDB.Teardown()

	emailSentDate := time.Now().Add(time.Hour * 7)
	expectedStatus := tracking.Completed
	require.NoError(t, store.EmailSent(ctx, expectedID, &emailSentDate, expectedStatus))

	items := dyDB.Scan(ctx, testTableName)
	require.Len(t, items, 1)
	updatedItem := items[0]

	AssertEqualAttributeValueString(t, emailSentDate.Format(time.RFC3339Nano), updatedItem[tracking.EmailSentDateAttrName])
	AssertEqualAttributeValueString(t, string(expectedStatus), updatedItem[tracking.RehydrationStatusAttrName])

	AssertEqualAttributeValueString(t, expectedID, updatedItem[tracking.IDAttrName])
	AssertEqualAttributeValueString(t, dataset.DatasetVersion(), updatedItem[tracking.DatasetVersionAttrName])
	AssertEqualAttributeValueString(t, user.Name, updatedItem[tracking.UserNameAttrName])
	AssertEqualAttributeValueString(t, user.Email, updatedItem[tracking.UserEmailAttrName])
	AssertEqualAttributeValueString(t, expectedLambdaLog, updatedItem[tracking.LambdaLogStreamAttrName])
	AssertEqualAttributeValueString(t, expectedAWSRequestID, updatedItem[tracking.AWSRequestIDAttrName])
	AssertEqualAttributeValueString(t, expectedFargateTaskARN, updatedItem[tracking.FargateTaskARNAttrName])
	AssertEqualAttributeValueString(t, origEntry.RequestDate.Format(time.RFC3339Nano), updatedItem[tracking.RequestDateAttrName])

	// A second try should fail
	err := store.EmailSent(ctx, expectedID, &emailSentDate, expectedStatus)
	var alreadyExistsError *tracking.EntryAlreadyExistsError
	if assert.ErrorAs(t, err, &alreadyExistsError) {
		assert.NoError(t, alreadyExistsError.UnmarshallingError)
		assert.NotNil(t, alreadyExistsError.Existing)
		AssertEqualEntryItem(t, *alreadyExistsError.Existing, updatedItem)
	}
}

func TestDyDBStore_QueryDatasetVersionIndex(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)
	store := tracking.NewStore(dyDBClient, logging.Default, testTableName)

	dataset := models.Dataset{
		ID:        898,
		VersionID: 7,
	}
	user1 := models.User{
		Name:  "First Last",
		Email: "last@example.com",
	}
	user2 := models.User{
		Name:  "Guy Sur",
		Email: "sur@example.com",
	}
	expectedFargateTaskARN := "arn::::test:test"
	// An entry from a previous, completed rehydration of the same dataset.
	// It's already been handled, so the Query being tested should not return it.
	oldCompletedRequestDate := time.Now().Add(-time.Hour * 24)
	oldCompletedEmailSentData := oldCompletedRequestDate.Add(time.Hour * 12)
	oldCompletedEntry := &tracking.Entry{
		DatasetVersionIndex: tracking.DatasetVersionIndex{
			ID:                uuid.NewString(),
			DatasetVersion:    dataset.DatasetVersion(),
			UserName:          "Hal Blaine",
			UserEmail:         "hb@example.com",
			RehydrationStatus: tracking.Completed,
			EmailSentDate:     &oldCompletedEmailSentData,
		},
		LambdaLogStream: uuid.NewString(),
		AWSRequestID:    uuid.NewString(),
		RequestDate:     oldCompletedRequestDate,
		FargateTaskARN:  uuid.NewString(),
	}
	// An entry from a previous, failed rehydration of the same dataset.
	// It's already been handled, so the Query being tested should not return it.
	oldFailedRequestDate := time.Now().Add(-time.Hour * 48)
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
	// New, as yet, unhandled entries. These are the only entries the Query being tested should find.
	unhandledEntries := []test.Itemer{
		tracking.NewEntry(uuid.NewString(), dataset, user1, uuid.NewString(), uuid.NewString(), expectedFargateTaskARN),
		tracking.NewEntry(uuid.NewString(), dataset, user2, uuid.NewString(), uuid.NewString(), expectedFargateTaskARN),
		tracking.NewEntry(uuid.NewString(), dataset, user1, uuid.NewString(), uuid.NewString(), expectedFargateTaskARN),
	}
	unhandledEntryIndicesByID := map[string]tracking.DatasetVersionIndex{}
	for _, e := range unhandledEntries {
		asEntry := e.(*tracking.Entry)
		unhandledEntryIndicesByID[asEntry.ID] = asEntry.DatasetVersionIndex
	}
	allEntries := append(unhandledEntries, oldCompletedEntry, oldFailedEntry)
	dyDB := test.NewDynamoDBFixture(t, awsConfig, test.TrackingCreateTableInput(testTableName)).WithItems(test.ItemersToPutItemInputs(t, testTableName, allEntries...)...)
	defer dyDB.Teardown()

	indexItems, err := store.QueryDatasetVersionIndexUnhandled(ctx, dataset, 2)
	require.NoError(t, err)
	require.Len(t, indexItems, len(unhandledEntries))
	for _, i := range indexItems {
		require.Contains(t, unhandledEntryIndicesByID, i.ID)
		assert.Equal(t, unhandledEntryIndicesByID[i.ID], i)
	}
}

func TestDyDBStore_QueryExpirationIndex(t *testing.T) {
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithDynamoDB().Config(ctx, false)
	dyDBClient := dynamodb.NewFromConfig(awsConfig)

	expirationPeriodDays := 14
	expirationPeriodHours := time.Duration(24 * expirationPeriodDays)
	today := time.Now()
	expirationThreshold := today.Add(-time.Hour * expirationPeriodHours)
	user := models.User{
		Name:  "First Last",
		Email: "last@example.com",
	}
	dataset1 := models.Dataset{
		ID:        244,
		VersionID: 2,
	}
	dataset2 := models.Dataset{
		ID:        355,
		VersionID: 3,
	}
	var entries []test.Itemer
	candidatesByEmailSentDate := map[string]*tracking.Entry{}

	toBeExpired1 := expirationThreshold.Add(-time.Second)
	expirationCandidate1 := withStatusAndEmailSent(test.NewTestEntry(dataset1, user), tracking.Completed, &toBeExpired1)
	candidatesByEmailSentDate[expirationCandidate1.EmailSentDate.Format(time.RFC3339Nano)] = expirationCandidate1
	entries = append(entries, expirationCandidate1)

	toBeExpired3 := expirationThreshold.Add(-time.Hour * 23)
	expirationCandidate2 := withStatusAndEmailSent(test.NewTestEntry(dataset1, user), tracking.Completed, &toBeExpired3)
	candidatesByEmailSentDate[expirationCandidate2.EmailSentDate.Format(time.RFC3339Nano)] = expirationCandidate2
	entries = append(entries, expirationCandidate2)

	entries = append(entries, withStatusAndEmailSent(test.NewTestEntry(dataset2, user), tracking.Expired, &toBeExpired3))

	toKeep2 := today.Add(-time.Hour)
	entries = append(entries, withStatusAndEmailSent(test.NewTestEntry(dataset1, user), tracking.Completed, &toKeep2))
	entries = append(entries, withStatusAndEmailSent(test.NewTestEntry(dataset2, user), tracking.Completed, &toKeep2))

	toKeep3 := today.Add(-time.Hour * 13)
	entries = append(entries, withStatusAndEmailSent(test.NewTestEntry(dataset1, user), tracking.Completed, &toKeep3))
	entries = append(entries, withStatusAndEmailSent(test.NewTestEntry(dataset2, user), tracking.Completed, &toKeep3))

	dyDB := test.NewDynamoDBFixture(t, awsConfig, test.TrackingCreateTableInput(testTableName)).WithItems(test.ItemersToPutItemInputs(t, testTableName, entries...)...)
	defer dyDB.Teardown()

	store := tracking.NewStore(dyDBClient, logging.Default, testTableName)
	results, err := store.QueryExpirationIndex(ctx, expirationThreshold, 10)
	require.NoError(t, err)
	assert.Len(t, results, len(candidatesByEmailSentDate))
	for _, r := range results {
		emailSentDateString := r.EmailSentDate.Format(time.RFC3339Nano)
		require.Contains(t, candidatesByEmailSentDate, emailSentDateString)
		expected := candidatesByEmailSentDate[emailSentDateString]
		assert.Equal(t, expected.DatasetVersion, r.DatasetVersion)
		assert.Equal(t, expected.RehydrationStatus, r.RehydrationStatus)
	}

}

func withStatusAndEmailSent(entry *tracking.Entry, status tracking.RehydrationStatus, emailSentDate *time.Time) *tracking.Entry {
	entry.RehydrationStatus = status
	entry.EmailSentDate = emailSentDate
	return entry
}
