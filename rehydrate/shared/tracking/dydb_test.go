package tracking_test

import (
	"context"
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
	store := tracking.NewStore(awsConfig, logging.Default, testTableName)

	dyDB := test.NewDynamoDBFixture(t, awsConfig, test.TrackingCreateTableInput(testTableName, tracking.IDAttrName))
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
		},
		LambdaLogStream: expectedLambdaLog,
		AWSRequestID:    awsRequestID,
		RequestDate:     requestDate,
		EmailSentDate:   &emailSentDate,
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
	store := tracking.NewStore(awsConfig, logging.Default, testTableName)

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

	dyDB := test.NewDynamoDBFixture(t, awsConfig, test.TrackingCreateTableInput(testTableName, tracking.IDAttrName)).WithItems(test.ItemersToPutItemInputs(t, testTableName, origEntry)...)
	defer dyDB.Teardown()

	emailSentDate := time.Now().Add(time.Hour * 7)
	expectedStatus := tracking.Completed
	require.NoError(t, store.EmailSent(ctx, expectedID, emailSentDate, expectedStatus))

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
	err := store.EmailSent(ctx, expectedID, emailSentDate, expectedStatus)
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
	store := tracking.NewStore(awsConfig, logging.Default, testTableName)

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
	origEntries := []test.Itemer{
		tracking.NewEntry(uuid.NewString(), dataset, user1, uuid.NewString(), uuid.NewString(), expectedFargateTaskARN),
		tracking.NewEntry(uuid.NewString(), dataset, user2, uuid.NewString(), uuid.NewString(), expectedFargateTaskARN),
		tracking.NewEntry(uuid.NewString(), dataset, user1, uuid.NewString(), uuid.NewString(), expectedFargateTaskARN),
	}
	origEntryIndicesByID := map[string]tracking.DatasetVersionIndex{}
	for _, e := range origEntries {
		asEntry := e.(*tracking.Entry)
		origEntryIndicesByID[asEntry.ID] = asEntry.DatasetVersionIndex
	}

	dyDB := test.NewDynamoDBFixture(t, awsConfig, test.TrackingCreateTableInput(testTableName, tracking.IDAttrName)).WithItems(test.ItemersToPutItemInputs(t, testTableName, origEntries...)...)
	defer dyDB.Teardown()

	indexItems, err := store.QueryDatasetVersionIndex(ctx, dataset, 10)
	require.NoError(t, err)
	require.Len(t, indexItems, len(origEntries))
	for _, i := range indexItems {
		require.Contains(t, origEntryIndicesByID, i.ID)
		assert.Equal(t, origEntryIndicesByID[i.ID], i)
	}
}
