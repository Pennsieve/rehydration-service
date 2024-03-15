package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/pennsieve/rehydration-service/service/handler"
	"github.com/pennsieve/rehydration-service/service/models"
	sharedidempotency "github.com/pennsieve/rehydration-service/shared/idempotency"
	sharedmodels "github.com/pennsieve/rehydration-service/shared/models"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/pennsieve/rehydration-service/shared/tracking"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

var rehydrationServiceHandlerEnv = test.NewEnvironmentVariables().
	With("TASK_DEF_ARN", "test-ecs-task-definition-arn").
	With("SUBNET_IDS", "test-subnet-1, test-subnet-2").
	With("CLUSTER_ARN", "test-cluster-arn").
	With("SECURITY_GROUP", "test-sg").
	With("TASK_DEF_CONTAINER_NAME", "test-rehydrate-fargate-container").
	With("ENV", "test")

func TestRehydrationServiceHandler(t *testing.T) {
	rehydrationServiceHandlerEnv.Setenv(t)

	request := models.Request{
		Dataset: sharedmodels.Dataset{ID: 5065, VersionID: 2},
		User:    sharedmodels.User{Name: "First Last", Email: "last@example.com"},
	}
	expectedTaskARN := "arn:aws:ecs:test-task-arn"

	fixture := NewFixtureBuilder(t).withECSRequestAssertionFunc(request).withExpectedTaskARN(expectedTaskARN).withIdempotencyTable().withTrackingTable().build()
	defer fixture.teardown()

	lambdaRequest := newLambdaRequest(requestToBody(t, request))
	ctx := context.Background()
	expectedStatusCode := 202
	beforeRequest := time.Now()
	response, err := handler.RehydrationServiceHandler(ctx, lambdaRequest)
	require.NoError(t, err)
	afterRequest := time.Now()
	assert.Equal(t, expectedStatusCode, response.StatusCode,
		"expected status code %v, got %v", expectedStatusCode, response.StatusCode)
	assert.Contains(t, response.Body, expectedTaskARN)

	idempotencyItems := fixture.dyDB.Scan(ctx, fixture.idempotencyTable)
	require.Len(t, idempotencyItems, 1)
	record, err := sharedidempotency.FromItem(idempotencyItems[0])
	require.NoError(t, err)
	assert.Equal(t, sharedidempotency.RecordID(request.ID, request.VersionID), record.ID)
	assert.Equal(t, sharedidempotency.InProgress, record.Status)
	assert.Empty(t, record.RehydrationLocation)
	assert.Equal(t, expectedTaskARN, record.FargateTaskARN)

	trackingItems := fixture.dyDB.Scan(ctx, fixture.trackingTable)
	require.Len(t, trackingItems, 1)
	entry, err := tracking.FromItem(trackingItems[0])
	require.NoError(t, err)
	assert.Equal(t, request.Dataset.DatasetVersion(), entry.DatasetVersion)
	assert.Equal(t, tracking.InProgress, entry.RehydrationStatus)
	assert.Equal(t, request.User.Name, entry.UserName)
	assert.Equal(t, request.User.Email, entry.UserEmail)
	assert.Equal(t, expectedTaskARN, entry.FargateTaskARN)
	assert.False(t, beforeRequest.After(entry.RequestDate))
	assert.False(t, afterRequest.Before(entry.RequestDate))
	assert.Nil(t, entry.EmailSentDate)
	// Don't know the expected value of this without access to the RehydrationRequest object.
	assert.NotEmpty(t, entry.ID)

}

func TestRehydrationServiceHandler_InProgress(t *testing.T) {
	rehydrationServiceHandlerEnv.Setenv(t)

	dataset := sharedmodels.Dataset{ID: 5065, VersionID: 2}
	user := sharedmodels.User{Name: "First Last", Email: "last@example.com"}
	inProgress := sharedidempotency.Record{
		ID:             sharedidempotency.RecordID(dataset.ID, dataset.VersionID),
		Status:         sharedidempotency.InProgress,
		FargateTaskARN: "arn:aws:ecs:test:test:test:test",
	}
	existingInProgressTracking := tracking.NewEntry(uuid.NewString(), dataset, user, uuid.NewString(), uuid.NewString(), inProgress.FargateTaskARN)
	fixture := NewFixtureBuilder(t).withIdempotencyTable(inProgress).withTrackingTable(*existingInProgressTracking).build()
	defer fixture.teardown()

	request := models.Request{
		Dataset: dataset,
		User:    user,
	}
	lambdaRequest := newLambdaRequest(requestToBody(t, request))
	ctx := context.Background()
	expectedStatusCode := 202
	beforeRequest := time.Now()
	response, err := handler.RehydrationServiceHandler(ctx, lambdaRequest)
	require.NoError(t, err)
	afterRequest := time.Now()
	require.Equal(t, expectedStatusCode, response.StatusCode,
		"expected status code %v, got %v", expectedStatusCode, response.StatusCode)
	require.Contains(t, response.Body, inProgress.FargateTaskARN)

	scanned := fixture.dyDB.Scan(ctx, fixture.idempotencyTable)
	require.Len(t, scanned, 1)
	record, err := sharedidempotency.FromItem(scanned[0])
	require.NoError(t, err)
	assert.Equal(t, inProgress, *record)

	trackingItems := fixture.dyDB.Scan(ctx, fixture.trackingTable)
	assert.Len(t, trackingItems, 2)
	for _, i := range trackingItems {
		entry, err := tracking.FromItem(i)
		require.NoError(t, err)
		assert.Equal(t, dataset.DatasetVersion(), entry.DatasetVersion)
		assert.Equal(t, tracking.InProgress, entry.RehydrationStatus)
		assert.Equal(t, user.Name, entry.UserName)
		assert.Equal(t, user.Email, entry.UserEmail)
		assert.Equal(t, existingInProgressTracking.FargateTaskARN, entry.FargateTaskARN)
		assert.Nil(t, entry.EmailSentDate)
		if entry.ID == existingInProgressTracking.ID {
			assert.Equal(t, existingInProgressTracking.RequestDate.Format(time.RFC3339Nano), entry.RequestDate.Format(time.RFC3339Nano))
		} else {
			// Don't know the expected value of this without access to the RehydrationRequest object.
			assert.NotEmpty(t, entry.ID)
			assert.False(t, beforeRequest.After(entry.RequestDate))
			assert.False(t, afterRequest.Before(entry.RequestDate))
		}

	}
}

//TODO finish updating the tests below for tracking. Fiqure out why DynamoDB CreateTable sometimes fails only when running 'make test'

func TestRehydrationServiceHandler_Expired(t *testing.T) {
	rehydrationServiceHandlerEnv.Setenv(t)

	dataset := sharedmodels.Dataset{ID: 5065, VersionID: 2}
	expired := sharedidempotency.Record{
		ID:                  sharedidempotency.RecordID(dataset.ID, dataset.VersionID),
		RehydrationLocation: fmt.Sprintf("some/location/%s", sharedidempotency.RecordID(dataset.ID, dataset.VersionID)),
		Status:              sharedidempotency.Expired,
		FargateTaskARN:      "arn:aws:ecs:test:test:test:test",
	}
	fixture := NewFixtureBuilder(t).withIdempotencyTable(expired).build()
	defer fixture.teardown()

	user := sharedmodels.User{Name: "First Last", Email: "last@example.com"}
	request := models.Request{
		Dataset: dataset,
		User:    user,
	}
	lambdaRequest := newLambdaRequest(requestToBody(t, request))

	expectedStatusCode := 500
	response, err := handler.RehydrationServiceHandler(context.Background(), lambdaRequest)
	require.NoError(t, err)
	require.Equal(t, expectedStatusCode, response.StatusCode,
		"expected status code %v, got %v", expectedStatusCode, response.StatusCode)
	require.Contains(t, response.Body, "expiration in progress")

	scanned := fixture.dyDB.Scan(context.Background(), fixture.idempotencyTable)
	require.Len(t, scanned, 1)
	record, err := sharedidempotency.FromItem(scanned[0])
	require.NoError(t, err)
	assert.Equal(t, expired, *record)
}

func TestRehydrationServiceHandler_Completed(t *testing.T) {
	rehydrationServiceHandlerEnv.Setenv(t)

	dataset := sharedmodels.Dataset{ID: 5065, VersionID: 2}
	completed := sharedidempotency.Record{
		ID:                  sharedidempotency.RecordID(dataset.ID, dataset.VersionID),
		RehydrationLocation: fmt.Sprintf("some/location/%s", sharedidempotency.RecordID(dataset.ID, dataset.VersionID)),
		Status:              sharedidempotency.Completed,
		FargateTaskARN:      "arn:aws:ecs:test:test:test:test",
	}
	fixture := NewFixtureBuilder(t).withIdempotencyTable(completed).build()
	defer fixture.teardown()

	user := sharedmodels.User{Name: "First Last", Email: "last@example.com"}
	request := models.Request{
		Dataset: dataset,
		User:    user,
	}
	lambdaRequest := newLambdaRequest(requestToBody(t, request))
	expectedStatusCode := 202
	response, err := handler.RehydrationServiceHandler(context.Background(), lambdaRequest)
	require.NoError(t, err)
	assert.Equal(t, expectedStatusCode, response.StatusCode,
		"expected status code %v, got %v", expectedStatusCode, response.StatusCode)
	assert.Contains(t, response.Body, completed.RehydrationLocation)
	assert.Contains(t, response.Body, completed.FargateTaskARN)

	scanned := fixture.dyDB.Scan(context.Background(), fixture.idempotencyTable)
	require.Len(t, scanned, 1)
	record, err := sharedidempotency.FromItem(scanned[0])
	require.NoError(t, err)
	assert.Equal(t, completed, *record)
}

func TestRehydrationServiceHandler_ECSError(t *testing.T) {
	rehydrationServiceHandlerEnv.Setenv(t)

	expectedStatusCode := 500
	errorBody := `{"code": "ECSError", "message": "server error on ECS"}`
	fixture := NewFixtureBuilder(t).withECSError(expectedStatusCode, errorBody).withIdempotencyTable().build()
	defer fixture.teardown()

	dataset := sharedmodels.Dataset{ID: 5065, VersionID: 2}
	user := sharedmodels.User{Name: "First Last", Email: "last@example.com"}
	request := models.Request{
		Dataset: dataset,
		User:    user,
	}
	lambdaRequest := newLambdaRequest(requestToBody(t, request))
	response, err := handler.RehydrationServiceHandler(context.Background(), lambdaRequest)
	require.NoError(t, err)
	assert.Equal(t, expectedStatusCode, response.StatusCode,
		"expected status code %v, got %v", expectedStatusCode, response.StatusCode)
	fmt.Println(response.Body)

	scanned := fixture.dyDB.Scan(context.Background(), fixture.idempotencyTable)
	require.Len(t, scanned, 0)
}

func TestRehydrationServiceHandler_BadRequests(t *testing.T) {
	rehydrationServiceHandlerEnv.Setenv(t)

	fixture := NewFixtureBuilder(t).build()
	defer fixture.teardown()

	for name, params := range map[string]struct {
		body                 string
		expectedResponsePart string
	}{
		"empty body":               {"", "unmarshall"},
		"non-json body":            {"not a json body", "unmarshall"},
		"wrong format":             {`{"some": "other", "wrong": "format"}`, "missing"},
		"missing datasetId":        {requestToBody(t, models.Request{Dataset: sharedmodels.Dataset{VersionID: 3}, User: sharedmodels.User{Name: "First Last", Email: "last@example.com"}}), "datasetId"},
		"missing datasetVersionId": {requestToBody(t, models.Request{Dataset: sharedmodels.Dataset{ID: 3879}, User: sharedmodels.User{Name: "First Last", Email: "last@example.com"}}), "datasetVersionId"},
		"empty name":               {requestToBody(t, models.Request{Dataset: sharedmodels.Dataset{ID: 3879, VersionID: 4}, User: sharedmodels.User{Email: "last@example.com"}}), "name"},
		"empty email":              {requestToBody(t, models.Request{Dataset: sharedmodels.Dataset{ID: 3879, VersionID: 4}, User: sharedmodels.User{Name: "First Last"}}), "email"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			request := newLambdaRequest(params.body)

			response, err := handler.RehydrationServiceHandler(ctx, request)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, response.StatusCode,
				"expected status code %v, got %v", http.StatusBadRequest, response.StatusCode)
			assert.Contains(t, response.Body, params.expectedResponsePart)
		})
	}
}

func requestToBody(t *testing.T, request models.Request) string {
	bytes, err := json.Marshal(request)
	require.NoError(t, err)
	return string(bytes)
}

func newLambdaRequest(body string) events.APIGatewayV2HTTPRequest {
	requestContext := events.APIGatewayV2HTTPRequestContext{
		HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{
			Method: "POST",
		},
		Authorizer: &events.APIGatewayV2HTTPRequestContextAuthorizerDescription{
			Lambda: make(map[string]interface{}),
		},
	}

	return events.APIGatewayV2HTTPRequest{
		RouteKey:       "POST /discover/rehydrate",
		Body:           body,
		RequestContext: requestContext,
	}
}

func taskARNResponse(t require.TestingT, expectedTaskARN string) *test.HTTPTestResponse {
	respMap := map[string][]map[string]*string{"tasks": {{"taskArn": aws.String(expectedTaskARN)}}}
	respBytes, err := json.Marshal(respMap)
	require.NoError(t, err)
	respBody := string(respBytes)
	return &test.HTTPTestResponse{Body: respBody}
}

type Fixture struct {
	awsConfig        aws.Config
	mockECS2         test.HTTPTestFixture
	dyDB             *test.DynamoDBFixture
	idempotencyTable string
	trackingTable    string
}

func (f *Fixture) teardown() {
	f.mockECS2.Teardown()
	if f.dyDB != nil {
		f.dyDB.Teardown()
	}
	handler.AWSConfigFactory.Set(nil)
}

type FixtureBuilder struct {
	testingT                *testing.T
	logAWSRequests          bool
	mockECSResponse         *test.HTTPTestResponse
	ecsRequestAssertionFunc test.RequestAssertionFunc
	createTableInputs       []*dynamodb.CreateTableInput
	putItemInputs           []*dynamodb.PutItemInput
	idempotencyTableName    string
	trackingTableName       string
}

func NewFixtureBuilder(t *testing.T) *FixtureBuilder {
	return &FixtureBuilder{testingT: t}
}

// Built Fixture will have a mock ECS server that will always return the given task ARN. If this method is not called,
// the mock ECS server will fail the test if any request are received.
func (b *FixtureBuilder) withExpectedTaskARN(expectedTaskARN string) *FixtureBuilder {
	b.mockECSResponse = taskARNResponse(b.testingT, expectedTaskARN)
	return b
}

// Built Fixture will have a mock ECS server that will always return the given error. If this method is not called,
// the mock ECS server will fail the test if any request are received.
func (b *FixtureBuilder) withECSError(httpStatus int, responseBody string) *FixtureBuilder {
	b.mockECSResponse = &test.HTTPTestResponse{Status: httpStatus, Body: responseBody}
	return b
}

func (b *FixtureBuilder) withECSRequestAssertionFunc(rehydrationReq models.Request) *FixtureBuilder {
	expectedContainerOverrides := expectedECSContainerOverrides(b.testingT, rehydrationReq)
	b.ecsRequestAssertionFunc = func(t require.TestingT, request *http.Request) bool {
		// Ideally, we'd decode the body into the RunTaskInput struct that it represents, but
		// AWS has something specialized going on, so a straight application of the json package
		// does not Unmarshall the way we want.
		var reqMap map[string]any
		err := json.NewDecoder(request.Body).Decode(&reqMap)
		if decoded := assert.NoError(t, err, "error decoding request body to a map"); !decoded {
			return decoded
		}
		overrides := reqMap["overrides"].(map[string]any)
		containerOverridesSlice := overrides["containerOverrides"].([]any)
		containerOverrides := containerOverridesSlice[0].(map[string]any)
		fmt.Printf("%#v", containerOverrides)
		if passed := assertECSContainerOverridesEqual(t, expectedContainerOverrides, containerOverrides); !passed {
			return false
		}
		return true
	}
	return b
}

func expectedECSContainerOverrides(t require.TestingT, rehydrationReq models.Request) map[string]any {
	envValue, ok := os.LookupEnv(sharedmodels.ECSTaskEnvKey)
	require.Truef(t, ok, "env variable %s is not set", sharedmodels.ECSTaskEnvKey)
	idempotencyTableValue, ok := os.LookupEnv(sharedidempotency.TableNameKey)
	require.True(t, ok, "env variable %s is not set", sharedidempotency.TableNameKey)
	trackingTableValue, ok := os.LookupEnv(tracking.TableNameKey)
	require.True(t, ok, "env variable %s is not set", tracking.TableNameKey)
	containerNameValue, ok := os.LookupEnv("TASK_DEF_CONTAINER_NAME")
	require.True(t, ok, "env variable TASK_DEF_CONTAINER_NAME is not set")
	return map[string]any{
		"environment": []any{
			map[string]any{"name": sharedmodels.ECSTaskEnvKey, "value": envValue},
			map[string]any{"name": sharedmodels.ECSTaskDatasetIDKey, "value": strconv.Itoa(rehydrationReq.Dataset.ID)},
			map[string]any{"name": sharedmodels.ECSTaskDatasetVersionIDKey, "value": strconv.Itoa(rehydrationReq.Dataset.VersionID)},
			map[string]any{"name": sharedmodels.ECSTaskUserNameKey, "value": rehydrationReq.User.Name},
			map[string]any{"name": sharedmodels.ECSTaskUserEmailKey, "value": rehydrationReq.User.Email},
			map[string]any{"name": sharedidempotency.TableNameKey, "value": idempotencyTableValue},
			map[string]any{"name": tracking.TableNameKey, "value": trackingTableValue}},
		"name": containerNameValue}
}

func assertECSContainerOverridesEqual(t require.TestingT, expected map[string]any, actual map[string]any) bool {
	expectedName, actualName := expected["name"], actual["name"]
	expectedEnv, actualEnv := expected["environment"], actual["environment"]
	return assert.Equal(t, expectedName, actualName) && assert.ElementsMatch(t, expectedEnv, actualEnv)
}

func (b *FixtureBuilder) withIdempotencyTable(records ...sharedidempotency.Record) *FixtureBuilder {
	table, ok := os.LookupEnv(sharedidempotency.TableNameKey)
	if !ok || len(table) == 0 {
		assert.FailNow(b.testingT, "idempotency table name missing from environment variables or empty", "env var name: %s", sharedidempotency.TableNameKey)
	}
	b.idempotencyTableName = table
	b.createTableInputs = append(b.createTableInputs, test.IdempotencyCreateTableInput(table, sharedidempotency.KeyAttrName))
	for i := range records {
		record := &records[i]
		b.putItemInputs = append(b.putItemInputs, test.ItemersToPutItemInputs(b.testingT, b.idempotencyTableName, record)...)
	}
	return b
}

func (b *FixtureBuilder) withTrackingTable(entries ...tracking.Entry) *FixtureBuilder {
	table, ok := os.LookupEnv(tracking.TableNameKey)
	if !ok || len(table) == 0 {
		assert.FailNow(b.testingT, "tracking table name missing from environment variables or empty", "env var name: %s", tracking.TableNameKey)
	}
	b.trackingTableName = table
	b.createTableInputs = append(b.createTableInputs, test.TrackingCreateTableInput(table, tracking.IDAttrName))
	for i := range entries {
		entry := &entries[i]
		b.putItemInputs = append(b.putItemInputs, test.ItemersToPutItemInputs(b.testingT, b.trackingTableName, entry)...)
	}
	return b
}

func (b *FixtureBuilder) withLoggedAWSRequests() *FixtureBuilder {
	b.logAWSRequests = true
	return b
}

func (b *FixtureBuilder) build() *Fixture {
	mockECS := test.NewHTTPTestFixture(b.testingT, b.ecsRequestAssertionFunc, b.mockECSResponse)

	awsConfig := test.NewAWSEndpoints(b.testingT).
		WithDynamoDB().
		WithECS(mockECS.Server.URL).
		Config(context.Background(), b.logAWSRequests)
	handler.AWSConfigFactory.Set(&awsConfig)

	dyDB := test.NewDynamoDBFixture(b.testingT, awsConfig, b.createTableInputs...).WithItems(b.putItemInputs...)

	return &Fixture{
		awsConfig:        awsConfig,
		mockECS2:         mockECS,
		dyDB:             dyDB,
		idempotencyTable: b.idempotencyTableName,
		trackingTable:    b.trackingTableName,
	}
}
