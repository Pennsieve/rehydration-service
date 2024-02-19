package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pennsieve/rehydration-service/service/handler"
	idempotency2 "github.com/pennsieve/rehydration-service/service/idempotency"
	"github.com/pennsieve/rehydration-service/service/models"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
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

	expectedTaskARN := "arn:aws:ecs:test-task-arn"
	fixture := NewFixtureBuilder(t).withExpectedTaskARN(expectedTaskARN).withIdempotencyTable().build()
	defer fixture.teardown()

	request := models.Request{
		Dataset: models.Dataset{ID: 5065, VersionID: 2},
		User:    models.User{Name: "First Last", Email: "last@example.com"},
	}
	lambdaRequest := newLambdaRequest(requestToBody(t, request))
	ctx := context.Background()
	expectedStatusCode := 202
	response, err := handler.RehydrationServiceHandler(ctx, lambdaRequest)
	require.NoError(t, err)
	assert.Equal(t, expectedStatusCode, response.StatusCode,
		"expected status code %v, got %v", expectedStatusCode, response.StatusCode)
	assert.Contains(t, response.Body, expectedTaskARN)

	scanned := fixture.dyDB.Scan(ctx, fixture.idempotencyTable)
	require.Len(t, scanned, 1)
	record, err := idempotency.FromItem(scanned[0])
	require.NoError(t, err)
	assert.Equal(t, idempotency.RecordID(request.ID, request.VersionID), record.ID)
	assert.Equal(t, idempotency.InProgress, record.Status)
	assert.Empty(t, record.RehydrationLocation)
	assert.Equal(t, expectedTaskARN, record.FargateTaskARN)
}

func TestRehydrationServiceHandler_InProgress(t *testing.T) {
	rehydrationServiceHandlerEnv.Setenv(t)

	dataset := models.Dataset{ID: 5065, VersionID: 2}
	inProgress := idempotency.Record{
		ID:                  idempotency.RecordID(dataset.ID, dataset.VersionID),
		RehydrationLocation: fmt.Sprintf("some/location/%s", idempotency.RecordID(dataset.ID, dataset.VersionID)),
		Status:              idempotency.InProgress,
		FargateTaskARN:      "arn:aws:ecs:test:test:test:test",
	}
	fixture := NewFixtureBuilder(t).withIdempotencyTable().withIdempotencyRecords(inProgress).build()
	defer fixture.teardown()

	user := models.User{Name: "First Last", Email: "last@example.com"}
	request := models.Request{
		Dataset: dataset,
		User:    user,
	}
	lambdaRequest := newLambdaRequest(requestToBody(t, request))

	expectedStatusCode := 500
	response, err := handler.RehydrationServiceHandler(context.Background(), lambdaRequest)
	require.Error(t, err)
	var inProgressError idempotency2.InProgressError
	require.ErrorAs(t, err, &inProgressError)
	require.Equal(t, expectedStatusCode, response.StatusCode,
		"expected status code %v, got %v", expectedStatusCode, response.StatusCode)
	require.Contains(t, response.Body, inProgressError.Error())

	scanned := fixture.dyDB.Scan(context.Background(), fixture.idempotencyTable)
	require.Len(t, scanned, 1)
	record, err := idempotency.FromItem(scanned[0])
	require.NoError(t, err)
	assert.Equal(t, inProgress, *record)
}

func TestRehydrationServiceHandler_Expired(t *testing.T) {
	rehydrationServiceHandlerEnv.Setenv(t)

	dataset := models.Dataset{ID: 5065, VersionID: 2}
	expired := idempotency.Record{
		ID:                  idempotency.RecordID(dataset.ID, dataset.VersionID),
		RehydrationLocation: fmt.Sprintf("some/location/%s", idempotency.RecordID(dataset.ID, dataset.VersionID)),
		Status:              idempotency.Expired,
		FargateTaskARN:      "arn:aws:ecs:test:test:test:test",
	}
	fixture := NewFixtureBuilder(t).withIdempotencyTable().withIdempotencyRecords(expired).build()
	defer fixture.teardown()

	user := models.User{Name: "First Last", Email: "last@example.com"}
	request := models.Request{
		Dataset: dataset,
		User:    user,
	}
	lambdaRequest := newLambdaRequest(requestToBody(t, request))

	expectedStatusCode := 500
	response, err := handler.RehydrationServiceHandler(context.Background(), lambdaRequest)
	require.Error(t, err)
	var expiredError idempotency2.ExpiredError
	require.ErrorAs(t, err, &expiredError)
	require.Equal(t, expectedStatusCode, response.StatusCode,
		"expected status code %v, got %v", expectedStatusCode, response.StatusCode)
	require.Contains(t, response.Body, expiredError.Error())

	scanned := fixture.dyDB.Scan(context.Background(), fixture.idempotencyTable)
	require.Len(t, scanned, 1)
	record, err := idempotency.FromItem(scanned[0])
	require.NoError(t, err)
	assert.Equal(t, expired, *record)
}

func TestRehydrationServiceHandler_Completed(t *testing.T) {
	rehydrationServiceHandlerEnv.Setenv(t)

	dataset := models.Dataset{ID: 5065, VersionID: 2}
	completed := idempotency.Record{
		ID:                  idempotency.RecordID(dataset.ID, dataset.VersionID),
		RehydrationLocation: fmt.Sprintf("some/location/%s", idempotency.RecordID(dataset.ID, dataset.VersionID)),
		Status:              idempotency.Completed,
		FargateTaskARN:      "arn:aws:ecs:test:test:test:test",
	}
	fixture := NewFixtureBuilder(t).withIdempotencyTable().withIdempotencyRecords(completed).build()
	defer fixture.teardown()

	user := models.User{Name: "First Last", Email: "last@example.com"}
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
	record, err := idempotency.FromItem(scanned[0])
	require.NoError(t, err)
	assert.Equal(t, completed, *record)
}

func TestRehydrationServiceHandler_ECSError(t *testing.T) {
	rehydrationServiceHandlerEnv.Setenv(t)

	expectedStatusCode := 500
	errorBody := `{"code": "ECSError", "message": "server error on ECS"}`
	fixture := NewFixtureBuilder(t).withECSError(expectedStatusCode, errorBody).withIdempotencyTable().build()
	defer fixture.teardown()

	dataset := models.Dataset{ID: 5065, VersionID: 2}
	user := models.User{Name: "First Last", Email: "last@example.com"}
	request := models.Request{
		Dataset: dataset,
		User:    user,
	}
	lambdaRequest := newLambdaRequest(requestToBody(t, request))
	response, err := handler.RehydrationServiceHandler(context.Background(), lambdaRequest)
	require.Error(t, err)
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
		"missing datasetId":        {requestToBody(t, models.Request{Dataset: models.Dataset{VersionID: 3}, User: models.User{Name: "First Last", Email: "last@example.com"}}), "datasetId"},
		"missing datasetVersionId": {requestToBody(t, models.Request{Dataset: models.Dataset{ID: 3879}, User: models.User{Name: "First Last", Email: "last@example.com"}}), "datasetVersionId"},
		"empty name":               {requestToBody(t, models.Request{Dataset: models.Dataset{ID: 3879, VersionID: 4}, User: models.User{Email: "last@example.com"}}), "name"},
		"empty email":              {requestToBody(t, models.Request{Dataset: models.Dataset{ID: 3879, VersionID: 4}, User: models.User{Name: "First Last"}}), "email"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			request := newLambdaRequest(params.body)

			response, err := handler.RehydrationServiceHandler(ctx, request)
			require.Error(t, err)
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

func taskARNHandlerFunction(t *testing.T, expectedTaskARN string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		respMap := map[string][]map[string]*string{"tasks": {{"taskArn": aws.String(expectedTaskARN)}}}
		respBytes, err := json.Marshal(respMap)
		require.NoError(t, err)
		respBody := string(respBytes)
		written, err := fmt.Fprintln(writer, respBody)
		require.NoError(t, err)
		// +1 for the newline
		require.Equal(t, len(respBody)+1, written)
	}
}

func assertNoECSCallsHandlerFunction(t *testing.T) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		requestBody, err := io.ReadAll(request.Body)
		require.NoError(t, err)
		assert.FailNow(t, "unexpected call to ECS endpoint", "request body: %s", string(requestBody))
	}
}

func returnErrorHandlerFunction(t *testing.T, returnStatus int, returnBody string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(returnStatus)
		written, err := fmt.Fprintln(writer, returnBody)
		require.NoError(t, err)
		// +1 for the newline
		require.Equal(t, len(returnBody)+1, written)
	}
}

type Fixture struct {
	awsConfig        aws.Config
	mockECS          *httptest.Server
	dyDB             *test.DynamoDBFixture
	idempotencyTable string
}

func (f *Fixture) teardown() {
	if f.mockECS != nil {
		f.mockECS.Close()
	}
	if f.dyDB != nil {
		f.dyDB.Teardown()
	}
	handler.AWSConfigFactory.Set(nil)
}

type FixtureBuilder struct {
	testingT               *testing.T
	logAWSRequests         bool
	mockECSHandlerFunction http.HandlerFunc
	createTableInputs      []*dynamodb.CreateTableInput
	putItemInputs          []*dynamodb.PutItemInput
	idempotencyTableName   string
}

func NewFixtureBuilder(t *testing.T) *FixtureBuilder {
	return &FixtureBuilder{testingT: t}
}

// Built Fixture will have a mock ECS server that will always return the given task ARN. If this method is not called,
// the mock ECS server will fail the test if any request are received.
func (b *FixtureBuilder) withExpectedTaskARN(expectedTaskARN string) *FixtureBuilder {
	b.mockECSHandlerFunction = taskARNHandlerFunction(b.testingT, expectedTaskARN)
	return b
}

// Built Fixture will have a mock ECS server that will always return the given error. If this method is not called,
// the mock ECS server will fail the test if any request are received.
func (b *FixtureBuilder) withECSError(httpStatus int, responseBody string) *FixtureBuilder {
	b.mockECSHandlerFunction = returnErrorHandlerFunction(b.testingT, httpStatus, responseBody)
	return b
}

func (b *FixtureBuilder) withIdempotencyTable() *FixtureBuilder {
	table, ok := os.LookupEnv(idempotency.TableNameKey)
	if !ok || len(table) == 0 {
		assert.FailNow(b.testingT, "idempotency table name missing from environment variables or empty", "env var name: %s", idempotency.KeyAttrName)
	}
	b.idempotencyTableName = table
	b.createTableInputs = append(b.createTableInputs, test.IdempotencyCreateTableInput(table, idempotency.KeyAttrName))
	return b
}

func (b *FixtureBuilder) withIdempotencyRecords(records ...idempotency.Record) *FixtureBuilder {
	if len(b.idempotencyTableName) == 0 {
		assert.FailNow(b.testingT, "idempotencyTableName is empty; call withIdempotencyTable before calling this method")
	}
	for i := range records {
		record := &records[i]
		b.putItemInputs = append(b.putItemInputs, test.RecordsToPutItemInputs(b.testingT, b.idempotencyTableName, record)...)
	}
	return b
}

func (b *FixtureBuilder) withLoggedAWSRequests() *FixtureBuilder {
	b.logAWSRequests = true
	return b
}

func (b *FixtureBuilder) build() *Fixture {
	if b.mockECSHandlerFunction == nil {
		b.mockECSHandlerFunction = assertNoECSCallsHandlerFunction(b.testingT)
	}
	mockECS := httptest.NewServer(b.mockECSHandlerFunction)

	awsEndpoints := test.NewAwsEndpointMap().WithECS(mockECS.URL)
	awsConfig := test.GetTestAWSConfig(b.testingT, awsEndpoints, b.logAWSRequests)
	handler.AWSConfigFactory.Set(&awsConfig)

	dyDB := test.NewDynamoDBFixture(b.testingT, awsConfig, b.createTableInputs...).WithItems(b.putItemInputs...)

	return &Fixture{
		awsConfig:        awsConfig,
		mockECS:          mockECS,
		dyDB:             dyDB,
		idempotencyTable: b.idempotencyTableName,
	}
}
