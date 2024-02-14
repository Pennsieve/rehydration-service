package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pennsieve/rehydration-service/service/handler"
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
	mockECS := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		respMap := map[string][]map[string]*string{"tasks": {{"taskArn": aws.String(expectedTaskARN)}}}
		respBytes, err := json.Marshal(respMap)
		require.NoError(t, err)
		respBody := string(respBytes)
		written, err := fmt.Fprintln(writer, respBody)
		require.NoError(t, err)
		// +1 for the newline
		require.Equal(t, len(respBody)+1, written)
	}))
	defer mockECS.Close()

	testEndpoints := test.NewAwsEndpointMap().WithECS(mockECS.URL)
	testConfig := test.GetTestAWSConfig(t, testEndpoints, false)
	handler.AWSConfigFactory.Set(&testConfig)
	defer handler.AWSConfigFactory.Set(nil)
	table, ok := os.LookupEnv(idempotency.TableNameKey)
	if !ok || len(table) == 0 {
		assert.FailNow(t, "idempotency table name missing from environment variables or empty", "env var name: %s", idempotency.KeyAttrName)
	}
	dyDB := test.NewDynamoDBFixture(t, dynamodb.NewFromConfig(testConfig), test.IdempotencyCreateTableInput(table, idempotency.KeyAttrName))
	defer dyDB.Teardown()

	ctx := context.Background()
	requestContext := events.APIGatewayV2HTTPRequestContext{
		HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{
			Method: "POST",
		},
		Authorizer: &events.APIGatewayV2HTTPRequestContextAuthorizerDescription{
			Lambda: make(map[string]interface{}),
		},
	}
	body := fmt.Sprintf(`{"datasetId": %d, "datasetVersionId": %d, "name": %q, "email": %q}`, 5056, 2, "First Last", "last@example.com")
	request := events.APIGatewayV2HTTPRequest{
		RouteKey:       "POST /discover/rehydrate",
		Body:           body,
		RequestContext: requestContext,
	}

	expectedStatusCode := 202
	response, err := handler.RehydrationServiceHandler(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, expectedStatusCode, response.StatusCode,
		"expected status code %v, got %v", expectedStatusCode, response.StatusCode)
	assert.Contains(t, response.Body, expectedTaskARN)

}

func TestRehydrationServiceHandler_BadRequests(t *testing.T) {
	rehydrationServiceHandlerEnv.Setenv(t)

	mockECS := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestBody, err := io.ReadAll(request.Body)
		require.NoError(t, err)
		assert.FailNow(t, "unexpected call to ECS endpoint", "request body: %s", string(requestBody))
	}))
	defer mockECS.Close()

	testEndpoints := test.NewAwsEndpointMap().WithECS(mockECS.URL)
	testConfig := test.GetTestAWSConfig(t, testEndpoints, false)
	handler.AWSConfigFactory.Set(&testConfig)
	defer handler.AWSConfigFactory.Set(nil)

	requestContext := events.APIGatewayV2HTTPRequestContext{
		HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{
			Method: "POST",
		},
		Authorizer: &events.APIGatewayV2HTTPRequestContextAuthorizerDescription{
			Lambda: make(map[string]interface{}),
		},
	}
	bodyFormat := `{"datasetId": %d, "datasetVersionId": %d, "name": %q, "email": %q}`

	for name, params := range map[string]struct {
		body                 string
		expectedResponsePart string
	}{
		"empty body":               {"", "unmarshall"},
		"non-json body":            {"not a json body", "unmarshall"},
		"wrong format":             {`{"some": "other", "wrong": "format"}`, "missing"},
		"missing datasetId":        {fmt.Sprintf(bodyFormat, 0, 3, "First Last", "email@example.com"), "datasetId"},
		"missing datasetVersionId": {fmt.Sprintf(bodyFormat, 5034, 0, "First Last", "email@example.com"), "datasetVersionId"},
		"empty name":               {fmt.Sprintf(bodyFormat, 5034, 3, "", "email@example.com"), "name"},
		"empty email":              {fmt.Sprintf(bodyFormat, 5034, 3, "First last", ""), "email"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			request := events.APIGatewayV2HTTPRequest{
				RouteKey:       "POST /rehydrate",
				Body:           params.body,
				RequestContext: requestContext,
			}

			response, err := handler.RehydrationServiceHandler(ctx, request)
			require.Error(t, err)
			assert.Equal(t, http.StatusBadRequest, response.StatusCode,
				"expected status code %v, got %v", http.StatusBadRequest, response.StatusCode)
			assert.Contains(t, response.Body, params.expectedResponsePart)
		})
	}
}
