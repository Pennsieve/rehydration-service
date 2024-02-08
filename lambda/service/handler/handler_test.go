package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pennsieve/rehydration-service/service/handler"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRehydrationServiceHandler(t *testing.T) {
	t.Setenv("TASK_DEF_ARN", "test-ecs-task-definition-arn")
	t.Setenv("SUBNET_IDS", "test-subnet-1, test-subnet-2")
	t.Setenv("CLUSTER_ARN", "test-cluster-arn")
	t.Setenv("SECURITY_GROUP", "test-sg")
	t.Setenv("TASK_DEF_CONTAINER_NAME", "test-rehydrate-fargate-container")
	t.Setenv("ENV", "test")

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

	ctx := context.Background()
	requestContext := events.APIGatewayV2HTTPRequestContext{
		HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{
			Method: "POST",
		},
		Authorizer: &events.APIGatewayV2HTTPRequestContextAuthorizerDescription{
			Lambda: make(map[string]interface{}),
		},
	}
	request := events.APIGatewayV2HTTPRequest{
		RouteKey:       "POST /rehydrate",
		Body:           "{ \"datasetId\": 5069, \"datasetVersionId\": 2}",
		RequestContext: requestContext,
	}

	expectedStatusCode := 202
	response, err := handler.RehydrationServiceHandler(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, expectedStatusCode, response.StatusCode,
		"expected status code %v, got %v", expectedStatusCode, response.StatusCode)
	assert.Contains(t, response.Body, expectedTaskARN)

}
