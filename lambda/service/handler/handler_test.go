package handler_test

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/rehydration-service/service/handler"
)

func TestRehydrationServiceHandler(t *testing.T) {
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

	expectedStatusCode := 500
	response, _ := handler.RehydrationServiceHandler(ctx, request)
	if response.StatusCode != expectedStatusCode {
		t.Errorf("expected status code %v, got %v", expectedStatusCode, response.StatusCode)
	}
}
