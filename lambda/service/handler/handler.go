package handler

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/pennsieve/rehydration-service/service/ecs"
	"github.com/pennsieve/rehydration-service/service/idempotency"
	"github.com/pennsieve/rehydration-service/service/models"
	"github.com/pennsieve/rehydration-service/service/request"
	"github.com/pennsieve/rehydration-service/shared/awsconfig"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"log/slog"
	"net/http"
)

var logger = logging.Default
var AWSConfigFactory = awsconfig.NewFactory()

func RehydrationServiceHandler(ctx context.Context, lambdaRequest events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	taskConfig, err := models.TaskConfigFromEnvironment()
	if err != nil {
		logger.Error("error getting ECS task configuration from environment variables", "error", err)
		return errorResponse(500, err, lambdaRequest)
	}

	awsConfig, err := AWSConfigFactory.Get(ctx)
	if err != nil {
		logger.Error("error getting AWS config", "error", err)
		return errorResponse(500, err, lambdaRequest)
	}

	ecsHandler := ecs.NewHandler(*awsConfig, taskConfig)

	rehydrationRequest, err := request.NewRehydrationRequest(lambdaRequest)
	if err != nil {
		logger.Error("error creating RehydrationRequest", "error", err)
		var badRequest *request.BadRequestError
		if errors.As(err, &badRequest) {
			return errorResponse(http.StatusBadRequest, err, lambdaRequest)
		}
		return errorResponse(500, err, lambdaRequest)
	}

	idempotencyConfig := idempotency.Config{
		AWSConfig:        *awsConfig,
		IdempotencyTable: taskConfig.IdempotencyTableName,
	}

	handler, err := idempotency.NewHandler(idempotencyConfig, rehydrationRequest, ecsHandler)
	if err != nil {
		rehydrationRequest.Logger.Error("error creating idempotency handler", "error", err)
		return errorResponse(500, err, lambdaRequest)
	}

	out, err := handler.Handle(ctx)
	if err != nil {
		rehydrationRequest.Logger.Error("error handling RehydrationRequest", "error", err)
		return errorResponse(500, err, lambdaRequest)
	}
	respBody, err := out.String()
	if err != nil {
		rehydrationRequest.Logger.Error("unable to marshall successful response", slog.Any("error", err))
		return errorResponse(500, err, lambdaRequest)
	}
	return events.APIGatewayV2HTTPResponse{
		StatusCode: 202,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       respBody,
	}, nil
}

func errorBody(err error, lambdaRequest events.APIGatewayV2HTTPRequest) string {
	return fmt.Sprintf(`{"requestID": %q, "logStream": %q, "message": %q}`, lambdaRequest.RequestContext.RequestID, lambdacontext.LogStreamName, err)
}

func errorResponse(statusCode int, err error, lambdaRequest events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	return events.APIGatewayV2HTTPResponse{
		StatusCode: statusCode,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       errorBody(err, lambdaRequest),
	}, err
}
