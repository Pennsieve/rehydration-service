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
	"github.com/pennsieve/rehydration-service/shared/notification"
	"github.com/pennsieve/rehydration-service/shared/tracking"
	"log/slog"
	"net/http"
)

var logger = logging.Default
var AWSConfigFactory = awsconfig.NewFactory()

func RehydrationServiceHandler(ctx context.Context, lambdaRequest events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	taskConfig, err := models.TaskConfigFromEnvironment()
	if err != nil {
		logger.Error("error getting ECS task configuration from environment variables", "error", err)
		return errorResponse(http.StatusInternalServerError, err, lambdaRequest)
	}

	awsConfig, err := AWSConfigFactory.Get(ctx)
	if err != nil {
		logger.Error("error getting AWS config", "error", err)
		return errorResponse(http.StatusInternalServerError, err, lambdaRequest)
	}

	ecsHandler := ecs.NewHandler(*awsConfig, taskConfig)

	rehydrationRequest, err := request.NewRehydrationRequest(lambdaRequest)
	if err != nil {
		logger.Error("error creating RehydrationRequest", "error", err)
		var badRequest *request.BadRequestError
		if errors.As(err, &badRequest) {
			return errorResponse(http.StatusBadRequest, err, lambdaRequest)
		}
		return errorResponse(http.StatusInternalServerError, err, lambdaRequest)
	}

	trackingStore := tracking.NewStore(*awsConfig, rehydrationRequest.Logger, taskConfig.TrackingTableName)

	emailer, err := notification.NewEmailer(*awsConfig, taskConfig.PennsieveDomain, taskConfig.AWSRegion)
	if err != nil {
		rehydrationRequest.Logger.Error("error creating emailer", "error", err)
		rehydrationRequest.WriteNewUnknownRequest(ctx, trackingStore)
		return errorResponse(http.StatusInternalServerError, err, lambdaRequest)
	}

	idempotencyConfig := idempotency.Config{
		AWSConfig:        *awsConfig,
		IdempotencyTable: taskConfig.IdempotencyTableName,
	}

	handler := idempotency.NewHandler(idempotencyConfig, rehydrationRequest, ecsHandler)

	out, err := handler.Handle(ctx)
	if err != nil {
		rehydrationRequest.Logger.Error("error handling RehydrationRequest", "error", err)
		var expiredError idempotency.ExpiredError
		if errors.As(err, &expiredError) {
			rehydrationRequest.WriteNewExpiredRequest(ctx, trackingStore)
		} else {
			// Maybe we should be writing failed to the tracking table in this case?
			// But now we are only using the "failed" state for a task that started okay, but failed along the way.
			// Here all we know is that the task failed to start, not that the task itself failed.
			// Maybe we should add a new status for this.
			rehydrationRequest.WriteNewUnknownRequest(ctx, trackingStore)
		}
		return errorResponse(500, err, lambdaRequest)
	}

	completionLogAttrs := []any{slog.String("fargateTaskARN", out.TaskARN)}
	if len(out.RehydrationLocation) != 0 {
		// this will only be true if this request is for an already completed, non-expired rehydration
		emailSentDate := rehydrationRequest.SendCompletedEmail(ctx, emailer, out.RehydrationLocation)
		rehydrationRequest.WriteNewCompletedRequest(ctx, trackingStore, out.TaskARN, emailSentDate)
		completionLogAttrs = append(completionLogAttrs, slog.String("rehydrationLocation", out.RehydrationLocation))
	} else {
		rehydrationRequest.WriteNewInProgressRequest(ctx, trackingStore, out.TaskARN)
	}
	rehydrationRequest.Logger.Info("request complete", completionLogAttrs...)

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
	}, nil
}
