package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/rehydration-service/service/models"
	"github.com/pennsieve/rehydration-service/service/runner"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"log/slog"
)

var logger = logging.Default

func RehydrationServiceHandler(ctx context.Context, request events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	taskConfig, err := models.TaskConfigFromEnvironment()
	if err != nil {
		logger.Error("error getting ECS task configuration from environment variables", "error", err)
		return errorResponse(500, err, request)
	}
	rehydrationRequest, err := NewRehydrationRequest(ctx, request)
	if err != nil {
		logger.Error("error creating RehydrationRequest", "error", err)
		return errorResponse(500, err, request)
	}
	err = rehydrationRequest.handle(ctx, taskConfig)
	if err != nil {
		logger.Error("error handling RehydrationRequest", "error", err)
		return errorResponse(500, err, request)
	}
	handlerName := "RehydrationServiceHandler"
	return events.APIGatewayV2HTTPResponse{
		StatusCode: 202,
		Body:       fmt.Sprintf("%s: Fargate task accepted", handlerName),
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

type RehydrationRequest struct {
	dataset             models.Dataset
	user                models.User
	awsConfig           aws.Config
	logger              *slog.Logger
	lambdaRequest       events.APIGatewayV2HTTPRequest
	lambdaLogStreamName string
	awsRequestID        string
}

func NewRehydrationRequest(ctx context.Context, lambdaRequest events.APIGatewayV2HTTPRequest) (*RehydrationRequest, error) {
	awsRequestID := lambdaRequest.RequestContext.RequestID

	userClaim := authorizer.ParseClaims(lambdaRequest.RequestContext.Authorizer.Lambda).UserClaim
	user := models.User{ID: userClaim.Id, NodeID: userClaim.NodeId}

	var dataset models.Dataset
	if err := json.Unmarshal([]byte(lambdaRequest.Body), &dataset); err != nil {
		return nil, fmt.Errorf("error unmarshalling request body [%s]: %w", lambdaRequest.Body, err)
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("error loading default AWS config: %w", err)
	}

	requestLogger := logging.Default.With(slog.String("requestID", awsRequestID),
		slog.Group("dataset", slog.Int("id", dataset.ID)), slog.Int("versionID", dataset.VersionID),
		slog.Group("user", slog.Int64("id", user.ID), slog.String("nodeID", user.NodeID)))

	return &RehydrationRequest{
		dataset:             dataset,
		user:                user,
		awsConfig:           cfg,
		logger:              requestLogger,
		lambdaRequest:       lambdaRequest,
		lambdaLogStreamName: lambdacontext.LogStreamName,
		awsRequestID:        awsRequestID,
	}, nil
}

func (r *RehydrationRequest) handle(ctx context.Context, taskConfig *models.ECSTaskConfig) error {
	client := ecs.NewFromConfig(r.awsConfig)
	r.logger.Info("Initiating new Rehydrate Fargate Task.")

	runTaskIn := taskConfig.RunTaskInput(r.dataset)

	runner := runner.NewECSTaskRunner(client, runTaskIn)
	if err := runner.Run(ctx); err != nil {
		return fmt.Errorf("error starting Fargate task: %w", err)
	}

	return nil
}
