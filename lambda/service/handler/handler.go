package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/pennsieve/rehydration-service/service/models"
	"github.com/pennsieve/rehydration-service/service/runner"
	"github.com/pennsieve/rehydration-service/shared/awsconfig"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"log/slog"
	"net/http"
	"strings"
)

var logger = logging.Default
var AWSConfigFactory = awsconfig.NewFactory()

func RehydrationServiceHandler(ctx context.Context, request events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	taskConfig, err := models.TaskConfigFromEnvironment()
	if err != nil {
		logger.Error("error getting ECS task configuration from environment variables", "error", err)
		return errorResponse(500, err, request)
	}
	rehydrationRequest, err := NewRehydrationRequest(ctx, request)
	if err != nil {
		logger.Error("error creating RehydrationRequest", "error", err)
		var badRequest *BadRequestError
		if errors.As(err, &badRequest) {
			return errorResponse(http.StatusBadRequest, err, request)
		}
		return errorResponse(500, err, request)
	}
	out, err := rehydrationRequest.handle(ctx, taskConfig)
	if err != nil {
		rehydrationRequest.logger.Error("error handling RehydrationRequest", "error", err)
		return errorResponse(500, err, request)
	}
	return events.APIGatewayV2HTTPResponse{
		StatusCode: 202,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       fmt.Sprintf(`{"rehydrationARN": %q}`, out),
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

type BadRequestError struct {
	message string
}

func (e *BadRequestError) Error() string {
	return e.message
}

func validateRequest(request models.Request) *BadRequestError {
	if request.Dataset.ID == 0 {
		return &BadRequestError{`missing "datasetId"`}
	}
	if request.Dataset.VersionID == 0 {
		return &BadRequestError{`missing "datasetVersionId"`}
	}
	if len(request.User.Name) == 0 {
		return &BadRequestError{`missing user "name"`}
	}
	if len(request.User.Email) == 0 {
		return &BadRequestError{`missing user "email"`}
	}
	return nil
}

func NewRehydrationRequest(ctx context.Context, lambdaRequest events.APIGatewayV2HTTPRequest) (*RehydrationRequest, error) {
	awsRequestID := lambdaRequest.RequestContext.RequestID

	logging.Default.Info("handling request", slog.String("body", lambdaRequest.Body))
	var request models.Request
	if err := json.Unmarshal([]byte(lambdaRequest.Body), &request); err != nil {
		return nil, &BadRequestError{fmt.Sprintf("error unmarshalling request body [%s]: %v", lambdaRequest.Body, err)}
	}
	if err := validateRequest(request); err != nil {
		return nil, err
	}
	dataset, user := request.Dataset, request.User
	cfg, err := AWSConfigFactory.Get(ctx)
	if err != nil {
		return nil, err
	}

	requestLogger := logging.Default.With(slog.String("requestID", awsRequestID),
		slog.Group("dataset", slog.Int("id", dataset.ID), slog.Int("versionID", dataset.VersionID)),
		slog.Group("user", slog.String("name", user.Name), slog.String("email", user.Email)))

	return &RehydrationRequest{
		dataset:             dataset,
		user:                user,
		awsConfig:           *cfg,
		logger:              requestLogger,
		lambdaRequest:       lambdaRequest,
		lambdaLogStreamName: lambdacontext.LogStreamName,
		awsRequestID:        awsRequestID,
	}, nil
}

func (r *RehydrationRequest) handle(ctx context.Context, taskConfig *models.ECSTaskConfig) (string, error) {
	client := ecs.NewFromConfig(r.awsConfig)
	r.logger.Info("Initiating new Rehydrate Fargate Task.")

	runTaskIn := taskConfig.RunTaskInput(r.dataset)

	runner := runner.NewECSTaskRunner(client, runTaskIn)
	out, err := runner.Run(ctx)
	if err != nil {
		return "", fmt.Errorf("error starting Fargate task: %w", err)
	}
	var taskARN string
	if len(out.Tasks) > 0 {
		taskARN = aws.ToString(out.Tasks[0].TaskArn)
		r.logger.Info("fargate task started", taskLogGroup(out.Tasks[0]))
		for i := 1; i < len(out.Tasks); i++ {
			r.logger.Warn("unexpected additional tasks started", taskLogGroup(out.Tasks[i]))
		}
	}
	if len(out.Failures) == 0 {
		if len(taskARN) > 0 {
			// simple success
			return taskARN, nil
		}
		//this shouldn't occur: no failures, but also no tasks
		return "", fmt.Errorf("unexpected error, ECS runTask returned no tasks and no failures")
	}
	// there must be some failures
	var failMsgs []string
	for _, fail := range out.Failures {
		failMsgs = append(failMsgs, fmt.Sprintf("[arn: %s, reason: %s, detail: %s]",
			aws.ToString(fail.Arn),
			aws.ToString(fail.Reason),
			aws.ToString(fail.Detail)))
	}
	var taskFailure error
	if len(taskARN) == 0 {
		// simple failure
		taskFailure = fmt.Errorf("task failures: %s", strings.Join(failMsgs, ", "))
	} else if len(taskARN) > 0 {
		taskFailure = fmt.Errorf("task %s started, but there were failures: %s", taskARN,
			strings.Join(failMsgs, ", "))
	}
	return taskARN, taskFailure
}

func taskLogGroup(task types.Task) slog.Attr {
	return slog.Group("task",
		slog.String("arn", aws.ToString(task.TaskArn)),
		slog.String("lastStatus", aws.ToString(task.LastStatus)),
	)
}
