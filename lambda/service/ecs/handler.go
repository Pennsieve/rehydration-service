package ecs

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/pennsieve/rehydration-service/service/models"
	"github.com/pennsieve/rehydration-service/service/runner"
	sharedmodels "github.com/pennsieve/rehydration-service/shared/models"
	"log/slog"
	"strings"
)

type Handler interface {
	Handle(ctx context.Context, dataset sharedmodels.Dataset, user sharedmodels.User, logger *slog.Logger) (string, error)
}
type handler struct {
	taskConfig *models.ECSTaskConfig
	client     *ecs.Client
}

func NewHandler(awsConfig aws.Config, taskConfig *models.ECSTaskConfig) Handler {
	client := ecs.NewFromConfig(awsConfig)
	return &handler{
		taskConfig: taskConfig,
		client:     client,
	}
}

func (h *handler) Handle(ctx context.Context, dataset sharedmodels.Dataset, user sharedmodels.User, logger *slog.Logger) (string, error) {
	logger.Info("Initiating new Rehydrate Fargate Task.")

	runTaskIn := h.taskConfig.RunTaskInput(dataset, user)

	runner := runner.NewECSTaskRunner(h.client, runTaskIn)
	out, err := runner.Run(ctx)
	if err != nil {
		return "", fmt.Errorf("error starting Fargate task: %w", err)
	}
	var taskARN string
	if len(out.Tasks) > 0 {
		taskARN = aws.ToString(out.Tasks[0].TaskArn)
		logger.Info("fargate task started", taskLogGroup(out.Tasks[0]))
		for i := 1; i < len(out.Tasks); i++ {
			logger.Warn("unexpected additional tasks started", taskLogGroup(out.Tasks[i]))
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

// taskLogGroup returns a view of a types.Task as a slog.Group for structured logging
func taskLogGroup(task types.Task) slog.Attr {
	return slog.Group("task",
		slog.String("arn", aws.ToString(task.TaskArn)),
		slog.String("lastStatus", aws.ToString(task.LastStatus)),
	)
}
