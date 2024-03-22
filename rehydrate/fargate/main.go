package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/pennsieve/rehydration-service/fargate/config"
	"github.com/pennsieve/rehydration-service/shared/awsconfig"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"github.com/pennsieve/rehydration-service/shared/notification"
	"github.com/pennsieve/rehydration-service/shared/tracking"
	"log/slog"
	"os"
)

const ThresholdSize = int64(100 * 1024 * 1024)

var awsConfigFactory = awsconfig.NewFactory()

func main() {
	// The os.Exit call below make this function untestable. Reason for the os.Exit is so
	// that the task shows up as failed in the hope that this will surface errors quickly. In the
	// AWS console, datadog, notifications, etc.
	//
	// All the logic is in RehydrationTaskHandler. Everything proceeding that should just be setup.
	ctx := context.Background()
	taskConfig, err := initConfig(ctx)
	if err != nil {
		logging.Default.Error("error initializing config", err)
		logging.Default.Warn("task failed prior to creating idempotency store; idempotency record has not been deleted")
		os.Exit(1)
	}
	taskHandler, err := NewTaskHandler(taskConfig, ThresholdSize)
	if err != nil {
		logging.Default.Error("error creating TaskHandler", err)
		logging.Default.Warn("task failed prior to creating idempotency store; idempotency record has not been deleted")
		os.Exit(1)
	}

	taskConfig.Logger.Info("starting rehydration task")
	if err := RehydrationTaskHandler(ctx, taskHandler); err != nil {
		taskConfig.Logger.Error("error rehydrating dataset: %v", err)
		os.Exit(1)
	}
	taskConfig.Logger.Info("rehydration complete")
}

func RehydrationTaskHandler(ctx context.Context, taskHandler *TaskHandler) error {
	rehydrator := taskHandler.DatasetRehydrator

	results, err := rehydrator.rehydrate(ctx)
	if err != nil {
		es := taskHandler.failed(ctx)
		es = append(es, fmt.Errorf("error rehydrating dataset: %w", err))
		return errors.Join(es...)
	}

	var errs []error
	for _, result := range results.FileResults {
		if result.Error != nil {
			errs = append(errs, fmt.Errorf("error rehydrating file %s: %w", result.Rehydration.Src.GetVersionedUri(), result.Error))
		}
	}

	if len(errs) > 0 {
		// there are real rehydration failures. So no harm in adding any idempotency/tracking/notification errors
		errs = append(errs, taskHandler.failed(ctx)...)
		return errors.Join(errs...)
	}
	for _, notificationError := range taskHandler.completed(ctx, results.Location) {
		// there are no real rehydration failures. So we just log idempotency/tracking/notification errors if there are any
		taskHandler.DatasetRehydrator.logger.Error(
			"rehydration succeeded but there were non-fatal errors",
			slog.Any("error", notificationError))
	}
	return nil
}

func initConfig(ctx context.Context) (*config.Config, error) {
	awsConfig, err := awsConfigFactory.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting AWS config: %w", err)
	}
	configEnv, err := config.LookupEnv()
	if err != nil {
		return nil, fmt.Errorf("error getting taskConfig environment variables: %w", err)
	}
	taskConfig := config.NewConfig(*awsConfig, configEnv)
	return taskConfig, nil
}

type TaskHandler struct {
	DatasetRehydrator *DatasetRehydrator
	IdempotencyStore  idempotency.Store
	TrackingStore     tracking.Store
	Emailer           notification.Emailer
	Result            *TaskResult
}

func NewTaskHandler(taskConfig *config.Config, multipartCopyThresholdBytes int64) (*TaskHandler, error) {
	emailer, err := taskConfig.Emailer()
	if err != nil {
		return nil, err
	}
	return &TaskHandler{
		DatasetRehydrator: NewDatasetRehydrator(taskConfig, multipartCopyThresholdBytes),
		IdempotencyStore:  taskConfig.IdempotencyStore(),
		TrackingStore:     taskConfig.TrackingStore(),
		Emailer:           emailer,
	}, nil
}

// failed handles idempotency/notification/tracking for FAILED rehydrations
func (h *TaskHandler) failed(ctx context.Context) []error {
	h.Result = NewFailedResult()
	return h.finalize(ctx)
}

// completed handles idempotency/notification/tracking for COMPLETED rehydrations
func (h *TaskHandler) completed(ctx context.Context, rehydrationLocation string) []error {
	h.Result = NewCompletedResult(rehydrationLocation)
	return h.finalize(ctx)
}

func (h *TaskHandler) finalize(ctx context.Context) []error {
	var errs []error
	if err := h.finalizeIdempotency(ctx); err != nil {
		errs = append(errs, fmt.Errorf("error finalizing idempotency record: %w", err))
	}
	if queryResults, err := h.TrackingStore.QueryDatasetVersionIndexUnhandled(ctx, *h.DatasetRehydrator.dataset, 20); err != nil {
		errs = append(errs, err)
	} else {
		errs = append(errs, h.emailAndLog(ctx, queryResults)...)
	}
	return errs
}

type TaskResult struct {
	RehydrationLocation string
}

func NewFailedResult() *TaskResult {
	return &TaskResult{}
}

func NewCompletedResult(rehydrationLocation string) *TaskResult {
	return &TaskResult{RehydrationLocation: rehydrationLocation}
}

func (r *TaskResult) Failed() bool {
	return len(r.RehydrationLocation) == 0
}

func (r *TaskResult) RehydrationStatus() tracking.RehydrationStatus {
	if r.Failed() {
		return tracking.Failed
	}
	return tracking.Completed
}
