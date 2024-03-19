package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/pennsieve/rehydration-service/fargate/config"
	"github.com/pennsieve/rehydration-service/shared/awsconfig"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/logging"
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
	taskHandler := NewTaskHandler(taskConfig, ThresholdSize)

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
		es := taskHandler.failure(ctx)
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
		errs = append(errs, taskHandler.failure(ctx)...)
		return errors.Join(errs...)
	}
	for _, notificationError := range taskHandler.success(ctx, results.Location) {
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
}

func NewTaskHandler(taskConfig *config.Config, multipartCopyThresholdBytes int64) *TaskHandler {
	return &TaskHandler{
		DatasetRehydrator: NewDatasetRehydrator(taskConfig, multipartCopyThresholdBytes),
		IdempotencyStore:  taskConfig.IdempotencyStore(),
		TrackingStore:     taskConfig.TrackingStore(),
	}
}

func (h *TaskHandler) failure(ctx context.Context) []error {
	var errs []error
	if deleteErr := delete(ctx, h.IdempotencyStore, h.DatasetRehydrator.dataset); deleteErr != nil {
		errs = append(errs, fmt.Errorf("error deleting idempotency record: %w", deleteErr))
	}
	if queryResults, err := h.TrackingStore.QueryDatasetVersionIndexUnhandled(ctx, *h.DatasetRehydrator.dataset, 20); err != nil {
		errs = append(errs, err)
	} else {
		// TODO rewrite this to both send emails and update tracking (if we're sending emails for failures)
		errs = append(errs, trackingWriteUpdates(ctx, h.TrackingStore, tracking.Failed, queryResults)...)
	}
	return errs
}

func (h *TaskHandler) success(ctx context.Context, rehydrationLocation string) []error {
	var errs []error
	if err := success(ctx, h.IdempotencyStore, h.DatasetRehydrator.dataset, rehydrationLocation); err != nil {
		h.DatasetRehydrator.logger.Error("error updating idempotency record for success", slog.Any("error", err))
	}
	if queryResults, err := h.TrackingStore.QueryDatasetVersionIndexUnhandled(ctx, *h.DatasetRehydrator.dataset, 20); err != nil {
		errs = append(errs, err)
	} else {
		// TODO rewrite this to both send emails and update tracking
		errs = append(errs, trackingWriteUpdates(ctx, h.TrackingStore, tracking.Completed, queryResults)...)
	}
	return errs
}
