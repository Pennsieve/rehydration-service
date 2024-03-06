package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/pennsieve/rehydration-service/fargate/config"
	"github.com/pennsieve/rehydration-service/shared/awsconfig"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"log/slog"
	"os"
)

const ThresholdSize = int64(100 * 1024 * 1024)

var awsConfigFactory = awsconfig.NewFactory()

func main() {
	// The os.Exit calls below make this function untestable. Reason for the os.Exit is so
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
	rehydrator, idempotencyStore, err := taskHandlerDependencies(taskConfig)
	if err != nil {
		taskConfig.Logger.Error("error creating task dependencies: %v", err)
		taskConfig.Logger.Warn("task failed prior to creating idempotency store; idempotency record has not been deleted")
		os.Exit(1)
	}

	if err := RehydrationTaskHandler(ctx, rehydrator, idempotencyStore); err != nil {
		taskConfig.Logger.Error("error rehydrating dataset: %v", err)
		os.Exit(1)
	}
}

func RehydrationTaskHandler(ctx context.Context, rehydrator *DatasetRehydrator, idempotencyStore idempotency.Store) error {
	var errs []error
	results, err := rehydrator.rehydrate(ctx)
	if err != nil {
		errs = append(errs, fmt.Errorf("error rehydrating dataset: %w", err))
		if deleteErr := delete(ctx, idempotencyStore, rehydrator.dataset); deleteErr != nil {
			errs = append(errs, fmt.Errorf("error deleting idempotency record: %w", deleteErr))
		}
		//TODO update per-user DynamoDB with error
		return errors.Join(errs...)
	}

	for _, result := range results.FileResults {
		if result.Error != nil {
			errs = append(errs, fmt.Errorf("error rehydrating file %s: %w", result.Rehydration.Src.GetVersionedUri(), result.Error))
		}
	}

	if len(errs) > 0 {
		if deleteErr := delete(ctx, idempotencyStore, rehydrator.dataset); deleteErr != nil {
			errs = append(errs, fmt.Errorf("error deleting idempotency record: %w", deleteErr))
		}
		//TODO update per-user DynamoDB with error
		return errors.Join(errs...)
	}
	if err := success(ctx, idempotencyStore, rehydrator.dataset, results.Location); err != nil {
		// logging this here and not returning it because we don't want to fail the whole rehydration
		// because of this. But maybe we should?
		rehydrator.logger.Error("error updating idempotency record for success", slog.Any("error", err))
	}
	//TODO update per-user DynamoDB with success
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

func taskHandlerDependencies(taskConfig *config.Config) (*DatasetRehydrator, idempotency.Store, error) {
	rehydrator := NewDatasetRehydrator(taskConfig, ThresholdSize)
	idempotencyStore, err := taskConfig.IdempotencyStore()
	if err != nil {
		return nil, nil, fmt.Errorf("error creating idempotencyStore: %w", err)
	}
	return rehydrator, idempotencyStore, nil
}
