package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/pennsieve/rehydration-service/fargate/config"
	"github.com/pennsieve/rehydration-service/shared/awsconfig"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"os"
)

const ThresholdSize = int64(100 * 1024 * 1024)

var awsConfigFactory = awsconfig.NewFactory()

func main() {
	ctx := context.Background()
	awsConfig, err := awsConfigFactory.Get(ctx)
	if err != nil {
		logging.Default.Error("error getting AWS config: %v", err)
		logging.Default.Warn("task failed prior to creating idempotency store; idempotency record has not been deleted")
		os.Exit(1)
	}
	configEnv, err := config.LookupEnv()
	if err != nil {
		logging.Default.Error("error getting taskConfig environment variables: %v", err)
		logging.Default.Warn("task failed prior to creating idempotency store; idempotency record has not been deleted")
		os.Exit(1)
	}
	taskConfig := config.NewConfig(*awsConfig, configEnv)

	rehydrator := NewDatasetRehydrator(taskConfig, ThresholdSize)
	idempotencyStore, err := taskConfig.IdempotencyStore()
	if err != nil {
		rehydrator.logger.Error("error creating idempotencyStore: %v", err)
		rehydrator.logger.Warn("task failed prior to creating idempotency store; idempotency record has not been deleted")
		os.Exit(1)
	}

	if err := RehydrationTaskHandler(ctx, rehydrator, idempotencyStore); err != nil {
		rehydrator.logger.Error("error rehydrating dataset: %v", err)
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
		rehydrator.logger.Error("error updating idempotency record for success: %v", err)
	}
	//TODO update per-user DynamoDB with success
	return nil
}
