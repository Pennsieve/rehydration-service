package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/rehydration-service/shared"
	"github.com/pennsieve/rehydration-service/shared/awsconfig"
	"github.com/pennsieve/rehydration-service/shared/expiration"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/lambdautils"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"github.com/pennsieve/rehydration-service/shared/s3cleaner"
	"log/slog"
	"net/http"
)

// awsConfigFactory so that one could set the AWS config in a test using MinIO and dynamodb-local before calling ExpirationHandler.
var awsConfigFactory = awsconfig.NewFactory()
var logger = logging.Default

// handler is the expiration.Handler that contains all the logic of checking for expired records, deleting the expired
// rehydration from S3, and finally deleting the idempotency record, so that future rehydration requests can be processed.
//
// Tests of the ExpirationHandler can set this value before calling the function if they require it to use mocks for one
// expiration.Handler's dependencies.
var handler *expiration.Handler

func ExpirationHandler(ctx context.Context, lambdaRequest events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	if err := initializeHandler(ctx); err != nil {
		logger.Error("error initializing expiration handler", slog.Any("error", err))
		return lambdautils.ErrorResponse(http.StatusInternalServerError, err, lambdaRequest)
	}

	if err := handler.Handle(ctx); err != nil {
		logger.Error("error running expiration", slog.Any("error", err))
		return lambdautils.ErrorResponse(http.StatusInternalServerError, err, lambdaRequest)
	}

	return events.APIGatewayV2HTTPResponse{StatusCode: http.StatusNoContent}, nil
}

// initializeHandler if the package var handler is nil, creates a new expiration.Handler and sets
// handler to that value.
//
// If handler is not nil, immediately returns. Allows tests to set handler created with mocks.
func initializeHandler(ctx context.Context) error {
	if handler != nil {
		return nil
	}
	awsConfig, err := awsConfigFactory.Get(ctx)
	if err != nil {
		return fmt.Errorf("error getting AWS config: %w", err)
	}
	idempotencyTable, err := shared.NonEmptyFromEnvVar(idempotency.TableNameKey)
	if err != nil {
		return err
	}
	idempotencyStore := idempotency.NewStore(dynamodb.NewFromConfig(*awsConfig), logger, idempotencyTable)
	s3Cleaner, err := s3cleaner.NewCleaner(s3.NewFromConfig(*awsConfig), s3cleaner.MaxCleanBatch)
	if err != nil {
		return fmt.Errorf("error creating S3 cleaner: %w", err)
	}

	handler = expiration.NewHandler(idempotencyStore, s3Cleaner, logger)
	return nil
}
