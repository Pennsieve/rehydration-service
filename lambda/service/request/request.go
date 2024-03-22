package request

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/google/uuid"
	"github.com/pennsieve/rehydration-service/service/models"
	"github.com/pennsieve/rehydration-service/shared/logging"
	sharedmodels "github.com/pennsieve/rehydration-service/shared/models"
	"github.com/pennsieve/rehydration-service/shared/notification"
	"github.com/pennsieve/rehydration-service/shared/tracking"
	"log/slog"
	"time"
)

type RehydrationRequest struct {
	Dataset             sharedmodels.Dataset
	User                sharedmodels.User
	Logger              *slog.Logger
	lambdaRequest       events.APIGatewayV2HTTPRequest
	lambdaLogStreamName string
	awsRequestID        string
	requestID           string
	trackingEntry       *tracking.Entry
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
		return &BadRequestError{`missing User "name"`}
	}
	if len(request.User.Email) == 0 {
		return &BadRequestError{`missing User "email"`}
	}
	return nil
}

func NewRehydrationRequest(lambdaRequest events.APIGatewayV2HTTPRequest) (*RehydrationRequest, error) {
	requestID := uuid.NewString()
	awsRequestID := lambdaRequest.RequestContext.RequestID
	lambdaLogStreamName := lambdacontext.LogStreamName

	logging.Default.Info("handling request", slog.String("body", lambdaRequest.Body))
	var request models.Request
	if err := json.Unmarshal([]byte(lambdaRequest.Body), &request); err != nil {
		return nil, &BadRequestError{fmt.Sprintf("error unmarshalling request body [%s]: %v", lambdaRequest.Body, err)}
	}
	if err := validateRequest(request); err != nil {
		return nil, err
	}
	dataset, user := request.Dataset, request.User

	requestLogger := logging.Default.With(slog.String("awsRequestID", awsRequestID),
		slog.String("requestID", requestID),
		slog.Group("dataset", slog.Int("id", dataset.ID), slog.Int("versionId", dataset.VersionID)),
		slog.Group("user", slog.String("name", user.Name), slog.String("email", user.Email)))

	trackingEntry := &tracking.Entry{
		DatasetVersionIndex: tracking.DatasetVersionIndex{
			ID:             requestID,
			DatasetVersion: dataset.DatasetVersion(),
			UserName:       user.Name,
			UserEmail:      user.Email,
		},
		LambdaLogStream: lambdaLogStreamName,
		AWSRequestID:    awsRequestID,
		RequestDate:     time.Now(),
	}

	return &RehydrationRequest{
		Dataset:             dataset,
		User:                user,
		Logger:              requestLogger,
		lambdaRequest:       lambdaRequest,
		lambdaLogStreamName: lambdaLogStreamName,
		awsRequestID:        awsRequestID,
		requestID:           requestID,
		trackingEntry:       trackingEntry,
	}, nil
}

func (r *RehydrationRequest) WriteNewUnknownRequest(ctx context.Context, trackingStore tracking.Store) {
	r.writeTrackingEntryWithStatus(ctx, trackingStore, tracking.Unknown)
}

func (r *RehydrationRequest) WriteNewInProgressRequest(ctx context.Context, trackingStore tracking.Store, fargateTaskARN string) {
	r.trackingEntry.FargateTaskARN = fargateTaskARN
	r.writeTrackingEntryWithStatus(ctx, trackingStore, tracking.InProgress)
}

func (r *RehydrationRequest) WriteNewCompletedRequest(ctx context.Context, trackingStore tracking.Store, fargateTaskARN string, emailSentDate *time.Time) {
	r.trackingEntry.FargateTaskARN = fargateTaskARN
	r.trackingEntry.EmailSentDate = emailSentDate
	r.writeTrackingEntryWithStatus(ctx, trackingStore, tracking.Completed)
}

func (r *RehydrationRequest) WriteNewExpiredRequest(ctx context.Context, trackingStore tracking.Store) {
	r.writeTrackingEntryWithStatus(ctx, trackingStore, tracking.Expired)
}

func (r *RehydrationRequest) writeTrackingEntryWithStatus(ctx context.Context, trackingStore tracking.Store, status tracking.RehydrationStatus) {
	r.trackingEntry.RehydrationStatus = status
	if err := trackingStore.PutEntry(ctx, r.trackingEntry); err != nil {
		// don't want to fail request if we can't write to tracking table
		r.Logger.Warn("error writing rehydration status to tracking table",
			slog.Any("rehydrationStatus", status),
			slog.Any("error", err))
	}
}

func (r *RehydrationRequest) SendCompletedEmail(ctx context.Context, emailer notification.Emailer, rehydrationLocation string) *time.Time {
	if err := emailer.SendRehydrationComplete(ctx, r.Dataset, r.User, rehydrationLocation); err != nil {
		// don't want to fail request if we can't email user
		r.Logger.Warn("error sending rehydration complete email",
			slog.Any("rehydrationLocation", rehydrationLocation),
			slog.Any("error", err))
		return nil
	}
	emailSent := time.Now()
	return &emailSent
}
