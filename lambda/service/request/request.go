package request

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/pennsieve/rehydration-service/service/models"
	"github.com/pennsieve/rehydration-service/shared/logging"
	sharedmodels "github.com/pennsieve/rehydration-service/shared/models"
	"log/slog"
)

type RehydrationRequest struct {
	Dataset             sharedmodels.Dataset
	User                sharedmodels.User
	Logger              *slog.Logger
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
		return &BadRequestError{`missing User "name"`}
	}
	if len(request.User.Email) == 0 {
		return &BadRequestError{`missing User "email"`}
	}
	return nil
}

func NewRehydrationRequest(lambdaRequest events.APIGatewayV2HTTPRequest) (*RehydrationRequest, error) {
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

	requestLogger := logging.Default.With(slog.String("requestID", awsRequestID),
		slog.Group("dataset", slog.Int("id", dataset.ID), slog.Int("versionId", dataset.VersionID)),
		slog.Group("user", slog.String("name", user.Name), slog.String("email", user.Email)))

	return &RehydrationRequest{
		Dataset:             dataset,
		User:                user,
		Logger:              requestLogger,
		lambdaRequest:       lambdaRequest,
		lambdaLogStreamName: lambdacontext.LogStreamName,
		awsRequestID:        awsRequestID,
	}, nil
}
