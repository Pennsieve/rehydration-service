package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pennsieve/rehydration-service/service/ecs"
	"github.com/pennsieve/rehydration-service/service/request"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"log/slog"
)

const maxRetries = 2

type Config struct {
	Client           *dynamodb.Client
	IdempotencyTable string
}
type Handler struct {
	store      idempotency.Store
	request    *request.RehydrationRequest
	ecsHandler ecs.Handler
}

func NewHandler(config Config, req *request.RehydrationRequest, ecsHandler ecs.Handler) *Handler {
	store := idempotency.NewStore(config.Client, req.Logger, config.IdempotencyTable)
	return &Handler{
		store:      store,
		request:    req,
		ecsHandler: ecsHandler,
	}
}

type Response struct {
	RehydrationLocation string `json:"rehydrationLocation"`
	TaskARN             string `json:"taskARN"`
}

func (r *Response) String() (string, error) {
	bytes, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("error marshalling Response: %w", err)
	}
	return string(bytes), nil
}

func (h *Handler) Handle(ctx context.Context) (response *Response, err error) {
	datasetID := h.request.Dataset.ID
	datasetVersionID := h.request.Dataset.VersionID
	for retry, i := true, 0; retry; i++ {
		h.request.Logger.Info("idempotency Handler", slog.Int("attempt", i))
		response, err = h.processIdempotency(ctx, datasetID, datasetVersionID)
		retry = err != nil && isRetryableError(err) && i < maxRetries
		if retry {
			h.request.Logger.Info("retrying on retryable error", slog.Any("retryable", err))
		}
	}
	return
}

func isRetryableError(err error) bool {
	var inconsistentStateError InconsistentStateError
	var expiredError ExpiredError
	switch {
	case errors.As(err, &inconsistentStateError), errors.As(err, &expiredError):
		return true
	default:
		return false
	}
}

func (h *Handler) processIdempotency(ctx context.Context, datasetID, datasetVersionID int) (*Response, error) {
	// try to create a new idempotency record; error if one exists
	if err := h.store.SaveInProgress(ctx, datasetID, datasetVersionID); err != nil {
		// If a record exists, respond with an existing rehydration location if we can, otherwise an error
		var recordAlreadyExistsError *idempotency.RecordAlreadyExistsError
		if errors.As(err, &recordAlreadyExistsError) {
			record, err := h.getIdempotencyRecord(ctx, datasetID, datasetVersionID, recordAlreadyExistsError)
			if err != nil {
				return nil, err
			}
			return h.handleForStatus(record)
		}
		// no record exists; we got some other error
		return nil, err
	}
	// we were able to create a new record, so start the rehydration task and respond with taskARN
	return h.startRehydrationTask(ctx)

}

func (h *Handler) getIdempotencyRecord(ctx context.Context, datasetID, datasetVersionID int, alreadyExistsError *idempotency.RecordAlreadyExistsError) (*idempotency.Record, error) {
	if alreadyExistsError != nil && alreadyExistsError.Existing != nil {
		return alreadyExistsError.Existing, nil
	}
	recordID := idempotency.RecordID(datasetID, datasetVersionID)
	record, err := h.store.GetRecord(ctx, recordID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		// This code path will only be triggered if the record is removed between saveInProgress and getRecord
		return nil, InconsistentStateError{"saveInProgress and getRecord return inconsistent results"}
	}
	return record, nil
}

func (h *Handler) handleForStatus(record *idempotency.Record) (*Response, error) {
	switch record.Status {
	case idempotency.Expired:
		return nil, ExpiredError{fmt.Sprintf("rehydration expiration in progress for %s", record.ID)}
	case idempotency.InProgress:
		// Treat this as normal and not an error. Tracking entry will be written and user notified when rehydration complete
		return &Response{TaskARN: record.FargateTaskARN}, nil
	default:
		return &Response{
			RehydrationLocation: record.RehydrationLocation,
			TaskARN:             record.FargateTaskARN}, nil
	}
}

func (h *Handler) startRehydrationTask(ctx context.Context) (*Response, error) {
	recordID := idempotency.RecordID(h.request.Dataset.ID, h.request.Dataset.VersionID)
	taskARN, err := h.ecsHandler.Handle(ctx, h.request.Dataset, h.request.User, h.request.Logger)
	if err != nil {
		deleteErr := h.store.DeleteRecord(ctx, recordID)
		if deleteErr != nil {
			return nil, fmt.Errorf("error starting rehydration task: %w, in addition, there was an error when deleting the idempotency record: %w", err, deleteErr)
		}
		return nil, err
	}
	if err := h.store.SetTaskARN(ctx, recordID, taskARN); err != nil {
		// seems wrong to fail the request because of this, but I'm not sure
		h.request.Logger.Error("error setting taskARN of rehydration", slog.String("taskARN", taskARN), slog.Any("error", err))
	}
	return &Response{TaskARN: taskARN}, nil
}

type InconsistentStateError struct {
	message string
}

func (e InconsistentStateError) Error() string {
	return e.message
}

type ExpiredError struct {
	message string
}

func (e ExpiredError) Error() string {
	return e.message
}
