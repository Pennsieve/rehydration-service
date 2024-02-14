package idempotency

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pennsieve/rehydration-service/service/ecs"
	"github.com/pennsieve/rehydration-service/service/request"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"log/slog"
)

const maxRetries = 2

type Handler struct {
	store      idempotency.Store
	request    *request.RehydrationRequest
	ecsHandler ecs.Handler
}

func NewHandler(awsConfig aws.Config, req *request.RehydrationRequest, ecsHandler ecs.Handler) (*Handler, error) {
	store, err := idempotency.NewStore(awsConfig, req.Logger)
	if err != nil {
		return nil, err
	}
	return &Handler{
		store:      store,
		request:    req,
		ecsHandler: ecsHandler,
	}, nil
}

type Response struct {
	RehydrationLocation string
	TaskARN             string
}

func (h *Handler) Handle(ctx context.Context) (response *Response, err error) {
	datasetID := h.request.Dataset.ID
	datasetVersionID := h.request.Dataset.VersionID
	for retry, i := true, 0; retry; i++ {
		h.request.Logger.Info("idempotency Handler", slog.Int("attempt", i))
		response, err = h.processIdempotency(ctx, datasetID, datasetVersionID)
		var inconsistentStateError InconsistentStateError
		retry = err != nil && errors.As(err, &inconsistentStateError) && i < maxRetries
	}
	return
}

func (h *Handler) processIdempotency(ctx context.Context, datasetID, datasetVersionID int) (*Response, error) {
	// try to create a new idempotency record; error if one exists
	if err := h.store.SaveInProgress(ctx, datasetID, datasetVersionID); err != nil {
		// If a record exists, respond with an existing rehydration location if we can, otherwise an error
		var recordAlreadyExistsError idempotency.RecordAlreadyExistsError
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

func (h *Handler) getIdempotencyRecord(ctx context.Context, datasetID, datasetVersionID int, alreadyExistsError idempotency.RecordAlreadyExistsError) (*idempotency.Record, error) {
	if alreadyExistsError.Existing != nil {
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
		return nil, InProgressError{fmt.Sprintf("rehydration already in progress for %s", record.ID)}
	default:
		return &Response{RehydrationLocation: record.RehydrationLocation}, nil
	}
}

func (h *Handler) startRehydrationTask(ctx context.Context) (*Response, error) {
	taskARN, err := h.ecsHandler.Handle(ctx, h.request.Dataset, h.request.Logger)
	if err != nil {
		return nil, err
	}
	return &Response{TaskARN: taskARN}, nil
}

type InconsistentStateError struct {
	message string
}

func (e InconsistentStateError) Error() string {
	return e.message
}

type InProgressError struct {
	message string
}

func (e InProgressError) Error() string {
	return e.message
}

type ExpiredError struct {
	message string
}

func (e ExpiredError) Error() string {
	return e.message
}
