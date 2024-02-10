package idempotency

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"log/slog"
)

const maxRetries = 2

type Handler struct {
	store  *Store
	logger *slog.Logger
}

func NewHandler(awsConfig aws.Config, logger *slog.Logger) (*Handler, error) {
	store, err := NewStore(awsConfig, logger)
	if err != nil {
		return nil, err
	}
	return &Handler{
		store:  store,
		logger: logger,
	}, nil
}

type Response struct {
	rehydrationLocation string
}

func (h *Handler) Handle(ctx context.Context, datasetID, datasetVersionID int) (response *Response, err error) {
	for retry, i := true, 0; retry; i++ {
		response, err = h.processIdempotency(ctx, datasetID, datasetVersionID)
		if err != nil {
			var inconsistentStateError InconsistentStateError
			retry = errors.As(err, &inconsistentStateError) && i < maxRetries
		}
	}
	return
}

func (h *Handler) processIdempotency(ctx context.Context, datasetID, datasetVersionID int) (*Response, error) {
	// try to create a new idempotency record; error if one exists
	if err := h.store.SaveInProgress(ctx, datasetID, datasetVersionID); err != nil {
		// If a record exists, respond with an existing rehydration location if we can, otherwise an error
		var recordAlreadyExistsError RecordAlreadyExistsError
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
	return h.startRehydrationTask(ctx, datasetID, datasetVersionID)

}

func (h *Handler) getIdempotencyRecord(ctx context.Context, datasetID, datasetVersionID int, alreadyExistsError RecordAlreadyExistsError) (*Record, error) {
	if alreadyExistsError.Existing != nil {
		return alreadyExistsError.Existing, nil
	}
	recordID := recordID(datasetID, datasetVersionID)
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

func (h *Handler) handleForStatus(record *Record) (*Response, error) {
	switch record.Status {
	case Expired:
		return nil, InconsistentStateError{"saveInProgress and getRecord return inconsistent results"}
	case InProgress:
		return nil, InProgressError{fmt.Sprintf("rehydration already in progress for %s", record.ID)}
	default:
		return &Response{rehydrationLocation: record.RehydrationLocation}, nil
	}
}

func (h *Handler) startRehydrationTask(ctx context.Context, datasetID, datasetVersionID int) (*Response, error) {
	// TODO
	return nil, fmt.Errorf("not yet implemented")
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
