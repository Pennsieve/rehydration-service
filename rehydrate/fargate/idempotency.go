package main

import (
	"context"
	"fmt"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
)

func (h *TaskHandler) finalizeIdempotency(ctx context.Context) error {
	if h.Result == nil {
		return fmt.Errorf("illegal state: TaskResult has not been set")
	}
	recordID := idempotency.RecordID(h.DatasetRehydrator.dataset.ID, h.DatasetRehydrator.dataset.VersionID)
	if h.Result.Failed() {
		return h.IdempotencyStore.DeleteRecord(ctx, recordID)
	}
	record := idempotency.Record{
		ID:                  recordID,
		RehydrationLocation: h.Result.RehydrationLocation,
		Status:              idempotency.Completed,
	}
	return h.IdempotencyStore.UpdateRecord(ctx, record)
}
