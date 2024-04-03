package main

import (
	"context"
	"fmt"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"log/slog"
)

func (h *TaskHandler) finalizeIdempotency(ctx context.Context) error {
	if h.Result == nil {
		return fmt.Errorf("illegal state: TaskResult has not been set")
	}
	recordID := idempotency.RecordID(h.DatasetRehydrator.dataset.ID, h.DatasetRehydrator.dataset.VersionID)
	if h.Result.Failed() {
		return h.finalizeFailedIdempotency(ctx, recordID)
	}
	record := idempotency.Record{
		ID:                  recordID,
		RehydrationLocation: h.Result.RehydrationLocation,
		Status:              idempotency.Completed,
	}
	return h.IdempotencyStore.UpdateRecord(ctx, record)
}

// finalizeFailedIdempotency does the following to finalize the idempotency state of a failed rehydration
//
// * Sets the idempotency record's status to EXPIRED so that any incoming rehydration requests for this dataset version
// fail while we clean up.
// * Cleans the rehydration location in the S3 bucket by deleting any objects found there.
// * Finally, deletes the idempotency record so that new rehydration requests for the dataset version can be handled in
// the future. The idempotency record is not deleted if the clean is incomplete because of errors.
func (h *TaskHandler) finalizeFailedIdempotency(ctx context.Context, recordID string) error {
	if err := h.IdempotencyStore.ExpireRecord(ctx, recordID); err != nil {
		return err
	}
	cleanResp, err := h.Cleaner.Clean(ctx, recordID)
	if err != nil {
		return err
	}
	h.DatasetRehydrator.logger.Info("cleaned rehydration location",
		slog.Group("rehydrationLocation", slog.String("bucket", cleanResp.Bucket), slog.String("prefix", recordID)),
		slog.Int("fileCount", cleanResp.Count),
		slog.Int("deletedCount", cleanResp.Deleted))
	if len(cleanResp.Errors) == 0 {
		return h.IdempotencyStore.DeleteRecord(ctx, recordID)
	}
	for _, e := range cleanResp.Errors {
		h.DatasetRehydrator.logger.Error("error deleting object",
			slog.Group("object", slog.String("bucket", cleanResp.Bucket), slog.String("key", e.Key)),
			slog.String("error", e.Message))
	}
	return nil
}
