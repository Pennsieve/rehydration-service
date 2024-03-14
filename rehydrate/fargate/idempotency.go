package main

import (
	"context"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/models"
)

func success(ctx context.Context, store idempotency.Store, dataset *models.Dataset, rehydrationLocation string) error {
	record := idempotency.Record{
		ID:                  idempotency.RecordID(dataset.ID, dataset.VersionID),
		RehydrationLocation: rehydrationLocation,
		Status:              idempotency.Completed,
	}
	return store.UpdateRecord(ctx, record)
}

func delete(ctx context.Context, store idempotency.Store, dataset *models.Dataset) error {
	return store.DeleteRecord(ctx, idempotency.RecordID(dataset.ID, dataset.VersionID))
}
