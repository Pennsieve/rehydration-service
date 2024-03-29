package idempotency

import "context"

type Store interface {
	SaveInProgress(ctx context.Context, datasetID, datasetVersionID int) error
	GetRecord(ctx context.Context, recordID string) (*Record, error)
	PutRecord(ctx context.Context, record Record) error
	UpdateRecord(ctx context.Context, record Record) error
	SetTaskARN(ctx context.Context, recordID string, taskARN string) error
	DeleteRecord(ctx context.Context, recordID string) error
}
