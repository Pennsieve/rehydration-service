package idempotency

import (
	"context"
	"time"
)

type Store interface {
	SaveInProgress(ctx context.Context, datasetID, datasetVersionID int) error
	GetRecord(ctx context.Context, recordID string) (*Record, error)
	PutRecord(ctx context.Context, record Record) error
	UpdateRecord(ctx context.Context, record Record) error
	SetTaskARN(ctx context.Context, recordID string, taskARN string) error
	DeleteRecord(ctx context.Context, recordID string) error
	ExpireRecord(ctx context.Context, recordID string) error
	SetExpirationDate(ctx context.Context, recordID string, expirationDate time.Time) error
}
