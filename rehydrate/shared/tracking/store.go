package tracking

import (
	"context"
	"github.com/pennsieve/rehydration-service/shared/models"
	"time"
)

type Store interface {
	PutEntry(ctx context.Context, entry *Entry) error
	EmailSent(ctx context.Context, id string, emailSentDate *time.Time, status RehydrationStatus) error
	// QueryDatasetVersionIndexUnhandled looks up DatasetVersionIndex entries for the give dataset where no emailSentDate has been set.
	// limit is a page size, but this method does the pagination and returns all matching entries in one call.
	QueryDatasetVersionIndexUnhandled(ctx context.Context, dataset models.Dataset, limit int32) ([]DatasetVersionIndex, error)
}
