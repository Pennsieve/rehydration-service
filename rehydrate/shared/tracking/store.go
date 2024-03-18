package tracking

import (
	"context"
	"github.com/pennsieve/rehydration-service/shared/models"
	"time"
)

type Store interface {
	PutEntry(ctx context.Context, entry *Entry) error
	EmailSent(ctx context.Context, id string, emailSentDate time.Time, status RehydrationStatus) error
	QueryDatasetVersionIndex(ctx context.Context, dataset models.Dataset, limit int32) ([]DatasetVersionIndex, error)
}
