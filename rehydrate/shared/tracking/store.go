package tracking

import (
	"context"
	"github.com/pennsieve/rehydration-service/shared/models"
	"time"
)

type Store interface {
	PutEntry(ctx context.Context, entry *Entry) error
	NewInProgressEntry(ctx context.Context, id string, dataset models.Dataset, user models.User, lambdaLogStream, awsRequestID, fargateTaskARN string) error
	NewFailedEntry(ctx context.Context, id string, dataset models.Dataset, user models.User, lambdaLogStream, awsRequestID string) error
	NewUnknownEntry(ctx context.Context, id string, dataset models.Dataset, user models.User, lambdaLogStream, awsRequestID string) error
	EmailSent(ctx context.Context, id string, emailSentDate time.Time, status RehydrationStatus) error
	QueryDatasetVersionIndex(ctx context.Context, dataset models.Dataset, limit int32) ([]DatasetVersionIndex, error)
}
