package notification

import (
	"context"
	"github.com/pennsieve/rehydration-service/shared/models"
)

type Emailer interface {
	SendRehydrationComplete(ctx context.Context, dataset models.Dataset, user models.User, rehydrationLocation string) error
	SendRehydrationFailed(ctx context.Context, dataset models.Dataset, user models.User, requestID string) error
}
