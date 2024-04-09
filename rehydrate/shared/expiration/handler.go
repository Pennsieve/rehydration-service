package expiration

import (
	"context"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/s3cleaner"
	"log/slog"
	"time"
)

type Handler struct {
	idempotencyStore idempotency.Store
	cleaner          s3cleaner.Cleaner
	logger           *slog.Logger
}

func (h *Handler) Handle(ctx context.Context) error {
	now := time.Now()
	toExpire, err := h.idempotencyStore.QueryExpirationIndex(ctx, now, 100)
	if err != nil {
		return err
	}
	for _, expIndex := range toExpire {
		h.logger.Info("expiring idempotency record", slog.String("id", expIndex.ID))
		// Set record status to expired
		// Clean the rehydration location in S3 and delete idempotency record for DV
	}
	return nil
}

func DateFromNow(rehydrationTTLDays int) time.Time {
	return DateFrom(time.Now(), rehydrationTTLDays)
}

func DateFrom(start time.Time, rehydrationTTLDays int) time.Time {
	return start.Add(time.Hour * time.Duration(24*rehydrationTTLDays))
}
