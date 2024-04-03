package expiration

import (
	"context"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/s3cleaner"
	"github.com/pennsieve/rehydration-service/shared/tracking"
	"log/slog"
	"time"
)

type Handler struct {
	idempotencyStore   idempotency.Store
	trackingStore      tracking.Store
	cleaner            s3cleaner.Cleaner
	rehydrationTTLDays int
	logger             *slog.Logger
}

func (h *Handler) Handle(ctx context.Context) error {
	rehydrationTTLHours := time.Duration(h.rehydrationTTLDays * 24)
	expirationThreshold := time.Now().Add(-time.Hour * rehydrationTTLHours)
	candidates, err := h.trackingStore.QueryExpirationIndex(ctx, expirationThreshold, 100)
	if err != nil {
		return err
	}
	// remove duplicate datasetVersions
	datasetVersionCandidates := map[string]bool{}
	for _, c := range candidates {
		datasetVersionCandidates[c.DatasetVersion] = true
	}
	for dv := range datasetVersionCandidates {
		h.logger.Info("checking datasetVersion for expiration", slog.String("datasetVersion", dv))
		// Lock DV in the idempotency table by setting to EXPIRED
		if err = h.idempotencyStore.LockRecordForExpiration(ctx, dv); err != nil {
			return err
		}
		// Look for un-emailed or too-new requests in the tracking table that will disqualify DV from expiration

		// If DV disqualified from expiration, unlock DV in idempotency table by restoring previous status and continue

		// If DV okayed for expiration, clean the rehydration location in S3 and delete idempotency record for DV
	}
	return nil
}
