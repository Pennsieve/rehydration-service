package main

import (
	"context"
	"fmt"
	"github.com/pennsieve/rehydration-service/shared/tracking"
	"time"
)

func trackingWriteUpdates(ctx context.Context, trackingStore tracking.Store, rehydrationStatus tracking.RehydrationStatus, indexEntries []tracking.DatasetVersionIndex) []error {
	var errs []error
	for _, qr := range indexEntries {
		emailSentDate := time.Now()
		if err := trackingStore.EmailSent(ctx, qr.ID, emailSentDate, rehydrationStatus); err != nil {
			errs = append(errs, fmt.Errorf("error updating tracking entry for %s email to %s: %w", rehydrationStatus, qr.UserEmail, err))
		}
	}
	return errs
}
