package main

import (
	"context"
	"fmt"
	"github.com/pennsieve/rehydration-service/shared/models"
	"github.com/pennsieve/rehydration-service/shared/tracking"
	"log/slog"
	"time"
)

func (h *TaskHandler) emailAndLog(ctx context.Context, indexEntries []tracking.DatasetVersionIndex) []error {
	var errs []error
	if h.Result == nil {
		errs = append(errs, fmt.Errorf("illegal state: TaskResult has not been set"))
		return errs
	}
	rehydrationStatus := h.Result.RehydrationStatus()

	// If a user clicked rehydrate more than once, try to only send one email per address
	emailedAddresses := map[string]*time.Time{}
	for _, qr := range indexEntries {
		emailSentDate, alreadySent := emailedAddresses[qr.UserEmail]
		if !alreadySent {
			var err error
			if emailSentDate, err = h.sendEmail(ctx, qr); err == nil {
				// store the non-nil sent date to prevent more than one email per address
				emailedAddresses[qr.UserEmail] = emailSentDate
				h.DatasetRehydrator.logger.Info("sent email", slog.String("status", string(rehydrationStatus)),
					slog.Time("time", *emailSentDate))
			} else {
				errs = append(errs, fmt.Errorf("error sending %s email to %s (%s): %w",
					rehydrationStatus,
					qr.UserName,
					qr.UserEmail,
					err))
				// If the email failed, still want to update the tracking log below. Since we don't think the
				// email was sent, explicitly setting sent date to nil to capture that.
				emailSentDate = nil
			}
		}
		if err := h.TrackingStore.EmailSent(ctx, qr.ID, emailSentDate, rehydrationStatus); err != nil {
			errs = append(errs, fmt.Errorf("error updating tracking entry: status to %s email to %s: %w", rehydrationStatus, qr.UserEmail, err))
		}
	}
	return errs
}

func (h *TaskHandler) sendEmail(ctx context.Context, index tracking.DatasetVersionIndex) (*time.Time, error) {
	if h.Result == nil {
		return nil, fmt.Errorf("illegal state: TaskResult has not been set")
	}
	taskDataset := h.DatasetRehydrator.dataset
	if h.DatasetRehydrator.dataset.DatasetVersion() != index.DatasetVersion {
		return nil, fmt.Errorf("illegal argument: indexEntry datasetVersion %s is different than task datasetVesion: %s",
			index.DatasetVersion,
			taskDataset.DatasetVersion())
	}
	user := models.User{
		Name:  index.UserName,
		Email: index.UserEmail,
	}
	if h.Result.Failed() {
		if err := h.Emailer.SendRehydrationFailed(ctx, *taskDataset, user, index.ID); err != nil {
			return nil, err
		}
	} else {
		if err := h.Emailer.SendRehydrationComplete(ctx, *taskDataset, user, h.Result.RehydrationLocation); err != nil {
			return nil, err
		}
	}
	sentDate := time.Now()
	return &sentDate, nil
}
