package expiration

import (
	"context"
	"errors"
	"fmt"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/s3cleaner"
	"log/slog"
	"net/url"
	"strings"
	"time"
)

type Handler struct {
	idempotencyStore idempotency.Store
	cleaner          s3cleaner.Cleaner
	logger           *slog.Logger
}

func (h *Handler) Handle(ctx context.Context) error {
	now := time.Now()
	h.logger.Info("starting expiration check", slog.Time("time", now))
	toExpire, err := h.idempotencyStore.QueryExpirationIndex(ctx, now, 100)
	if err != nil {
		return err
	}
	if len(toExpire) == 0 {
		h.logger.Info("no rehydrations to expire")
		return nil
	}
	h.logger.Info("expiring rehydrations", slog.Int("countToExpire", len(toExpire)))

	var errs []error
	for _, expIndex := range toExpire {
		logger := h.logger.With(slog.String("id", expIndex.ID), slog.String("rehydrationLocation", expIndex.RehydrationLocation))
		errs = append(errs, h.expireByIndex(ctx, logger, expIndex)...)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (h *Handler) expireByIndex(ctx context.Context, logger *slog.Logger, expirationIndex idempotency.ExpirationIndex) (errs []error) {
	logger.Info("expiring idempotency record",
		slog.Time("expirationDate", *expirationIndex.ExpirationDate))
	record, err := h.idempotencyStore.ExpireByIndex(ctx, expirationIndex)
	if err != nil {
		errs = append(errs, fmt.Errorf("error expiring idempotency record %s: %w", expirationIndex.ID, err))
		return
	}
	parsed, err := parseRehydrationLocation(expirationIndex.RehydrationLocation)
	if err != nil {
		errs = append(errs, fmt.Errorf("error cleaning rehydration location %s: %w", expirationIndex.RehydrationLocation, err))
		return
	}
	logger.Info("deleting files for idempotency record")
	resp, err := h.cleaner.Clean(ctx, parsed.bucket, parsed.prefix)
	if err != nil {
		errs = append(errs, fmt.Errorf("error cleaning rehydration location %s: %w", expirationIndex.RehydrationLocation, err))
		return
	}
	logger.Info("deleted files for idempotency record",
		slog.Int("fileCount", resp.Count),
		slog.Int("deletedCount", resp.Deleted))
	for _, e := range resp.Errors {
		errs = append(errs, fmt.Errorf("error deleting file from rehydration location %s: %s", expirationIndex.RehydrationLocation, e.Message))
	}
	if len(errs) > 0 {
		return
	}
	if err := h.idempotencyStore.DeleteRecord(ctx, expirationIndex.ID); err != nil {
		errs = append(errs, err)
		return
	}
	logger.Info("deleted idempotency record",
		slog.String("fargateTaskARN", record.FargateTaskARN))

	return
}

func DateFromNow(rehydrationTTLDays int) time.Time {
	return DateFrom(time.Now(), rehydrationTTLDays)
}

func DateFrom(start time.Time, rehydrationTTLDays int) time.Time {
	return start.Add(time.Hour * time.Duration(24*rehydrationTTLDays))
}

func parseRehydrationLocation(rehydrationLocation string) (parsed struct {
	bucket string
	prefix string
}, err error) {
	parsedUrl, err := url.Parse(rehydrationLocation)
	if err != nil {
		err = fmt.Errorf("error parsing rehydration location %s: %w",
			rehydrationLocation, err)
		return
	}
	parsed.bucket = parsedUrl.Host
	parsed.prefix = strings.TrimPrefix(parsedUrl.Path, "/")
	return
}
