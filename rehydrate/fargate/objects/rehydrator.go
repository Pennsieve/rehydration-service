package objects

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/pennsieve/rehydration-service/fargate/utils"
	"log/slog"
)

// rehydration processor - implements object processor
type Rehydrator struct {
	S3            *s3.Client
	ThresholdSize int64
	logger        *slog.Logger
}

func NewRehydrator(s3 *s3.Client, thresholdSize int64, logger *slog.Logger) Processor {
	return &Rehydrator{s3, thresholdSize, logger}
}

func (r *Rehydrator) Copy(ctx context.Context, src Source, dest Destination) error {
	// file is less than 100MB ? simple copy : multiPart copy
	copyLogger := r.logger.With(SourceLogGroup(src), DestinationLogGroup(dest))

	if src.GetSize() < r.ThresholdSize {
		copyLogger.Info("simple copy")
		params := s3.CopyObjectInput{
			Bucket:     aws.String(dest.GetBucket()),
			CopySource: aws.String(src.GetVersionedUri()),
			Key:        aws.String(dest.GetKey()),
		}

		_, err := r.S3.CopyObject(ctx, &params)
		if err != nil {
			return fmt.Errorf("error processing simple copy for %s: %w", src.GetName(), err)
		}
	} else {
		copyLogger.Info("multipart copy")
		err := utils.MultiPartCopy(r.S3, src.GetSize(), src.GetVersionedUri(), dest.GetBucket(), dest.GetKey(), copyLogger)
		if err != nil {
			return fmt.Errorf("error processing multipart copy for %s: %w", src.GetName(), err)
		}
	}

	return nil
}
