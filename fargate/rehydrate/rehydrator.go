package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/pennsieve/rehydration-service/rehydrate/utils"
)

// rehydration processor - implements object processor
type Rehydrator struct {
	S3 *s3.Client
}

func NewRehydrator(s3 *s3.Client) ObjectProcessor {
	return &Rehydrator{s3}
}

func (r *Rehydrator) Copy(ctx context.Context, src Source, dest Destination) error {
	// file is less than 100MB ? simple copy : multiPart copy
	thresholdSize := 100 * 1024 * 1024 // 100 MB
	log.Printf("copying %s (size: %v) to bucket: %s key: %s from %s ; ",
		src.GetName(), src.GetSize(), dest.GetBucket(), dest.GetKey(), src.GetVersionedUri())

	if src.GetSize() < int64(thresholdSize) {
		log.Printf("simple copying %s (size: %v) to bucket: %s key: %s from %s ; ",
			src.GetName(), src.GetSize(), dest.GetBucket(), dest.GetKey(), src.GetVersionedUri())
		params := s3.CopyObjectInput{
			Bucket:     aws.String(dest.GetBucket()),
			CopySource: aws.String(src.GetVersionedUri()),
			Key:        aws.String(dest.GetKey()),
		}

		_, err := r.S3.CopyObject(ctx, &params)
		if err != nil {
			log.Printf("error processing simple copy for %s", src.GetName())
			return err
		}
	} else {
		log.Printf("multipart copying %s (size: %v) to bucket: %s key: %s from %s ; ",
			src.GetName(), src.GetSize(), dest.GetBucket(), dest.GetKey(), src.GetVersionedUri())
		err := utils.MultiPartCopy(r.S3, src.GetSize(), src.GetVersionedUri(), dest.GetBucket(), dest.GetKey())
		if err != nil {
			log.Printf("error processing multipart copy for %s", src.GetName())
			return err
		}
	}

	return nil
}
