package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
)

// rehydration processor - implements object processor
type Rehydrator struct {
	S3 *s3.Client
}

func NewRehydrator(s3 *s3.Client) ObjectProcessor {
	return &Rehydrator{s3}
}

func (r *Rehydrator) Copy(ctx context.Context, src Source, dest Destination) error {
	// TODO: file is less than or equal to 100MB ? simple copy : multiPart copy
	log.Printf("copying %s (size: %v) to bucket: %s key: %s from %s ; ",
		src.GetName(), src.GetSize(), dest.GetBucketUri(), dest.GetKey(), src.GetFullUri())

	params := s3.CopyObjectInput{
		Bucket:     aws.String(dest.GetBucketUri()),
		CopySource: aws.String(src.GetFullUri()),
		Key:        aws.String(dest.GetKey()),
	}

	_, err := r.S3.CopyObject(ctx, &params)
	if err != nil {
		return err
	}

	return nil
}
