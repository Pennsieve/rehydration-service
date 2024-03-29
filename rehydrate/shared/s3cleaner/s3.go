package s3cleaner

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Cleaner struct {
	client *s3.Client
	bucket string
}

func NewS3Cleaner(client *s3.Client, bucket string) *S3Cleaner {
	return &S3Cleaner{
		client: client,
		bucket: bucket,
	}
}

func (c *S3Cleaner) Clean(ctx context.Context, keyPrefix string) error {
	if len(keyPrefix) == 0 {
		return fmt.Errorf("illegal argument: keyPrefix cannot be empty")
	}
	listInput := &s3.ListObjectsV2Input{
		Bucket:       aws.String(c.bucket),
		Prefix:       aws.String(keyPrefix),
		RequestPayer: types.RequestPayerRequester,
	}
	var continuationToken *string
	for hasNextPage := true; hasNextPage; hasNextPage = continuationToken != nil {
		listInput.ContinuationToken = continuationToken
		listOut, err := c.client.ListObjectsV2(ctx, listInput)
		if err != nil {
			return fmt.Errorf("error listing objects from bucket %s under prefix %s: %w", c.bucket, keyPrefix, err)
		}
		if aws.ToBool(listOut.IsTruncated) {
			continuationToken = listOut.NextContinuationToken
		}
	}
	return nil
}
