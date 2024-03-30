package s3cleaner

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"strings"
)

type S3Cleaner struct {
	client *s3.Client
	bucket string
}

func NewCleaner(client *s3.Client, bucket string) Cleaner {
	return &S3Cleaner{
		client: client,
		bucket: bucket,
	}
}

func (c *S3Cleaner) Clean(ctx context.Context, keyPrefix string, batchSize int32) (*CleanResponse, error) {
	if batchSize <= 0 || batchSize > MaxCleanBatch {
		return nil, fmt.Errorf("illegal argument: batchSize %d is out of range (0, %d]", batchSize, MaxCleanBatch)
	}
	if len(keyPrefix) == 0 {
		return nil, fmt.Errorf("illegal argument: keyPrefix cannot be empty")
	}
	if !strings.HasSuffix(keyPrefix, "/") {
		return nil, fmt.Errorf("illegal argument: keyPrefix must end in '/': %s", keyPrefix)
	}
	var batchedObjectIdentifiers [][]types.ObjectIdentifier
	bucketParam := aws.String(c.bucket)

	listInput := &s3.ListObjectsV2Input{
		Bucket:       bucketParam,
		Prefix:       aws.String(keyPrefix),
		MaxKeys:      aws.Int32(batchSize),
		RequestPayer: types.RequestPayerRequester,
	}
	var continuationToken *string
	for hasNextPage := true; hasNextPage; hasNextPage = continuationToken != nil {
		listInput.ContinuationToken = continuationToken
		listOut, err := c.client.ListObjectsV2(ctx, listInput)
		if err != nil {
			return nil, fmt.Errorf("error listing objects from bucket %s under prefix %s: %w", c.bucket, keyPrefix, err)
		}
		continuationToken = listOut.NextContinuationToken
		batch := make([]types.ObjectIdentifier, len(listOut.Contents))
		for i := 0; i < len(listOut.Contents); i++ {
			batch[i] = types.ObjectIdentifier{
				Key: listOut.Contents[i].Key,
			}
		}
		batchedObjectIdentifiers = append(batchedObjectIdentifiers, batch)
		// Can we call deleteObjects in this loop to avoid holding onto the full list of ObjectIdentifiers?
		// Concern, is that deleting objects under the prefix mid-listing will somehow invalidate the continuationToken
		// for the next call to ListObjectsV2. Couldn't find mention of the issue one way or the other. But may be worth
		// experimenting with if there are memory issues.
	}
	response := &CleanResponse{
		Bucket: c.bucket,
	}
	deleteIn := &s3.DeleteObjectsInput{
		Bucket:       bucketParam,
		RequestPayer: types.RequestPayerRequester,
	}
	deleted := 0
	for _, batch := range batchedObjectIdentifiers {
		deleteIn.Delete = &types.Delete{
			Objects: batch,
			Quiet:   aws.Bool(true),
		}
		deleteOut, err := c.client.DeleteObjects(ctx, deleteIn)
		if err != nil {
			msg := fmt.Sprintf("error deleting objects from bucket %s under prefix %s", c.bucket, keyPrefix)
			if deleted > 0 {
				msg = fmt.Sprintf("%s (%d objects already deleted)", msg, deleted)
			}
			return nil, fmt.Errorf("%s: %w", msg, err)
		}
		batchErrors := deleteOut.Errors
		deleted += len(batch) - len(batchErrors)
		response.Errors = append(response.Errors, fromAWSErrors(batchErrors)...)
	}
	response.Deleted = deleted

	return response, nil
}
