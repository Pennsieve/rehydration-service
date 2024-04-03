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
	client    *s3.Client
	bucket    string
	batchSize int32
}

// NewCleaner creates a new Cleaner to delete "folders" in the given bucket.
// Deletes are done in batches of the given batchSize. It is an error if batchSize <= 0 or > MaxCleanBatch
func NewCleaner(client *s3.Client, bucket string, batchSize int32) (Cleaner, error) {
	if batchSize <= 0 || batchSize > MaxCleanBatch {
		return nil, fmt.Errorf("illegal argument: batchSize %d is out of range (0, %d]", batchSize, MaxCleanBatch)
	}
	return &S3Cleaner{
		client:    client,
		bucket:    bucket,
		batchSize: batchSize,
	}, nil
}

func (c *S3Cleaner) Clean(ctx context.Context, keyPrefix string) (*CleanResponse, error) {
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
		MaxKeys:      aws.Int32(c.batchSize),
		RequestPayer: types.RequestPayerRequester,
	}
	count := 0
	var continuationToken *string
	for hasNextPage := true; hasNextPage; hasNextPage = continuationToken != nil {
		listInput.ContinuationToken = continuationToken
		listOut, err := c.client.ListObjectsV2(ctx, listInput)
		if err != nil {
			return nil, fmt.Errorf("error listing objects from bucket %s under prefix %s: %w", c.bucket, keyPrefix, err)
		}
		continuationToken = listOut.NextContinuationToken
		countInPage := len(listOut.Contents)
		count += countInPage
		if countInPage > 0 {
			batch := make([]types.ObjectIdentifier, countInPage)
			for i := 0; i < countInPage; i++ {
				batch[i] = types.ObjectIdentifier{
					Key: listOut.Contents[i].Key,
				}
			}
			batchedObjectIdentifiers = append(batchedObjectIdentifiers, batch)
		}
		// Can we call deleteObjects in this loop to avoid holding onto the full list of ObjectIdentifiers?
		// Concern, is that deleting objects under the prefix mid-listing will somehow invalidate the continuationToken
		// for the next call to ListObjectsV2. Couldn't find mention of the issue one way or the other. But may be worth
		// experimenting with if there are memory issues.
	}
	response := &CleanResponse{
		Bucket: c.bucket,
		Count:  count,
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
