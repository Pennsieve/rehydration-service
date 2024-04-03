package s3cleaner

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestS3Cleaner_Clean(t *testing.T) {
	bucket := "cleaner-test-bucket"
	prefixToClean := "43/1/"
	prefixToKeep := "43/11/"
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithMinIO().Config(ctx, false)
	s3Client := s3.NewFromConfig(awsConfig)

	// set a small batch size to test Clean's pagination
	cleanBatchSize := 25

	for _, tst := range []struct {
		name         string
		toCleanCount int
	}{
		{name: "equal to batch size", toCleanCount: cleanBatchSize},
		{name: "more than batch size", toCleanCount: 103},
		{name: "fewer than batch size", toCleanCount: 4},
		{name: "empty prefix", toCleanCount: 0},
	} {
		t.Run(tst.name, func(t *testing.T) {
			objectsToClean := generateFiles(bucket, prefixToClean, tst.toCleanCount)
			objectsToKeep := generateFiles(bucket, prefixToKeep, 10)

			putObjectInputs := objectsToClean
			putObjectInputs = append(putObjectInputs, objectsToKeep...)

			s3Fixture, _ := test.NewS3Fixture(t, s3Client, &s3.CreateBucketInput{
				Bucket: aws.String(bucket),
			}).WithObjects(putObjectInputs...)
			defer s3Fixture.Teardown()

			cleaner, err := NewCleaner(s3Client, bucket, int32(cleanBatchSize))
			require.NoError(t, err)
			resp, err := cleaner.Clean(ctx, prefixToClean)
			require.NoError(t, err)
			assert.Empty(t, resp.Errors)
			assert.Equal(t, bucket, resp.Bucket)
			assert.Equal(t, len(objectsToClean), resp.Deleted)
			assert.Equal(t, len(objectsToClean), resp.Count)

			for _, expectedDeleted := range objectsToClean {
				key := aws.ToString(expectedDeleted.Key)
				assert.False(t, s3Fixture.ObjectExists(bucket, key))
			}
			for _, expectedKept := range objectsToKeep {
				key := aws.ToString(expectedKept.Key)
				assert.True(t, s3Fixture.ObjectExists(bucket, key))
			}

		})
	}
}

func TestS3Cleaner_NewCleaner_IllegalArgs(t *testing.T) {
	ctx := context.Background()
	bucket := "test-clean-bucket"
	awsConfig := test.NewAWSEndpoints(t).WithMinIO().Config(ctx, false)
	for _, tst := range []struct {
		name          string
		batchSize     int32
		expectedInErr string
	}{
		{"zero batch", 0, "out of range"},
		{"negative batch", -1, "out of range"},
		{"batch too large", MaxCleanBatch * 2, "out of range"},
	} {
		t.Run(tst.name, func(t *testing.T) {
			_, err := NewCleaner(s3.NewFromConfig(awsConfig), bucket, tst.batchSize)
			assert.ErrorContains(t, err, tst.expectedInErr)
		})
	}
}
func TestS3Cleaner_Clean_IllegalArgs(t *testing.T) {
	ctx := context.Background()
	bucket := "test-clean-bucket"
	awsConfig := test.NewAWSEndpoints(t).WithMinIO().Config(ctx, false)
	cleaner, err := NewCleaner(s3.NewFromConfig(awsConfig), bucket, MaxCleanBatch)
	require.NoError(t, err)
	for _, tst := range []struct {
		name          string
		keyPrefix     string
		batchSize     int32
		expectedInErr string
	}{
		{"empty prefix", "", MaxCleanBatch, "empty"},
		{"prefix does not end in slash", "12/23", MaxCleanBatch, "'/'"},
	} {
		t.Run(tst.name, func(t *testing.T) {
			_, err := cleaner.Clean(ctx, tst.keyPrefix)
			assert.ErrorContains(t, err, tst.expectedInErr)
		})
	}
}

func generateFiles(bucket string, prefix string, count int) []*s3.PutObjectInput {
	var putObjectIns []*s3.PutObjectInput
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("file%d.txt", i)
		key := fmt.Sprintf("%s%s", prefix, name)
		content := fmt.Sprintf("content of %s\n", name)
		putObjectIns = append(putObjectIns, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader(content),
		})
	}
	return putObjectIns
}
