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
	objectsToClean := generateFiles(bucket, prefixToClean, 103)
	prefixToKeep := "43/11/"
	objectsToKeep := generateFiles(bucket, prefixToKeep, 10)
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithMinIO().Config(ctx, false)
	s3Client := s3.NewFromConfig(awsConfig)
	putObjectInputs := objectsToClean
	putObjectInputs = append(putObjectInputs, objectsToKeep...)

	s3Fixture, _ := test.NewS3Fixture(t, s3Client, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}).WithObjects(putObjectInputs...)
	defer s3Fixture.Teardown()

	cleaner := NewCleaner(s3Client, bucket)
	resp, err := cleaner.Clean(ctx, prefixToClean, 25)
	require.NoError(t, err)
	assert.Empty(t, resp.Errors)
	assert.Equal(t, bucket, resp.Bucket)

	for _, expectedDeleted := range objectsToClean {
		key := aws.ToString(expectedDeleted.Key)
		assert.False(t, s3Fixture.ObjectExists(bucket, key))
	}
	for _, expectedKept := range objectsToKeep {
		key := aws.ToString(expectedKept.Key)
		assert.True(t, s3Fixture.ObjectExists(bucket, key))
	}
}

func TestS3Cleaner_Clean_IllegalArgs(t *testing.T) {
	ctx := context.Background()
	bucket := "test-clean-bucket"
	awsConfig := test.NewAWSEndpoints(t).WithMinIO().Config(ctx, false)
	cleaner := NewCleaner(s3.NewFromConfig(awsConfig), bucket)
	for _, tst := range []struct {
		name          string
		keyPrefix     string
		batchSize     int32
		expectedInErr string
	}{
		{"negative batch", "12/23/", -1, "out of range"},
		{"batch too large", "12/23/", MaxDeleteBatch * 2, "out of range"},
		{"empty prefix", "", MaxDeleteBatch, "empty"},
		{"prefix does not end in slash", "12/23", MaxDeleteBatch, "'/'"},
	} {
		t.Run(tst.name, func(t *testing.T) {
			_, err := cleaner.Clean(ctx, tst.keyPrefix, tst.batchSize)
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
