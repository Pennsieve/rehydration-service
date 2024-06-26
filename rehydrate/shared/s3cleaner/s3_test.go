package s3cleaner

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			objectsToClean := test.GeneratePutObjectInputs(bucket, prefixToClean, tst.toCleanCount)
			objectsToKeep := test.GeneratePutObjectInputs(bucket, prefixToKeep, 10)

			putObjectInputs := objectsToClean
			putObjectInputs = append(putObjectInputs, objectsToKeep...)

			s3Fixture, _ := test.NewS3Fixture(t, s3Client, &s3.CreateBucketInput{
				Bucket: aws.String(bucket),
			}).WithObjects(putObjectInputs...)
			defer s3Fixture.Teardown()

			cleaner, err := NewCleaner(s3Client, int32(cleanBatchSize))
			require.NoError(t, err)
			resp, err := cleaner.Clean(ctx, bucket, prefixToClean)
			require.NoError(t, err)
			assert.Empty(t, resp.Errors)
			assert.Equal(t, len(objectsToClean), resp.Deleted)
			assert.Equal(t, len(objectsToClean), resp.Count)

			s3Fixture.AssertPrefixEmpty(bucket, prefixToClean)
			for _, expectedKept := range objectsToKeep {
				key := aws.ToString(expectedKept.Key)
				assert.True(t, s3Fixture.ObjectExists(bucket, key))
			}

		})
	}
}

func TestS3Cleaner_NewCleaner_IllegalArgs(t *testing.T) {
	ctx := context.Background()
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
			_, err := NewCleaner(s3.NewFromConfig(awsConfig), tst.batchSize)
			assert.ErrorContains(t, err, tst.expectedInErr)
		})
	}
}
func TestS3Cleaner_Clean_IllegalArgs(t *testing.T) {
	ctx := context.Background()
	testBucketName := "test-clean-bucket"
	awsConfig := test.NewAWSEndpoints(t).WithMinIO().Config(ctx, false)
	cleaner, err := NewCleaner(s3.NewFromConfig(awsConfig), MaxCleanBatch)
	require.NoError(t, err)
	for _, tst := range []struct {
		name          string
		bucket        string
		keyPrefix     string
		batchSize     int32
		expectedInErr string
	}{
		{"empty bucket", "", "34/5/", MaxCleanBatch, "empty"},
		{"empty prefix", testBucketName, "", MaxCleanBatch, "empty"},
		{"prefix does not end in slash", testBucketName, "12/23", MaxCleanBatch, "'/'"},
	} {
		t.Run(tst.name, func(t *testing.T) {
			_, err := cleaner.Clean(ctx, tst.bucket, tst.keyPrefix)
			assert.ErrorContains(t, err, tst.expectedInErr)
		})
	}
}
