package utils

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestMultiPartCopy(t *testing.T) {
	//logging.Level.Set(slog.LevelDebug)
	// set a lower partSize for the test. This is 5 MiB, the minimum allowed part size
	partSize = 5242880
	ctx := context.Background()
	awsConfig := test.NewAWSEndpoints(t).WithMinIO().Config(ctx, false)
	sourceBucket := "test-source-bucket"
	sourceKey := "13/files/test-file.dat"
	targetBucket := "test-target-bucket"
	targetKey := "13/2/files/test-file.dat"

	testFile := openTestFile(t, "multipart-upload-test.dat")
	defer func() {
		require.NoError(t, testFile.Close())
	}()
	testFileInfo, err := testFile.Stat()
	require.NoError(t, err)
	testFileSize := testFileInfo.Size()

	s3Fixture, putObjectOuts := test.NewS3Fixture(t,
		s3.NewFromConfig(awsConfig),
		&s3.CreateBucketInput{Bucket: aws.String(sourceBucket)},
		&s3.CreateBucketInput{Bucket: aws.String(targetBucket)}).
		WithVersioning(sourceBucket).
		WithObjects(&s3.PutObjectInput{
			Bucket:        aws.String(sourceBucket),
			Key:           aws.String(sourceKey),
			Body:          testFile,
			ContentLength: aws.Int64(testFileSize),
		})
	defer s3Fixture.Teardown()

	putObjectOut, ok := putObjectOuts[test.S3Location{
		Bucket: sourceBucket,
		Key:    sourceKey,
	}]
	require.True(t, ok)
	require.NotNil(t, putObjectOut.VersionId)

	copySource := fmt.Sprintf("%s/%s?versionId%s", sourceBucket, sourceKey, aws.ToString(putObjectOut.VersionId))
	require.NoError(t,
		MultiPartCopy(ctx, s3Fixture.Client,
			testFileSize,
			copySource,
			targetBucket,
			targetKey,
			logging.Default))

	s3Fixture.AssertObjectExists(targetBucket, targetKey, testFileSize)
}

func openTestFile(t *testing.T, name string) *os.File {
	filePath := filepath.Join("testdata", name)
	file, err := os.Open(filePath)
	require.NoError(t, err)
	return file
}
