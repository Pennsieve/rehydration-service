package test

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type S3Location struct {
	Bucket string
	Key    string
}
type S3Fixture struct {
	Fixture
	Client *s3.Client
	// Buckets is a set of bucket names
	Buckets map[string]bool
	context context.Context
}

func NewS3Fixture(t *testing.T, client *s3.Client, inputs ...*s3.CreateBucketInput) *S3Fixture {
	f := S3Fixture{
		Fixture: Fixture{T: t},
		Client:  client,
		Buckets: map[string]bool{},
		context: context.Background(),
	}
	if len(inputs) == 0 {
		return &f
	}
	var waitInputs []s3.HeadBucketInput
	for _, input := range inputs {
		bucketName := aws.ToString(input.Bucket)
		if _, err := f.Client.CreateBucket(f.context, input); err != nil {
			assert.FailNow(f.T, "error creating test bucket", "bucket: %s, error: %v", bucketName, err)
		}
		f.Buckets[bucketName] = true
		waitInputs = append(waitInputs, s3.HeadBucketInput{Bucket: aws.String(bucketName)})
	}
	if err := waitForEverything(waitInputs, func(s s3.HeadBucketInput) error {
		return s3.NewBucketExistsWaiter(f.Client).Wait(f.context, &s, time.Minute)
	}); err != nil {
		assert.FailNow(f.T, "test bucket not created", err)
	}
	return &f
}

func (f *S3Fixture) WithVersioning(buckets ...string) *S3Fixture {
	for _, bucket := range buckets {
		if _, known := f.Buckets[bucket]; !known {
			require.FailNow(f.T, "bucket does not exist in this fixture", bucket)
		}
		versioningInput := &s3.PutBucketVersioningInput{
			Bucket: aws.String(bucket),
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: types.BucketVersioningStatusEnabled,
			},
		}
		if _, err := f.Client.PutBucketVersioning(f.context, versioningInput); err != nil {
			assert.FailNow(f.T, "error enabling versioning", "bucket: %s, error: %v", bucket, err)
		}
	}
	return f
}

func (f *S3Fixture) WithObjects(objectInputs ...*s3.PutObjectInput) (*S3Fixture, map[S3Location]*s3.PutObjectOutput) {
	var waitInputs []s3.HeadObjectInput
	putOutputs := map[S3Location]*s3.PutObjectOutput{}
	for _, input := range objectInputs {
		output, err := f.Client.PutObject(f.context, input)
		if err != nil {
			assert.FailNow(f.T, "error putting test object", "bucket: %s, key: %s, error: %v", aws.ToString(input.Bucket), aws.ToString(input.Key), err)
		} else {
			putOutputs[S3Location{
				Bucket: aws.ToString(input.Bucket),
				Key:    aws.ToString(input.Key),
			}] = output
		}
		waitInputs = append(waitInputs, s3.HeadObjectInput{Bucket: input.Bucket, Key: input.Key})
	}
	if err := waitForEverything(waitInputs, func(i s3.HeadObjectInput) error {
		return s3.NewObjectExistsWaiter(f.Client).Wait(f.context, &i, time.Minute)
	}); err != nil {
		assert.FailNow(f.T, "test object not created", err)
	}
	return f, putOutputs
}

func (f *S3Fixture) AssertObjectExists(bucket, key string, expectedSize int64) bool {
	headIn := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	headOut, err := f.Client.HeadObject(f.context, headIn)
	return assert.NoError(f.T, err, "HEAD object returned an error for expected bucket: %s, key: %s, size: %d", bucket, key, expectedSize) &&
		assert.Equal(f.T, expectedSize, aws.ToInt64(headOut.ContentLength))
}

func (f *S3Fixture) Teardown() {
	var waitInputs []s3.HeadBucketInput
	for name := range f.Buckets {
		listInput := s3.ListObjectVersionsInput{Bucket: aws.String(name)}
		listOutput, err := f.Client.ListObjectVersions(f.context, &listInput)
		if err != nil {
			assert.FailNow(f.T, "error listing test objects", "bucket: %s, error: %v", name, err)
		}
		if aws.ToBool(listOutput.IsTruncated) {
			assert.FailNow(f.T, "test object list is truncated; handling truncated object list is not yet implemented", "bucket: %s, error: %v", name, err)
		}
		if len(listOutput.DeleteMarkers)+len(listOutput.Versions) > 0 {
			objectIds := make([]types.ObjectIdentifier, len(listOutput.DeleteMarkers)+len(listOutput.Versions))
			i := 0
			for _, dm := range listOutput.DeleteMarkers {
				objectIds[i] = types.ObjectIdentifier{Key: dm.Key, VersionId: dm.VersionId}
				i++
			}
			for _, obj := range listOutput.Versions {
				objectIds[i] = types.ObjectIdentifier{Key: obj.Key, VersionId: obj.VersionId}
				i++
			}
			deleteObjectsInput := s3.DeleteObjectsInput{Bucket: aws.String(name), Delete: &types.Delete{Objects: objectIds}}
			if deleteObjectsOutput, err := f.Client.DeleteObjects(f.context, &deleteObjectsInput); err != nil {
				assert.FailNow(f.T, "error deleting test objects", "bucket: %s, error: %v", name, err)
			} else if len(deleteObjectsOutput.Errors) > 0 {
				// Convert AWS Errors to string so that all the pointers AWS uses become de-referenced and readable in the output
				var errs []string
				for _, err := range deleteObjectsOutput.Errors {
					errs = append(errs, AWSErrorToString(name, err))
				}
				assert.FailNow(f.T, "errors deleting test objects", "bucket: %s, errors: %v", name, errs)
			}
		}
		deleteBucketInput := s3.DeleteBucketInput{Bucket: aws.String(name)}
		if _, err := f.Client.DeleteBucket(f.context, &deleteBucketInput); err != nil {
			assert.FailNow(f.T, "error deleting test bucket", "bucket: %s, error: %v", name, err)
		}
		waitInputs = append(waitInputs, s3.HeadBucketInput{Bucket: aws.String(name)})
	}
	if err := waitForEverything(waitInputs, func(i s3.HeadBucketInput) error {
		return s3.NewBucketNotExistsWaiter(f.Client).Wait(f.context, &i, time.Minute)
	}); err != nil {
		assert.FailNow(f.T, "test bucket not deleted", err)
	}
}
func AWSErrorToString(bucket string, error types.Error) string {
	return fmt.Sprintf("AWS error: code: %s, message: %s, S3 Object: (%s, %s, %s)",
		aws.ToString(error.Code),
		aws.ToString(error.Message),
		bucket,
		aws.ToString(error.Key),
		aws.ToString(error.VersionId))
}
