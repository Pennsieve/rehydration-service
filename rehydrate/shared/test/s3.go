package test

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
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

func (f *S3Fixture) ObjectExists(bucket, key string) bool {
	headIn := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	_, err := f.Client.HeadObject(f.context, headIn)
	if err == nil {
		return true
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return false
	}
	return assert.NoError(f.T, err, "unexpected error when checking if object exists")
}

type DeleteMarkerSummary struct {
	IsLatest  bool
	Key       string
	VersionId string
}

func fromDeleteMarkerEntry(e types.DeleteMarkerEntry) DeleteMarkerSummary {
	return DeleteMarkerSummary{
		IsLatest:  aws.ToBool(e.IsLatest),
		Key:       aws.ToString(e.Key),
		VersionId: aws.ToString(e.VersionId),
	}
}

type ObjectVersionSummary struct {
	ETag      string
	IsLatest  bool
	Key       string
	Size      int64
	VersionId string
}

func fromObjectVersion(v types.ObjectVersion) ObjectVersionSummary {
	return ObjectVersionSummary{
		ETag:      aws.ToString(v.ETag),
		IsLatest:  aws.ToBool(v.IsLatest),
		Key:       aws.ToString(v.Key),
		Size:      aws.ToInt64(v.Size),
		VersionId: aws.ToString(v.VersionId),
	}
}

func summarize[S, T any](source []S, summarizer func(s S) T) []T {
	var target []T
	for _, s := range source {
		target = append(target, summarizer(s))
	}
	return target
}

// ListObjectVersions returns slices of all DeleteMarkers and Versions found in the given bucket under the given prefix if any.
// It takes care of pagination, so the returned slices are the entire listings. It is assumed that in a test situation that these
// will be small enough to hold in memory without any issues.
func (f *S3Fixture) ListObjectVersions(bucket string, prefix *string) struct {
	DeleteMarkers []DeleteMarkerSummary
	Versions      []ObjectVersionSummary
} {
	var deleteMarkers []DeleteMarkerSummary
	var versions []ObjectVersionSummary
	listInput := s3.ListObjectVersionsInput{Bucket: aws.String(bucket), Prefix: prefix}
	var isTruncated bool
	for makeRequest := true; makeRequest; makeRequest = isTruncated {
		listOutput, err := f.Client.ListObjectVersions(f.context, &listInput)
		if err != nil {
			assert.FailNow(f.T, "error listing test objects", "bucket: %s, error: %v", bucket, err)
		}
		deleteMarkers = append(deleteMarkers, summarize(listOutput.DeleteMarkers, fromDeleteMarkerEntry)...)
		versions = append(versions, summarize(listOutput.Versions, fromObjectVersion)...)
		isTruncated = aws.ToBool(listOutput.IsTruncated)
		if isTruncated {
			listInput.KeyMarker = listOutput.NextKeyMarker
			listInput.VersionIdMarker = listOutput.NextVersionIdMarker
		}
	}
	return struct {
		DeleteMarkers []DeleteMarkerSummary
		Versions      []ObjectVersionSummary
	}{
		deleteMarkers,
		versions,
	}
}

func (f *S3Fixture) AssertPrefixEmpty(bucket string, prefix string) bool {
	listOutput := f.ListObjectVersions(bucket, &prefix)
	return assert.Empty(f.T, listOutput.Versions, "prefix %s in bucket %s contains object versions", prefix, bucket) &&
		assert.Empty(f.T, listOutput.DeleteMarkers, "prefix %s in bucket %s contains delete markers", prefix, bucket)
}

func (f *S3Fixture) AssertBucketEmpty(bucket string) bool {
	listOutput := f.ListObjectVersions(bucket, nil)
	return assert.Empty(f.T, listOutput.Versions, "bucket %s contains object versions", bucket) &&
		assert.Empty(f.T, listOutput.DeleteMarkers, "bucket %s contains delete markers", bucket)
}

func (f *S3Fixture) Teardown() {
	var waitInputs []s3.HeadBucketInput
	for name := range f.Buckets {
		listOutput := f.ListObjectVersions(name, nil)
		if len(listOutput.DeleteMarkers)+len(listOutput.Versions) > 0 {
			objectIds := make([]types.ObjectIdentifier, len(listOutput.DeleteMarkers)+len(listOutput.Versions))
			i := 0
			for _, dm := range listOutput.DeleteMarkers {
				objectIds[i] = types.ObjectIdentifier{Key: aws.String(dm.Key), VersionId: aws.String(dm.VersionId)}
				i++
			}
			for _, obj := range listOutput.Versions {
				objectIds[i] = types.ObjectIdentifier{Key: aws.String(obj.Key), VersionId: aws.String(obj.VersionId)}
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

func GeneratePutObjectInputs(bucket string, prefix string, count int) []*s3.PutObjectInput {
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
