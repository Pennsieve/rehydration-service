package discovertest

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve/models/authentication"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve/models/discover"
	"github.com/pennsieve/rehydration-service/shared/models"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"testing"
)

var defaultCognitoConfig = authentication.CognitoConfig{
	Region: "us-east-1",
	UserPool: authentication.UserPool{
		Region:      "us-east-1",
		ID:          "mock-user-pool-id",
		AppClientID: "mock-user-pool-app-client-id",
	},
	TokenPool: authentication.TokenPool{
		Region:      "us-east-1",
		AppClientID: "mockTokenPoolAppClientId",
	},
	IdentityPool: authentication.IdentityPool{
		Region: "us-east-1",
		ID:     "mock-identity-pool-id",
	}}

type ServerFixture struct {
	*test.HTTPMuxTestFixture
	T require.TestingT
}

// NewServerFixture A wrapper around test.HTTPMuxTestFixture for mocking Discover service responses.
// Written for Go 1.21, so before methods and wildcards were part of http.ServeMux patterns.
func NewServerFixture(t require.TestingT, cognitoConfig *authentication.CognitoConfig, handlerBuilders ...*test.HandlerFuncBuilder) *ServerFixture {
	ccModel := cognitoConfig
	if ccModel == nil {
		ccModel = &defaultCognitoConfig
	}
	builders := []*test.HandlerFuncBuilder{test.NewHandlerFuncBuilder("/authentication/cognito-config").WithModel(ccModel)}
	builders = append(builders, handlerBuilders...)
	fixture := test.NewHTTPMuxTestFixture(t, builders...)
	return &ServerFixture{T: t, HTTPMuxTestFixture: fixture}
}

func (d *ServerFixture) HandleFunc(pattern string, handlerFunc http.HandlerFunc) {
	d.Mux.HandleFunc(pattern, handlerFunc)
}

// Paths

func GetDatasetFileByVersionPath(dataset models.Dataset) string {
	return fmt.Sprintf("/discover/datasets/%d/versions/%d/files", dataset.ID, dataset.VersionID)
}

func GetDatasetByVersionPath(dataset models.Dataset) string {
	return fmt.Sprintf("/discover/datasets/%d/versions/%d", dataset.ID, dataset.VersionID)
}

func GetDatasetMetadataByVersionPath(dataset models.Dataset) string {
	return fmt.Sprintf("/discover/datasets/%d/versions/%d/metadata", dataset.ID, dataset.VersionID)
}

// test.HandlerFuncBuilders that will return models

func GetDatasetByVersionHandlerBuilder(dataset models.Dataset, expectedBucket string) *test.HandlerFuncBuilder {
	pattern := GetDatasetByVersionPath(dataset)
	respModel := discover.GetDatasetByVersionResponse{
		ID:      int32(dataset.ID),
		Name:    "test dataset",
		Version: int32(dataset.VersionID),
		Uri:     fmt.Sprintf("s3://%s/%d/", expectedBucket, dataset.ID),
	}
	return test.NewHandlerFuncBuilder(pattern).WithModel(respModel)
}

func GetDatasetMetadataByVersionHandlerBuilder(dataset models.Dataset, expectedDatasetFiles []discover.DatasetFile) *test.HandlerFuncBuilder {
	pattern := GetDatasetMetadataByVersionPath(dataset)
	respModel := discover.GetDatasetMetadataByVersionResponse{
		ID:      int32(dataset.ID),
		Name:    "test dataset",
		Version: int32(dataset.VersionID),
		Files:   expectedDatasetFiles,
	}
	return test.NewHandlerFuncBuilder(pattern).WithModel(respModel)
}

func GetDatasetFileByVersionHandlerBuilder(dataset models.Dataset, expectedBucket string, expectedDatasetFileByPath map[string]*TestDatasetFile) *test.HandlerFuncBuilder {
	pattern := GetDatasetFileByVersionPath(dataset)
	selectorFunc := func(r *http.Request) (int, any) {
		pathQueryParam := r.URL.Query().Get("path")
		datasetFile, ok := expectedDatasetFileByPath[pathQueryParam]
		if !ok {
			return 0, nil
		}
		responseModel := discover.GetDatasetFileByVersionResponse{
			Name: "test dataset",
			// In these responses, the path is actually the key (so it includes the dataset id as the first component)
			Path:        datasetFile.s3key,
			Size:        datasetFile.Size,
			FileType:    datasetFile.FileType,
			Uri:         fmt.Sprintf("s3://%s/%d/%s", expectedBucket, dataset.ID, datasetFile.Path),
			S3VersionID: datasetFile.S3VersionID,
		}
		return http.StatusOK, responseModel
	}
	return test.NewHandlerFuncBuilder(pattern).WithSelectorFunc(selectorFunc)
}

// test.HandlerFuncBuilders that will return errors

// ErrorResponse returns a string formatted to match the format of an error response from the discover client.
//
// The struct used by the discover client is not exported (probably just an oversight), otherwise we'd just use that.
func ErrorResponse(message string, code int) string {
	return fmt.Sprintf(`{"Code": %d, "Message": %q}`, code, message)
}
func ErrorGetDatasetByVersionHandlerBuilder(dataset models.Dataset, msg string, statusCode int) *test.HandlerFuncBuilder {
	response := ErrorResponse(msg, statusCode)
	return test.NewHandlerFuncBuilder(GetDatasetByVersionPath(dataset)).WithStatusCode(statusCode).WithModel(response)
}

func ErrorGetDatasetMetadataByVersionHandlerBuilder(dataset models.Dataset, msg string, statusCode int) *test.HandlerFuncBuilder {
	response := ErrorResponse(msg, statusCode)
	return test.NewHandlerFuncBuilder(GetDatasetMetadataByVersionPath(dataset)).WithStatusCode(statusCode).WithModel(response)
}

func ErrorGetDatasetFileByVersionHandlerBuilder(dataset models.Dataset, expectedBucket string, expectedDatasetFileByPath map[string]discover.DatasetFile, pathsToFail map[string]bool) *test.HandlerFuncBuilder {
	pattern := GetDatasetFileByVersionPath(dataset)
	selectorFunc := func(r *http.Request) (int, any) {
		pathQueryParam := r.URL.Query().Get("path")
		datasetFile, ok := expectedDatasetFileByPath[pathQueryParam]
		if !ok {
			return 0, nil
		}
		if _, fail := pathsToFail[pathQueryParam]; fail {
			return http.StatusNotFound, ErrorResponse(fmt.Sprintf("file %s not found", pathQueryParam), http.StatusNotFound)
		}
		responseModel := discover.GetDatasetFileByVersionResponse{
			Name:        "test dataset",
			Path:        datasetFile.Path,
			Size:        datasetFile.Size,
			FileType:    datasetFile.FileType,
			Uri:         fmt.Sprintf("s3://%s/%d/%s", expectedBucket, dataset.ID, datasetFile.Path),
			S3VersionID: datasetFile.S3VersionID,
		}
		return http.StatusOK, responseModel
	}
	return test.NewHandlerFuncBuilder(pattern).WithSelectorFunc(selectorFunc)
}

type TestDatasetFile struct {
	discover.DatasetFile
	content string
	s3key   string
}

type TestDatasetFiles struct {
	Files  []TestDatasetFile
	ByPath map[string]*TestDatasetFile
	ByKey  map[string]*TestDatasetFile
}

func NewTestDatasetFiles(dataset models.Dataset, count int) *TestDatasetFiles {
	datasetFiles := make([]TestDatasetFile, count)
	datasetFilesByPath := map[string]*TestDatasetFile{}
	datasetFilesByKey := map[string]*TestDatasetFile{}
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("file%d.txt", i)
		path := fmt.Sprintf("files/dir%d/%s", i, name)
		content := fmt.Sprintf("content of %s\n", name)
		datasetFile := TestDatasetFile{
			DatasetFile: discover.DatasetFile{
				Name:     name,
				Path:     path,
				FileType: "TEXT",
				Size:     int64(len([]byte(content))),
			},
			content: content,
			s3key:   fmt.Sprintf("%d/%s", dataset.ID, path),
		}
		datasetFiles[i] = datasetFile
		datasetFilesByPath[datasetFile.Path] = &datasetFiles[i]
		datasetFilesByKey[datasetFile.s3key] = &datasetFiles[i]
	}

	return &TestDatasetFiles{
		Files:  datasetFiles,
		ByPath: datasetFilesByPath,
		ByKey:  datasetFilesByKey,
	}
}

func (f *TestDatasetFiles) WithFakeS3VersionsIDs() *TestDatasetFiles {
	for i := range f.Files {
		file := &f.Files[i]
		file.S3VersionID = uuid.NewString()
	}
	return f
}

func (f *TestDatasetFiles) SetS3VersionID(t *testing.T, s3Location test.S3Location, s3VersionId string) {
	datasetFile, ok := f.ByKey[s3Location.Key]
	require.Truef(t, ok, "missing DatasetFile: bucket: %s, key: %s", s3Location.Bucket, s3Location.Key)
	datasetFile.S3VersionID = s3VersionId
}

func (f *TestDatasetFiles) PutObjectInputs(bucket string) []*s3.PutObjectInput {
	var putObjectInputs []*s3.PutObjectInput
	for _, file := range f.Files {
		putObjectInputs = append(putObjectInputs, &s3.PutObjectInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(file.s3key),
			Body:          strings.NewReader(file.content),
			ContentLength: aws.Int64(file.Size),
		})
	}
	return putObjectInputs
}

func (f *TestDatasetFiles) DatasetFiles() []discover.DatasetFile {
	var datasetFiles []discover.DatasetFile
	for _, file := range f.Files {
		datasetFiles = append(datasetFiles, file.DatasetFile)
	}
	return datasetFiles
}

func (f *TestDatasetFiles) DatasetFilesByPath() map[string]discover.DatasetFile {
	dfByPath := map[string]discover.DatasetFile{}
	for p, f := range f.ByPath {
		dfByPath[p] = f.DatasetFile
	}
	return dfByPath
}
