package discovertest

import (
	"fmt"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve/models/authentication"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve/models/discover"
	"github.com/pennsieve/rehydration-service/shared/models"
	"github.com/pennsieve/rehydration-service/shared/test"
	"github.com/stretchr/testify/require"
	"net/http"
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

func GetDatasetFileByVersionHandlerBuilder(dataset models.Dataset, expectedBucket string, expectedDatasetFileByPath map[string]discover.DatasetFile) *test.HandlerFuncBuilder {
	pattern := GetDatasetFileByVersionPath(dataset)
	selectorFunc := func(r *http.Request) (int, any) {
		pathQueryParam := r.URL.Query().Get("path")
		datasetFile, ok := expectedDatasetFileByPath[pathQueryParam]
		if !ok {
			return 0, nil
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

func ErrorGetDatasetFileByVersionHandlerBuilder(dataset models.Dataset, msg string, statusCode int) *test.HandlerFuncBuilder {
	response := ErrorResponse(msg, statusCode)
	return test.NewHandlerFuncBuilder(GetDatasetFileByVersionPath(dataset)).WithStatusCode(statusCode).WithModel(response)
}