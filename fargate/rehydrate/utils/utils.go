package utils

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go/encoding/httpbinding"
)

func CreateDestinationKey(datasetId int32, versionId int32, path string) string {
	return fmt.Sprintf("rehydrated/%s/%s/%s",
		strconv.Itoa(int(datasetId)), strconv.Itoa(int(versionId)), path)
}

func CreateDestinationBucket(datasetUri string) (string, error) {
	u, err := url.Parse(datasetUri)
	if err != nil {
		return "", errors.New("error creating destination bucket")
	}

	return u.Host, nil
}

func CreateVersionedSource(uri string, version string) string {
	u, _ := url.Parse(uri)
	return fmt.Sprintf("%s%s?versionId=%s",
		u.Host, u.Path, version)
}

func GetApiHost(env string) string {
	if os.Getenv("ENV") == "dev" {
		return "https://api.pennsieve.net"
	} else {
		return "https://api.pennsieve.io"
	}
}

func CreateAWSEscapedPath(s string) *string {
	// Trial and error has shown that the encoding done by httpbinding.EscapePath works better
	// with S3 than either url.PathEscape() or url.Parse().EscapedPath(). Those net/url methods
	// resulted in 404 copy failures for some tricky file names.
	escapedPath := httpbinding.EscapePath(s, false)
	return aws.String(escapedPath)
}

func CreateURLEscapedPath(s string) string {
	return url.QueryEscape(s)
}
