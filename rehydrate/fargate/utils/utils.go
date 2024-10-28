package utils

import (
	"fmt"
	"net/url"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go/encoding/httpbinding"
)

func RehydrationLocation(destinationBucket string, datasetID, datasetVersionID int) string {
	return fmt.Sprintf("s3://%s/%s", destinationBucket, DestinationKeyPrefix(datasetID, datasetVersionID))
}
func DestinationKeyPrefix(datasetID, datasetVersionID int) string {
	return fmt.Sprintf("%d/%d/", datasetID, datasetVersionID)
}
func DestinationKey(datasetId int, versionId int, filePath string) string {
	return path.Join(DestinationKeyPrefix(datasetId, versionId), filePath)
}

func VersionedCopySource(uri string, version string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("error parsing S3 URI %s: %w", uri, err)
	}
	return fmt.Sprintf("%s%s?versionId=%s",
		u.Host, u.Path, version), nil
}

func GetApiHost(env string) string {
	if env == "prod" {
		return "https://api.pennsieve.io"

	}
	return "https://api.pennsieve.net"
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
