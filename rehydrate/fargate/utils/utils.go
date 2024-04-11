package utils

import (
	"fmt"
	"net/url"
	"path"
)

func RehydrationLocation(destinationBucket string, datasetID, datasetVersionID int) string {
	return fmt.Sprintf("s3://%s", path.Join(destinationBucket, DestinationKeyPrefix(datasetID, datasetVersionID)))
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
