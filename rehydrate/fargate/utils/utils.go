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
	return fmt.Sprintf("rehydrated/%d/%d/", datasetID, datasetVersionID)
}
func CreateDestinationKey(datasetId int, versionId int, filePath string) string {
	return path.Join(DestinationKeyPrefix(datasetId, versionId), filePath)
}

func CreateDestinationBucket(datasetUri string) (string, error) {
	u, err := url.Parse(datasetUri)
	if err != nil {
		return "", fmt.Errorf("error parsing destination bucket URI [%s]: %w", datasetUri, err)
	}

	return u.Host, nil
}

func CreateVersionedSource(uri string, version string) string {
	u, _ := url.Parse(uri)
	return fmt.Sprintf("%s%s?versionId=%s",
		u.Host, u.Path, version)
}

func GetApiHost(env string) string {
	if env == "prod" {
		return "https://api.pennsieve.io"

	}
	return "https://api.pennsieve.net"
}
