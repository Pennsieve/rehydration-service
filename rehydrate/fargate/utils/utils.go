package utils

import (
	"fmt"
	"net/url"
	"strconv"
)

func CreateDestinationKey(datasetId int32, versionId int32, path string) string {
	return fmt.Sprintf("rehydrated/%s/%s/%s",
		strconv.Itoa(int(datasetId)), strconv.Itoa(int(versionId)), path)
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
