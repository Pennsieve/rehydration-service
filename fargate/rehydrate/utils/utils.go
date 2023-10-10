package utils

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
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

func CreateVersionedSource(uri string, key string, version string) string {
	u, _ := url.Parse(uri)
	return fmt.Sprintf("%s%s%s?versionId=%s",
		u.Host, u.Path, key, version)
}

func GetApiHost(env string) string {
	if os.Getenv("ENV") == "dev" {
		return "https://api.pennsieve.net"
	} else {
		return "https://api.pennsieve.io"
	}
}
