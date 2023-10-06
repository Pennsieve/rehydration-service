package utils

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func CreateDestinationKey(datasetId int32, versionId int32, path string) string {
	return fmt.Sprintf("rehydrated/%s/%s/%s",
		strconv.Itoa(int(datasetId)), strconv.Itoa(int(versionId)), path)
}

func CreateDestinationBucketUri(datasetId int32, datasetUri string) (string, error) {
	destinationBucketUri, _, found := strings.Cut(
		datasetUri, strconv.Itoa(int(datasetId)))
	if !found {
		return "", errors.New("error creating destinationBucketUri")
	}
	return destinationBucketUri, nil
}

func GetApiHost(env string) string {
	if os.Getenv("ENV") == "dev" {
		return "https://api.pennsieve.net"
	} else {
		return "https://api.pennsieve.io"
	}
}
