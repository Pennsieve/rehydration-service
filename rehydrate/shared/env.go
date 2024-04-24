package shared

import (
	"fmt"
	"os"
	"strconv"
)

const AWSRegionKey = "REGION"
const RehydrationBucketKey = "REHYDRATION_BUCKET"

// NonEmptyFromEnvVar looks up value of env var with the given key and returns an error if the value is not set or
// is empty. Otherwise, returns the value.
func NonEmptyFromEnvVar(key string) (string, error) {
	if value, set := os.LookupEnv(key); !set {
		return "", fmt.Errorf("required environment variable %s is not set", key)
	} else if len(value) == 0 {
		return "", fmt.Errorf("empty value set for environment variable %s", key)
	} else {
		return value, nil
	}
}

func IntFromEnvVar(key string) (int, error) {
	strVal, err := NonEmptyFromEnvVar(key)
	if err != nil {
		return 0, err
	}
	value, err := strconv.Atoi(strVal)
	if err != nil {
		return 0, fmt.Errorf("error converting value %s of %s to int: %w",
			strVal,
			key,
			err)
	}
	return value, nil
}
