package shared

import (
	"fmt"
	"os"
)

const AWSRegionKey = "REGION"

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
