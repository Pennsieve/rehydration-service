package handler

import "github.com/pennsieve/rehydration-service/shared"

type RehydrationServiceHandlerConfig struct {
	AWSRegion string
}

func RehydrationServiceHandlerConfigFromEnvironment() (*RehydrationServiceHandlerConfig, error) {
	awsRegion, err := shared.NonEmptyFromEnvVar(shared.AWSRegionKey)
	if err != nil {
		return nil, err
	}
	return &RehydrationServiceHandlerConfig{AWSRegion: awsRegion}, nil
}
