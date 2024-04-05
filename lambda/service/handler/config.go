package handler

import (
	"github.com/pennsieve/rehydration-service/shared"
	"github.com/pennsieve/rehydration-service/shared/expiration"
)

type RehydrationServiceHandlerConfig struct {
	AWSRegion          string
	RehydrationTTLDays int
}

func RehydrationServiceHandlerConfigFromEnvironment() (*RehydrationServiceHandlerConfig, error) {
	awsRegion, err := shared.NonEmptyFromEnvVar(shared.AWSRegionKey)
	if err != nil {
		return nil, err
	}
	rehydrationTTLDays, err := shared.IntFromEnvVar(expiration.RehydrationTTLDays)
	if err != nil {
		return nil, err
	}
	return &RehydrationServiceHandlerConfig{AWSRegion: awsRegion, RehydrationTTLDays: rehydrationTTLDays}, nil
}
