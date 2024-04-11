package main

import (
	"context"
	"fmt"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve"
	"github.com/pennsieve/rehydration-service/fargate/config"
	"github.com/pennsieve/rehydration-service/fargate/objects"
	"github.com/pennsieve/rehydration-service/fargate/utils"
	"github.com/pennsieve/rehydration-service/shared/models"
	"log/slog"
)

type DatasetRehydrator struct {
	dataset            *models.Dataset
	user               *models.User
	pennsieveClient    *pennsieve.Client
	processor          objects.Processor
	logger             *slog.Logger
	rehydrationBucket  string
	rehydrationTTLDays int
}

func NewDatasetRehydrator(config *config.Config, thresholdSize int64) *DatasetRehydrator {
	return &DatasetRehydrator{
		dataset:            config.Env.Dataset,
		user:               config.Env.User,
		pennsieveClient:    config.PennsieveClient(),
		processor:          config.ObjectProcessor(thresholdSize),
		logger:             config.Logger,
		rehydrationBucket:  config.Env.RehydrationBucket,
		rehydrationTTLDays: config.Env.RehydrationTTLDays,
	}
}

func (dr *DatasetRehydrator) rehydrate(ctx context.Context) (*RehydrationResult, error) {
	dataset32 := int32(dr.dataset.ID)
	version32 := int32(dr.dataset.VersionID)

	datasetMetadataByVersionResponse, err := dr.pennsieveClient.Discover.GetDatasetMetadataByVersion(ctx, dataset32, version32)
	if err != nil {
		return nil, fmt.Errorf("error retrieving dataset metadata by version: %w", err)
	}

	numberOfRehydrations := len(datasetMetadataByVersionResponse.Files)
	rehydrationCh := make(chan *Rehydration, numberOfRehydrations)
	results := make(chan FileRehydrationResult, numberOfRehydrations)

	dr.logger.Info("Starting Rehydration process")
	// create workers
	NumConcurrentWorkers := 20
	for i := 1; i <= NumConcurrentWorkers; i++ {
		go worker(ctx, i, rehydrationCh, results, dr.processor)
	}

	// create work
	var rehydrations []*Rehydration
	for _, j := range datasetMetadataByVersionResponse.Files {
		if err != nil {
			return nil, err
		}
		datasetFileByVersionResponse, err := dr.pennsieveClient.Discover.GetDatasetFileByVersion(
			ctx, dataset32, version32, j.Path)
		if err != nil {
			return nil, fmt.Errorf("error retrieving dataset file %s by version: %w", j.Path, err)
		}
		source, err := NewSourceObject(datasetFileByVersionResponse.Uri,
			datasetFileByVersionResponse.Size,
			datasetFileByVersionResponse.Name,
			datasetFileByVersionResponse.S3VersionID,
			j.Path)
		if err != nil {
			return nil, fmt.Errorf("error creating Source for file %s: %w", j.Path, err)
		}
		rehydrations = append(rehydrations, NewRehydration(
			source,
			DestinationObject{
				Bucket: dr.rehydrationBucket,
				Key: utils.CreateDestinationKey(dr.dataset.ID,
					dr.dataset.VersionID,
					j.Path),
			}))
	}
	// Only submit rehydrations once we know there are no GetDatasetFileByVersion errors
	for _, rehydration := range rehydrations {
		rehydrationCh <- rehydration
	}
	close(rehydrationCh)

	var fileResults []FileRehydrationResult
	// wait for the done signal
	for j := 1; j <= numberOfRehydrations; j++ {
		result := <-results
		fileResults = append(fileResults, result)
	}

	return &RehydrationResult{
		Location:    utils.RehydrationLocation(dr.rehydrationBucket, dr.dataset.ID, dr.dataset.VersionID),
		FileResults: fileResults,
	}, nil
}

// processes rehydrations
func worker(ctx context.Context, w int, rehydrations <-chan *Rehydration, results chan<- FileRehydrationResult, processor objects.Processor) {
	for r := range rehydrations {
		result := FileRehydrationResult{
			Worker:      w,
			Rehydration: r,
		}
		err := processor.Copy(ctx, r.Src, r.Dest)
		if err != nil {
			result.Error = err
		}
		results <- result

	}
}

type RehydrationResult struct {
	Location    string
	FileResults []FileRehydrationResult
}
type FileRehydrationResult struct {
	Worker      int
	Rehydration *Rehydration
	Error       error
}
