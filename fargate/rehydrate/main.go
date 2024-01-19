package main

import (
	"context"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"log/slog"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve"
	"github.com/pennsieve/rehydration-service/rehydrate/utils"
)

const ThresholdSize = int64(100 * 1024 * 1024)

var logger = logging.Default

func main() {
	ctx := context.Background()

	datasetId, err := strconv.Atoi(os.Getenv("DATASET_ID"))
	if err != nil {
		logger.Error("error converting datasetId to int",
			"datasetID", os.Getenv("DATASET_ID"), "error", err)
		os.Exit(1)
	}
	versionId, err := strconv.Atoi(os.Getenv("DATASET_VERSION_ID"))
	if err != nil {
		logger.Error("error converting datasetVersionId to int",
			"datasetVersionID", os.Getenv("DATASET_VERSION_ID"), "error", err)
		os.Exit(1)
	}

	logger = logger.With(slog.Int("datasetID", datasetId), slog.Int("datasetVersionID", versionId))
	logger.Info("Running rehydrate task")

	pennsieveClient := pennsieve.NewClient(pennsieve.APIParams{ApiHost: utils.GetApiHost(os.Getenv("ENV"))})
	datasetByVersionResponse, err := pennsieveClient.Discover.GetDatasetByVersion(ctx, int32(datasetId), int32(versionId))
	if err != nil {
		logger.Error("error retrieving dataset by version", "error", err)
		os.Exit(1)
	}

	datasetMetadataByVersionResponse, err := pennsieveClient.Discover.GetDatasetMetadataByVersion(ctx, int32(datasetId), int32(versionId))
	if err != nil {
		logger.Error("error retrieving dataset metadata by version", "error", err)
		os.Exit(1)
	}

	// Initializing environment
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		logger.Error("error loading default AWS config", "error", err)
		os.Exit(1)
	}

	processor := NewRehydrator(s3.NewFromConfig(cfg), ThresholdSize)

	numberOfRehydrations := len(datasetMetadataByVersionResponse.Files)
	rehydrations := make(chan *Rehydration, numberOfRehydrations)
	results := make(chan WorkerResult, numberOfRehydrations)

	logger.Info("Starting Rehydration process")
	// create workers
	NumConcurrentWorkers := 20
	for i := 1; i <= NumConcurrentWorkers; i++ {
		go worker(ctx, i, rehydrations, results, processor)
	}

	// create work
	for _, j := range datasetMetadataByVersionResponse.Files {
		destinationBucket, err := utils.CreateDestinationBucket(datasetByVersionResponse.Uri)
		if err != nil {
			logger.Error("error creating destination bucket uri", "error", err)
			os.Exit(1)
		}
		datasetFileByVersionResponse, err := pennsieveClient.Discover.GetDatasetFileByVersion(
			ctx, int32(datasetId), int32(versionId), j.Path)
		if err != nil {
			logger.Error("error retrieving dataset file by version", "error", err)
			os.Exit(1)
		}

		rehydrations <- NewRehydration(
			SourceObject{
				DatasetUri: datasetFileByVersionResponse.Uri,
				Size:       j.Size,
				Name:       j.Name,
				VersionId:  datasetFileByVersionResponse.S3VersionID,
				Path:       j.Path},
			DestinationObject{
				Bucket: destinationBucket,
				Key: utils.CreateDestinationKey(datasetByVersionResponse.ID,
					datasetByVersionResponse.Version,
					j.Path),
			})
	}
	close(rehydrations)

	// wait for the done signal
	for j := 1; j <= numberOfRehydrations; j++ {
		result := <-results
		if result.Error != nil {
			logger.Error("error rehydrating file", result.LogGroups())
		} else {
			logger.Info("rehydrated file", result.LogGroups())
		}
	}

	logger.Info("Rehydration complete")
}

type WorkerResult struct {
	Worker      int
	Rehydration *Rehydration
	Error       error
}

func (wr *WorkerResult) LogGroups() []any {
	if wr.Error != nil {
		return wr.Rehydration.LogGroups(slog.Any("error", wr.Error))
	}
	return wr.Rehydration.LogGroups()
}

// processes rehydrations
func worker(ctx context.Context, w int, rehydrations <-chan *Rehydration, results chan<- WorkerResult, processor ObjectProcessor) {
	for r := range rehydrations {
		result := WorkerResult{
			Worker:      w,
			Rehydration: r,
		}
		logger.Info("processing on worker", r.LogGroups(slog.Int("worker", w))...)
		err := processor.Copy(ctx, r.Src, r.Dest)
		if err != nil {
			result.Error = err
		}
		results <- result

	}
}
