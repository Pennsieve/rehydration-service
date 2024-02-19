package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pennsieve/rehydration-service/shared/awsconfig"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"log/slog"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve"
	"github.com/pennsieve/rehydration-service/fargate/utils"
)

const ThresholdSize = int64(100 * 1024 * 1024)

var logger = logging.Default
var awsConfigFactory = awsconfig.NewFactory()

func main() {
	ctx := context.Background()
	rehydrator, err := NewDatasetRehydrator(ctx)
	if err != nil {
		logger.Error("error creating DatasetRehydrator: %v", err)
		//TODO update DynamoDB with error
		os.Exit(1)
	}

	results, err := rehydrator.rehydrate(ctx)
	if err != nil {
		rehydrator.logger.Error("error rehydrating dataset: %v", err)
		//TODO update DynamoDB with error
		os.Exit(1)
	}

	var fileErrors []error
	for _, result := range results.FileResults {
		if result.Error != nil {
			rehydrator.logger.Error("error rehydrating file", result.LogGroups()...)
			fileErrors = append(fileErrors, result.Error)
		}
	}

	if len(fileErrors) > 0 {
		//TODO update DynamoDB with error
		os.Exit(1)
	}
	//TODO update DynamoDB with success
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

func (wr *FileRehydrationResult) LogGroups() []any {
	if wr.Error != nil {
		return wr.Rehydration.LogGroups(slog.Any("error", wr.Error))
	}
	return wr.Rehydration.LogGroups()
}

// processes rehydrations
func worker(ctx context.Context, w int, rehydrations <-chan *Rehydration, results chan<- FileRehydrationResult, processor ObjectProcessor) {
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

type DatasetRehydrator struct {
	datasetID       int
	versionID       int
	pennsieveClient *pennsieve.Client
	awsConfig       aws.Config
	logger          *slog.Logger
}

func NewDatasetRehydrator(ctx context.Context) (*DatasetRehydrator, error) {
	datasetId, err := strconv.Atoi(os.Getenv("DATASET_ID"))
	if err != nil {
		return nil, fmt.Errorf("error converting env var DATASET_ID value [%s] to int: %w",
			os.Getenv("DATASET_ID"), err)
	}
	versionId, err := strconv.Atoi(os.Getenv("DATASET_VERSION_ID"))
	if err != nil {
		return nil, fmt.Errorf("error converting env var DATASET_VERSION_ID value [%s] to int: %w",
			os.Getenv("DATASET_VERSION_ID"), err)
	}

	// Initializing environment
	cfg, err := awsConfigFactory.Get(ctx)
	if err != nil {
		return nil, err
	}

	return &DatasetRehydrator{
		datasetID:       datasetId,
		versionID:       versionId,
		pennsieveClient: pennsieve.NewClient(pennsieve.APIParams{ApiHost: utils.GetApiHost(os.Getenv("ENV"))}),
		awsConfig:       *cfg,
		logger:          logging.Default.With(slog.Int("datasetID", datasetId), slog.Int("datasetVersionID", versionId)),
	}, nil
}

func (dr *DatasetRehydrator) rehydrate(ctx context.Context) (*RehydrationResult, error) {
	dr.logger.Info("Running rehydrate task")
	dataset32 := int32(dr.datasetID)
	version32 := int32(dr.versionID)

	datasetByVersionResponse, err := dr.pennsieveClient.Discover.GetDatasetByVersion(ctx, dataset32, version32)
	if err != nil {
		return nil, fmt.Errorf("error retrieving dataset by version: %w", err)
	}
	destinationBucket, err := utils.CreateDestinationBucket(datasetByVersionResponse.Uri)
	if err != nil {
		return nil, err
	}
	datasetMetadataByVersionResponse, err := dr.pennsieveClient.Discover.GetDatasetMetadataByVersion(ctx, dataset32, version32)
	if err != nil {
		return nil, fmt.Errorf("error retrieving dataset metadata by version: %w", err)
	}

	processor := NewRehydrator(s3.NewFromConfig(dr.awsConfig), ThresholdSize)

	numberOfRehydrations := len(datasetMetadataByVersionResponse.Files)
	rehydrations := make(chan *Rehydration, numberOfRehydrations)
	results := make(chan FileRehydrationResult, numberOfRehydrations)

	dr.logger.Info("Starting Rehydration process")
	// create workers
	NumConcurrentWorkers := 20
	for i := 1; i <= NumConcurrentWorkers; i++ {
		go worker(ctx, i, rehydrations, results, processor)
	}

	// create work
	for _, j := range datasetMetadataByVersionResponse.Files {
		if err != nil {
			return nil, err
		}
		datasetFileByVersionResponse, err := dr.pennsieveClient.Discover.GetDatasetFileByVersion(
			ctx, dataset32, version32, j.Path)
		if err != nil {
			return nil, fmt.Errorf("error retrieving dataset file %s by version: %w", j.Path, err)
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
				Key: utils.CreateDestinationKey(dr.datasetID,
					dr.versionID,
					j.Path),
			})
	}
	close(rehydrations)

	var fileResults []FileRehydrationResult
	// wait for the done signal
	for j := 1; j <= numberOfRehydrations; j++ {
		result := <-results
		fileResults = append(fileResults, result)
	}

	dr.logger.Info("Rehydration complete")
	return &RehydrationResult{
		Location:    utils.RehydrationLocation(destinationBucket, dr.datasetID, dr.versionID),
		FileResults: fileResults,
	}, nil
}
