package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve"
	"github.com/pennsieve/rehydration-service/fargate/utils"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"github.com/pennsieve/rehydration-service/shared/models"
	"log/slog"
	"os"
	"strconv"
)

type DatasetRehydrator struct {
	dataset         *models.Dataset
	user            *models.User
	pennsieveClient *pennsieve.Client
	awsConfig       aws.Config
	logger          *slog.Logger
}

func NewDatasetRehydrator(ctx context.Context) (*DatasetRehydrator, error) {
	dataset, err := datasetFromEnv()
	if err != nil {
		return nil, err
	}
	user, err := userFromEnv()
	if err != nil {
		return nil, err
	}
	cfg, err := awsConfigFactory.Get(ctx)
	if err != nil {
		return nil, err
	}

	return &DatasetRehydrator{
		dataset:         dataset,
		user:            user,
		pennsieveClient: pennsieve.NewClient(pennsieve.APIParams{ApiHost: utils.GetApiHost(os.Getenv(models.ECSTaskEnvKey))}),
		awsConfig:       *cfg,
		logger: logging.Default.With(
			slog.Group("dataset", slog.Int("id", dataset.ID), slog.Int("versionId", dataset.VersionID)),
			slog.Group("user", slog.String("name", user.Name), slog.String("email", user.Email))),
	}, nil
}

func (dr *DatasetRehydrator) rehydrate(ctx context.Context) (*RehydrationResult, error) {
	dr.logger.Info("Running rehydrate task")
	dataset32 := int32(dr.dataset.ID)
	version32 := int32(dr.dataset.VersionID)

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

	processor := NewRehydrator(s3.NewFromConfig(dr.awsConfig), ThresholdSize, dr.logger)

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
				Key: utils.CreateDestinationKey(dr.dataset.ID,
					dr.dataset.VersionID,
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
		Location:    utils.RehydrationLocation(destinationBucket, dr.dataset.ID, dr.dataset.VersionID),
		FileResults: fileResults,
	}, nil
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

func datasetFromEnv() (*models.Dataset, error) {
	datasetIdString := os.Getenv(models.ECSTaskDatasetIDKey)
	datasetId, err := strconv.Atoi(datasetIdString)
	if err != nil {
		return nil, fmt.Errorf("error converting env var %s value [%s] to int: %w",
			models.ECSTaskDatasetIDKey, datasetIdString, err)
	}
	datasetVersionIdString := os.Getenv(models.ECSTaskDatasetVersionIDKey)
	versionId, err := strconv.Atoi(datasetVersionIdString)
	if err != nil {
		return nil, fmt.Errorf("error converting env var %s value [%s] to int: %w",
			models.ECSTaskDatasetVersionIDKey, datasetVersionIdString, err)
	}
	return &models.Dataset{
		ID:        datasetId,
		VersionID: versionId,
	}, nil
}

func userFromEnv() (*models.User, error) {
	userName := os.Getenv(models.ECSTaskUserNameKey)
	if len(userName) == 0 {
		return nil, fmt.Errorf("env var %s value is empty",
			models.ECSTaskUserNameKey)
	}
	userEmail := os.Getenv(models.ECSTaskUserEmailKey)
	if len(userEmail) == 0 {
		return nil, fmt.Errorf("env var %s value is empty",
			models.ECSTaskUserEmailKey)
	}
	return &models.User{
		Name:  userName,
		Email: userEmail,
	}, nil
}
