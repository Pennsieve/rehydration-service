package main

import (
	"context"
	"fmt"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"log"
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
	logger.Info("Running rehydrate task")
	ctx := context.Background()

	// TODO: remove
	log.Println("DATASET_ID", os.Getenv("DATASET_ID"))
	log.Println("DATASET_VERSION_ID", os.Getenv("DATASET_VERSION_ID"))
	log.Println("ENV", os.Getenv("ENV"))

	datasetId, err := strconv.Atoi(os.Getenv("DATASET_ID"))
	if err != nil {
		log.Fatalf("error converting datasetId to int")
	}
	versionId, err := strconv.Atoi(os.Getenv("DATASET_VERSION_ID"))
	if err != nil {
		log.Fatalf("error converting versionId to int")
	}

	pennsieveClient := pennsieve.NewClient(pennsieve.APIParams{ApiHost: utils.GetApiHost(os.Getenv("ENV"))})
	datasetByVersionReponse, err := pennsieveClient.Discover.GetDatasetByVersion(ctx, int32(datasetId), int32(versionId))
	if err != nil {
		log.Fatalf("error retrieving dataset by version")
	}
	log.Println(datasetByVersionReponse) // TODO: remove

	datasetMetadataByVersionReponse, err := pennsieveClient.Discover.GetDatasetMetadataByVersion(ctx, int32(datasetId), int32(versionId))
	if err != nil {
		log.Fatalf("error retrieving dataset by version")
	}
	log.Println(datasetMetadataByVersionReponse) // TODO: remove

	// Initializing environment
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	processor := NewRehydrator(s3.NewFromConfig(cfg), ThresholdSize)

	numberOfRehydrations := len(datasetMetadataByVersionReponse.Files)
	rehydrations := make(chan *Rehydration, numberOfRehydrations)
	results := make(chan string, numberOfRehydrations)

	log.Println("Starting Rehydration process")
	// create workers
	NumConcurrentWorkers := 20
	for i := 1; i <= NumConcurrentWorkers; i++ {
		go worker(ctx, i, rehydrations, results, processor)
	}

	// create work
	for _, j := range datasetMetadataByVersionReponse.Files {
		destinationBucket, err := utils.CreateDestinationBucket(datasetByVersionReponse.Uri)
		if err != nil {
			log.Fatalf("error creating destination bucket uri")
		}
		datasetFileByVersionResponse, err := pennsieveClient.Discover.GetDatasetFileByVersion(
			ctx, int32(datasetId), int32(versionId), j.Path)
		if err != nil {
			log.Fatalf("error retrieving dataset file by version")
		}
		log.Println(datasetFileByVersionResponse) // TODO: remove

		rehydrations <- NewRehydration(
			SourceObject{
				DatasetUri: datasetFileByVersionResponse.Uri,
				Size:       j.Size,
				Name:       j.Name,
				VersionId:  datasetFileByVersionResponse.S3VersionID,
				Path:       j.Path},
			DestinationObject{
				Bucket: destinationBucket,
				Key: utils.CreateDestinationKey(datasetByVersionReponse.ID,
					datasetByVersionReponse.Version,
					j.Path),
			})
	}
	close(rehydrations)

	// wait for the done signal
	for j := 1; j <= numberOfRehydrations; j++ {
		log.Println(<-results)
	}

	log.Println("Rehydration complete")
}

// processes rehydrations
func worker(ctx context.Context, w int, rehydrations <-chan *Rehydration, results chan<- string, processor ObjectProcessor) {
	for r := range rehydrations {
		log.Println("processing on worker: ", w)
		err := processor.Copy(ctx, r.Src, r.Dest)
		if err != nil {
			results <- err.Error()
		} else {
			results <- fmt.Sprintf("%v done", w)
		}
	}
}
