package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve"
	"github.com/pennsieve/rehydration-service/rehydrate/utils"
)

func main() {
	log.Println("Running rehydrate task")
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
	processor := NewRehydrator(s3.NewFromConfig(cfg))

	numberOfJobs := len(datasetMetadataByVersionReponse.Files)
	rehydrations := make(chan *Rehydration, numberOfJobs)
	results := make(chan string, numberOfJobs)

	log.Println("Starting Rehydration process")
	// create workers
	NumConcurrentWorkers := 20
	for i := 1; i <= NumConcurrentWorkers; i++ {
		go worker(ctx, i, rehydrations, results, processor)
	}

	// create work
	for _, j := range datasetMetadataByVersionReponse.Files {
		destinationBucketUri, err := utils.CreateDestinationBucketUri(datasetByVersionReponse.ID, datasetByVersionReponse.Uri)
		if err != nil {
			log.Fatalf("error creating destination bucket uri")
		}
		rehydrations <- NewRehydration(
			SourceObject{
				DatasetUri: datasetByVersionReponse.Uri,
				Size:       j.Size,
				Name:       j.Name,
				VersionId:  j.S3VersionID,
				Path:       j.Path},
			DestinationObject{
				BucketUri: destinationBucketUri,
				Key: utils.CreateDestinationKey(datasetByVersionReponse.ID,
					datasetByVersionReponse.Version,
					j.Path),
			})
	}
	close(rehydrations)

	// wait for the done signal
	for j := 1; j <= numberOfJobs; j++ {
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
