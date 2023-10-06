package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve"
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

	pennsieveClient := pennsieve.NewClient(pennsieve.APIParams{ApiHost: getApiHost(os.Getenv("ENV"))})
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
	jobs := make(chan *Rehydration, numberOfJobs)
	results := make(chan string, numberOfJobs)

	log.Println("Starting Rehydration process")
	// create workers
	NumConcurrentWorkers := 20
	for i := 1; i <= NumConcurrentWorkers; i++ {
		go worker(i, jobs, results, processor)
	}

	// create work
	for _, j := range datasetMetadataByVersionReponse.Files {
		jobs <- NewRehydration(
			SrcObject{
				DatasetUri: datasetByVersionReponse.Uri,
				Size:       j.Size,
				Name:       j.Name,
				VersionId:  j.S3VersionID,
				Path:       j.Path},
			DestObject{
				BucketUri: createDestinationBucketUri(datasetByVersionReponse.ID, datasetByVersionReponse.Uri),
				Key: createDestinationKey(datasetByVersionReponse.ID,
					datasetByVersionReponse.Version,
					j.Path),
			})
	}
	close(jobs)

	// wait for the done signal
	for j := 1; j <= numberOfJobs; j++ {
		log.Println(<-results)
	}

	log.Println("Rehydration complete")
}

func getApiHost(env string) string {
	if os.Getenv("ENV") == "dev" {
		return "https://api.pennsieve.net"
	} else {
		return "https://api.pennsieve.io"
	}
}

func worker(w int, jobs <-chan *Rehydration, results chan<- string, processor ObjectProcessor) {
	for j := range jobs {
		log.Println("processing on worker: ", w)
		err := processor.Copy(j.Src, j.Dest)
		if err != nil {
			results <- err.Error() // TODO: test code path
		}
		results <- fmt.Sprintf("%v done", w)
	}
}

func createDestinationKey(id int32, version int32, path string) string {
	return fmt.Sprintf("rehydrated/%s/%s/%s",
		strconv.Itoa(int(id)), strconv.Itoa(int(version)), path)
}

func createDestinationBucketUri(id int32, datasetUri string) string {
	idString := strconv.Itoa(int(id))
	destinationBucketUri, _, found := strings.Cut(datasetUri, idString)
	if !found {
		log.Fatalf("error creating bucket Uri")
	}
	return destinationBucketUri
}
