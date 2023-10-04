package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/pennsieve/pennsieve-go/pkg/pennsieve"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve/models/discover"
)

func main() {
	log.Println("Running rehydrate task")
	ctx := context.Background()

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
	log.Println(datasetByVersionReponse)

	datasetMetadataByVersionReponse, err := pennsieveClient.Discover.GetDatasetMetadataByVersion(ctx, int32(datasetId), int32(versionId))
	if err != nil {
		log.Fatalf("error retrieving dataset by version")
	}
	log.Println(datasetMetadataByVersionReponse)

	numberOfJobs := len(datasetMetadataByVersionReponse.Files)
	jobs := make(chan discover.DatasetFile, numberOfJobs)
	results := make(chan string, numberOfJobs)

	log.Println("Starting Rehydration process")
	// create workers
	NumConcurrentWorkers := 20
	for i := 1; i <= NumConcurrentWorkers; i++ {
		go worker(i, jobs, results)
	}

	// create work
	for _, j := range datasetMetadataByVersionReponse.Files {
		jobs <- j
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

func worker(w int, jobs <-chan discover.DatasetFile, results chan<- string) {
	for j := range jobs {
		log.Println("starting ", w)
		log.Println("processing ", j)

		results <- fmt.Sprintf("%v done", w)
	}

}
