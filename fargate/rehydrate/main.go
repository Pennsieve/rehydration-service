package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/pennsieve/pennsieve-go/pkg/pennsieve"
)

var rehydrateProcessWg sync.WaitGroup

func main() {
	log.Println("Running rehydrate task")
	ctx := context.Background()

	log.Println("DATASET_ID", os.Getenv("DATASET_ID"))
	log.Println("DATASET_VERSION_ID", os.Getenv("DATASET_VERSION_ID"))
	log.Println("ENV", os.Getenv("ENV"))

	datasetId, err := strconv.Atoi(os.Getenv("DATASET_ID"))
	if err != nil {
		log.Fatalf("err converting datasetId to int")
	}
	versionId, err := strconv.Atoi(os.Getenv("DATASET_VERSION_ID"))
	if err != nil {
		log.Fatalf("err converting versionId to int")
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

	log.Println("Starting Rehaydration process")
	for i := 1; i <= 20; i++ {
		rehydrateProcessWg.Add(1)

		go func(i int) {
			defer rehydrateProcessWg.Done()
			worker(i)
		}(i)
	}

	rehydrateProcessWg.Wait()
	log.Println("Rehaydration complete")
}

func getApiHost(env string) string {
	if os.Getenv("ENV") == "dev" {
		return "https://api.pennsieve.net"
	} else {
		return "https://api.pennsieve.io"
	}
}

func worker(w int) {
	log.Println("starting ", w)
	log.Printf("%v done", w)

}
