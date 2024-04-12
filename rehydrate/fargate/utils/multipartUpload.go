package utils

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// maxPartSize constant for number of bits in 50 megabyte chunk
// this corresponds with max file size of 500GB per file as copy can do max 10,000 parts.
const maxPartSize = 50 * 1024 * 1024

// chunkSize is the number of bytes in a copy part. It is a variable to allow for testing with smaller files.
var chunkSize int64 = maxPartSize

// nrCopyWorkers number of threads for multipart uploader
const nrCopyWorkers = 10

// MultiPartCopy function that starts, perform each part upload, and completes the copy
func MultiPartCopy(svc *s3.Client, fileSize int64, copySource string, destBucket string, destKey string, logger *slog.Logger) error {

	partWalker := make(chan s3.UploadPartCopyInput, nrCopyWorkers)
	results := make(chan s3types.CompletedPart, nrCopyWorkers)

	parts := make([]s3types.CompletedPart, 0)

	ctx, cancelFn := context.WithTimeout(context.TODO(), 30*time.Minute)
	defer cancelFn()

	//struct for starting a multipart upload
	startInput := s3.CreateMultipartUploadInput{
		Bucket:       &destBucket,
		Key:          &destKey,
		RequestPayer: s3types.RequestPayerRequester,
	}

	//send command to start copy and get the upload id as it is needed later
	var uploadId string
	createOutput, err := svc.CreateMultipartUpload(ctx, &startInput)
	if err != nil {
		return err
	}
	if createOutput != nil {
		if createOutput.UploadId != nil {
			uploadId = *createOutput.UploadId
		}
	}
	if uploadId == "" {
		return errors.New("no upload id found in start upload request")
	}

	//numUploads := fileSize / maxPartSize
	//log.Printf("Will attempt upload in %d number of parts to %s\n", numUploads, destKey)

	// Walk over all files in IMPORTED status and make available on channel for processors.
	go allocate(uploadId, fileSize, copySource, destBucket, destKey, partWalker)

	done := make(chan bool)

	go aggregateResult(done, &parts, results)

	// Wait until all processors are completed.
	createWorkerPool(ctx, svc, nrCopyWorkers, uploadId, partWalker, results, logger, destBucket, destKey)

	// Wait until done channel has a value
	<-done

	// sort parts (required for complete method
	sort.Slice(parts, func(i, j int) bool {
		return *(parts[i].PartNumber) < *(parts[j].PartNumber)
	})

	//create struct for completing the upload
	mpu := s3types.CompletedMultipartUpload{
		Parts: parts,
	}

	//complete actual upload
	complete := s3.CompleteMultipartUploadInput{
		Bucket:          &destBucket,
		Key:             &destKey,
		UploadId:        &uploadId,
		MultipartUpload: &mpu,
		RequestPayer:    s3types.RequestPayerRequester,
	}
	compOutput, err := svc.CompleteMultipartUpload(context.TODO(), &complete)
	if err != nil {
		return fmt.Errorf("error completing upload: %w", err)
	}
	if compOutput != nil {
		logger.Info("multipart copy complete")
	}
	return nil
}

// buildCopySourceRange helper function to build the string for the range of bits to copy
func buildCopySourceRange(start int64, objectSize int64) string {
	end := start + chunkSize - 1
	if end > objectSize {
		end = objectSize - 1
	}
	startRange := strconv.FormatInt(start, 10)
	stopRange := strconv.FormatInt(end, 10)
	return "bytes=" + startRange + "-" + stopRange
}

// allocate create entries into the chunk channel for the workers to consume.
func allocate(uploadId string, fileSize int64, copySource string, destBucket string, destKey string, partWalker chan s3.UploadPartCopyInput) {
	defer func() {
		close(partWalker)
	}()

	var i int64
	var partNumber int32 = 1
	for i = 0; i < fileSize; i += chunkSize {
		copySourceRange := buildCopySourceRange(i, fileSize)
		partWalker <- s3.UploadPartCopyInput{
			Bucket:          &destBucket,
			CopySource:      &copySource,
			CopySourceRange: &copySourceRange,
			Key:             &destKey,
			PartNumber:      aws.Int32(partNumber),
			UploadId:        &uploadId,
			RequestPayer:    s3types.RequestPayerRequester,
		}
		partNumber++
	}
}

// createWorkerPool creates a worker pool for uploading parts
func createWorkerPool(ctx context.Context, svc *s3.Client, nrWorkers int, uploadId string,
	partWalker chan s3.UploadPartCopyInput, results chan s3types.CompletedPart, logger *slog.Logger, destBucket, destKey string) {

	defer func() {
		close(results)
	}()

	var copyWg sync.WaitGroup
	workerFailed := false
	for w := 1; w <= nrWorkers; w++ {
		copyWg.Add(1)
		logger.Debug("starting upload-part worker", "worker", w)
		w := int32(w)
		go func() {
			err := worker(ctx, svc, &copyWg, w, partWalker, results, logger)
			if err != nil {
				logger.Error("upload-part worker failed", "worker", w, "error", err)
				workerFailed = true
			}
		}()

	}

	// Wait until all workers are finished
	copyWg.Wait()

	// Check if workers finished due to error
	if workerFailed {
		logger.Info("attempting to abort upload")
		abortIn := s3.AbortMultipartUploadInput{
			Bucket:       aws.String(destBucket),
			Key:          aws.String(destKey),
			UploadId:     aws.String(uploadId),
			RequestPayer: s3types.RequestPayerRequester,
		}
		//ignoring any errors with aborting the copy
		_, err := svc.AbortMultipartUpload(context.TODO(), &abortIn)
		if err != nil {
			logger.Error("error aborting failed upload session", "error", err)
		}
	}

	logger.Debug("finished checking status of workers")
}

// aggregateResult grabs the e-tags from results channel and aggregates in array
func aggregateResult(done chan bool, parts *[]s3types.CompletedPart, results chan s3types.CompletedPart) {

	for cPart := range results {
		*parts = append(*parts, cPart)
	}

	done <- true
}

// worker uploads parts of a file as part of copy function.
func worker(ctx context.Context, svc *s3.Client, wg *sync.WaitGroup, workerId int32,
	partWalker chan s3.UploadPartCopyInput, results chan s3types.CompletedPart, logger *slog.Logger) error {

	// Close worker after it completes.
	// This happens when the items channel closes.
	defer func() {
		logger.Debug("closing UploadPart Worker", "worker", workerId)
		wg.Done()
	}()

	for partInput := range partWalker {

		//log.Printf("Attempting to upload part %d range: %s\n", partInput.PartNumber, *partInput.CopySourceRange)
		partResp, err := svc.UploadPartCopy(ctx, &partInput)

		if err != nil {
			return err
		}

		//copy etag and part number from response as it is needed for completion
		if partResp != nil {
			partNum := partInput.PartNumber
			etag := strings.Trim(*partResp.CopyPartResult.ETag, "\"")
			cPart := s3types.CompletedPart{
				ETag:       &etag,
				PartNumber: partNum,
			}

			results <- cPart

			logger.Debug("successfully uploaded part", "partNumber", partInput.PartNumber, "uploadId", *partInput.UploadId)
		}

	}

	return nil

}
