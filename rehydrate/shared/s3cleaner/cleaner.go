package s3cleaner

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// MaxCleanBatch is the maximum number of keys that can be sent to DeleteObjects in one call.
const MaxCleanBatch = int32(1000)

type Cleaner interface {
	// Clean deletes all objects in the given bucket under the given keyPrefix.
	//
	// It is an error for the bucket name to be empty.
	//
	// It is an error for the keyPrefix to be empty. (If we want to delete a whole bucket, we
	// should add a separate method for that so that it can't be done accidentally.)
	//
	// It is an error if the keyPrefix does not end in '/'
	//
	// Callers should check CleanResponse for DeleteObjectErrors which correspond to the non-error errors
	// DeleteObject returns.
	Clean(ctx context.Context, bucket string, keyPrefix string) (*CleanResponse, error)
}

type CleanResponse struct {
	// Count is the number of objects found under the given prefix
	Count int
	// Deleted is the number of object successfully deleted
	Deleted int
	// Errors will be non-empty if there were problems deleting individual objects
	Errors []DeleteObjectError
}

// DeleteObjectError corresponds to the AWS types.Error type returned by DeleteObject. These are not actually Go errors and are
// passed in the normal response, so we mimic that here.
type DeleteObjectError struct {
	Key     string
	Message string
}

func fromAWSError(awsError types.Error) DeleteObjectError {
	key := aws.ToString(awsError.Key)
	message := fmt.Sprintf("error deleting object %s: AWS message: %s, Code: %s",
		key,
		aws.ToString(awsError.Message),
		aws.ToString(awsError.Code))
	return DeleteObjectError{
		Key:     key,
		Message: message,
	}
}

func fromAWSErrors(awsErrors []types.Error) []DeleteObjectError {
	deleteObjectErrors := make([]DeleteObjectError, len(awsErrors))
	for i := 0; i < len(awsErrors); i++ {
		deleteObjectErrors[i] = fromAWSError(awsErrors[i])
	}
	return deleteObjectErrors
}
