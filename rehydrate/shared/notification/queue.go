// BLOCKED: this file depends on github.com/pennsieve/email-service/client, which
// is not yet tagged/published. It will not compile until that module is added
// to go.mod (see the BLOCKED note in shared/go.mod). The logic and the config
// swap are complete and ready for review; only the dependency wiring is pending.

package notification

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	emailclient "github.com/pennsieve/email-service/client"
	"github.com/pennsieve/rehydration-service/shared/models"
)

// QueueEmailer is an Emailer that sends rehydration emails through the Pennsieve
// email-service by enqueuing requests on its SQS queue, instead of rendering a
// template and calling SES directly. It is a drop-in replacement for SESEmailer
// behind the Emailer interface, so callers (fargate/tracking) do not change.
type QueueEmailer struct {
	client              *emailclient.Client
	supportEmailAddress string
	awsRegion           string
}

// NewQueueEmailer constructs a QueueEmailer. queueURL is the email-service send
// queue for the environment; pennsieveDomain forms the support sender address
// used as SupportEmailAddress in the failure email.
func NewQueueEmailer(sqsClient *sqs.Client, queueURL, pennsieveDomain, awsRegion string) (Emailer, error) {
	if queueURL == "" {
		return nil, fmt.Errorf("email-service queue URL is empty")
	}
	return &QueueEmailer{
		client:              emailclient.New(sqsClient, queueURL),
		supportEmailAddress: fmt.Sprintf("support@%s", pennsieveDomain),
		awsRegion:           awsRegion,
	}, nil
}

func (e *QueueEmailer) SendRehydrationComplete(ctx context.Context, dataset models.Dataset, user models.User, rehydrationLocation string) error {
	req := emailclient.RehydrationComplete(
		emailclient.To{Name: user.Name, Email: user.Email},
		emailclient.RehydrationCompleteArgs{
			DatasetID:           strconv.Itoa(dataset.ID),
			DatasetVersionID:    strconv.Itoa(dataset.VersionID),
			RehydrationLocation: rehydrationLocation,
			AWSRegion:           e.awsRegion,
		},
	)
	return e.client.Send(ctx, req)
}

func (e *QueueEmailer) SendRehydrationFailed(ctx context.Context, dataset models.Dataset, user models.User, requestID string) error {
	req := emailclient.RehydrationFailed(
		emailclient.To{Name: user.Name, Email: user.Email},
		emailclient.RehydrationFailedArgs{
			DatasetID:           strconv.Itoa(dataset.ID),
			DatasetVersionID:    strconv.Itoa(dataset.VersionID),
			RequestID:           requestID,
			SupportEmailAddress: e.supportEmailAddress,
		},
	)
	return e.client.Send(ctx, req)
}
