package notification

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/pennsieve/rehydration-service/shared/models"
)

const PennsieveDomainKey = "PENNSIEVE_DOMAIN"

type SESEmailer struct {
	client    *ses.Client
	sender    string
	charSet   string
	awsRegion string
}

func NewEmailer(client *ses.Client, pennsieveDomain string, awsRegion string) (Emailer, error) {
	if err := LoadTemplates(); err != nil {
		return nil, err
	}
	sender := fmt.Sprintf("support@%s", pennsieveDomain)
	return &SESEmailer{
		client:    client,
		sender:    sender,
		charSet:   "UTF-8",
		awsRegion: awsRegion,
	}, nil
}

type htmlEmail struct {
	Recipient string
	Subject   string
	Body      string
}

func (e *SESEmailer) SendRehydrationComplete(ctx context.Context, dataset models.Dataset, user models.User, rehydrationLocation string) error {
	body, err := RehydrationCompleteEmailBody(dataset.ID, dataset.VersionID, rehydrationLocation, e.awsRegion)
	if err != nil {
		return err
	}
	return e.sendEmail(ctx, htmlEmail{
		Recipient: user.Email,
		Subject:   "Dataset Rehydration Complete",
		Body:      body,
	})
}

func (e *SESEmailer) SendRehydrationFailed(ctx context.Context, dataset models.Dataset, user models.User, requestID string) error {
	body, err := RehydrationFailedEmailBody(dataset.ID, dataset.VersionID, requestID, e.sender)
	if err != nil {
		return err
	}
	return e.sendEmail(ctx, htmlEmail{
		Recipient: user.Email,
		Subject:   "Dataset Rehydration Failed",
		Body:      body,
	})
}

func (e *SESEmailer) sendEmail(ctx context.Context, email htmlEmail) error {
	sendInput := &ses.SendEmailInput{
		Destination: &types.Destination{
			ToAddresses: []string{email.Recipient},
		},
		Message: &types.Message{
			Body: &types.Body{
				Html: &types.Content{
					Data:    aws.String(email.Body),
					Charset: aws.String(e.charSet),
				},
			},
			Subject: &types.Content{
				Data:    aws.String(email.Subject),
				Charset: aws.String(e.charSet),
			},
		},
		Source: aws.String(e.sender),
	}
	_, err := e.client.SendEmail(ctx, sendInput)
	if err != nil {
		return fmt.Errorf("error sending email from %s to %s: %w",
			e.sender,
			email.Recipient,
			err)
	}
	return nil
}
