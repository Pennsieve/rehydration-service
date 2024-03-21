package notification

import (
	"embed"
	"fmt"
	"html/template"
	"strings"
)

//go:embed html/*.html
var rehydrationEmailTemplatesFS embed.FS
var RehydrationCompleteTemplate *template.Template
var RehydrationFailedTemplate *template.Template

type RehydrationComplete struct {
	DatasetID           int
	DatasetVersionID    int
	RehydrationLocation string
}

type RehydrationFailed struct {
	DatasetID        int
	DatasetVersionID int
	RequestID        string
}

func LoadTemplates() (err error) {
	RehydrationCompleteTemplate, err = template.ParseFS(rehydrationEmailTemplatesFS, "html/rehydration-complete.html")
	if err != nil {
		return
	}
	RehydrationFailedTemplate, err = template.ParseFS(rehydrationEmailTemplatesFS, "html/rehydration-failed.html")
	return
}

func RehydrationCompleteEmailBody(datasetID, datasetVersionID int, rehydrationLocation string) (string, error) {
	return executeTemplate(RehydrationCompleteTemplate, RehydrationComplete{
		DatasetID:           datasetID,
		DatasetVersionID:    datasetVersionID,
		RehydrationLocation: rehydrationLocation,
	})
}

func RehydrationFailedEmailBody(datasetID, datasetVersionID int, requestID string) (string, error) {
	return executeTemplate(RehydrationFailedTemplate, RehydrationFailed{
		DatasetID:        datasetID,
		DatasetVersionID: datasetVersionID,
		RequestID:        requestID,
	})
}

func executeTemplate(emailTemplate *template.Template, data any) (string, error) {
	if emailTemplate == nil {
		return "", fmt.Errorf("email templates are not initialized. Need to call notification.LoadTemplates()")
	}
	var emailBuilder strings.Builder
	if err := emailTemplate.Execute(&emailBuilder, data); err != nil {
		return "", fmt.Errorf("error executing %s email template: %w", emailTemplate.Name(), err)
	}
	return emailBuilder.String(), nil
}
