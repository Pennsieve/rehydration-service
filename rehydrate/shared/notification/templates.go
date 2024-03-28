package notification

import (
	"embed"
	"fmt"
	"html/template"
	"strings"
)

// The HTML email templates in the html directory of this package
// should not be edited directly. The build process generates them
// automatically, so any changes made here will be overwritten.
//
// To modify the templates, edit the corresponding MJML file found in
// the message-templates/mjml directory in the project root. To verify the
// changes or make them available for testing, run the make email-templates target
// in the project root.
//
//go:embed html/*.html
var rehydrationEmailTemplatesFS embed.FS
var rehydrationCompleteTemplate *template.Template
var rehydrationFailedTemplate *template.Template

type rehydrationCompleteData struct {
	DatasetID           int
	DatasetVersionID    int
	RehydrationLocation string
	AWSRegion           string
}

type rehydrationFailedData struct {
	DatasetID        int
	DatasetVersionID int
	RequestID        string
}

func parseTemplate(pattern string) (*template.Template, error) {
	emailTemplate, err := template.ParseFS(rehydrationEmailTemplatesFS, pattern)
	if err != nil {
		return nil, fmt.Errorf("error parsing template %s: %w", pattern, err)
	}
	return emailTemplate, nil
}

func LoadTemplates() (err error) {
	rehydrationCompleteTemplate, err = parseTemplate("html/rehydration-complete.html")
	if err != nil {
		return
	}
	rehydrationFailedTemplate, err = parseTemplate("html/rehydration-failed.html")
	return
}

func RehydrationCompleteEmailBody(datasetID, datasetVersionID int, rehydrationLocation, awsRegion string) (string, error) {
	return executeTemplate(rehydrationCompleteTemplate, rehydrationCompleteData{
		DatasetID:           datasetID,
		DatasetVersionID:    datasetVersionID,
		RehydrationLocation: rehydrationLocation,
		AWSRegion:           awsRegion,
	})
}

func RehydrationFailedEmailBody(datasetID, datasetVersionID int, requestID string) (string, error) {
	return executeTemplate(rehydrationFailedTemplate, rehydrationFailedData{
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
