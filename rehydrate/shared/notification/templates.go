package notification

import (
	"embed"
	"html/template"
)

//go:embed html/rehydration-complete.html
var RehydrationCompleteTemplateFS embed.FS

var RehydrationCompleteTemplate *template.Template

type RehydrationComplete struct {
	DatasetID           int
	DatasetVersionID    int
	RehydrationLocation string
}

func LoadTemplates() (err error) {
	RehydrationCompleteTemplate, err = template.ParseFS(RehydrationCompleteTemplateFS, "html/rehydration-complete.html")
	return err
}
