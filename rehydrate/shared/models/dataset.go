package models

import "fmt"

type Dataset struct {
	ID        int `json:"datasetId"`
	VersionID int `json:"datasetVersionId"`
}

func (d *Dataset) DatasetVersion() string {
	return DatasetVersion(d.ID, d.VersionID)
}

func DatasetVersion(datasetID int, datasetVersionID int) string {
	return fmt.Sprintf("%d/%d/", datasetID, datasetVersionID)
}
