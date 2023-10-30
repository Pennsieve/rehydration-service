package main

import (
	"github.com/pennsieve/rehydration-service/rehydrate/utils"
)

// SourceObject implements Source
type SourceObject struct {
	DatasetUri     string
	DatasetVersion int32
	Size           int64
	Name           string
	VersionId      string
	Path           string
}

// GetUri returns the dataset Uri, including the scheme
func (s SourceObject) GetDatasetUri() string {
	return s.DatasetUri
}

func (s SourceObject) GetSize() int64 {
	return s.Size
}

func (s SourceObject) GetName() string {
	return s.Name
}

func (s SourceObject) GetPath() string {
	return s.Path
}

// Get versioned source Uri, excludes scheme
func (s SourceObject) GetVersionedUri() string {
	return utils.CreateVersionedSource(
		s.GetDatasetUri(), s.VersionId)
}

// DestinationObject implements Destination
type DestinationObject struct {
	Bucket string
	Key    string
}

func (d DestinationObject) GetBucket() string {
	return d.Bucket
}

func (d DestinationObject) GetKey() string {
	return d.Key
}

// rehydration composite object - Source and Destination
type Rehydration struct {
	Src  Source
	Dest Destination
}

func NewRehydration(src SourceObject, dest DestinationObject) *Rehydration {
	return &Rehydration{src, dest}
}
