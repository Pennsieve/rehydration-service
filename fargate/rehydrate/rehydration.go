package main

import "fmt"

// SourceObject implements Source
type SourceObject struct {
	DatasetUri     string
	DatasetVersion int32
	Size           int64
	Name           string
	VersionId      string
	Path           string
}

// GetUri returns the dataset Uri
func (s SourceObject) GetUri() string {
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

func (s SourceObject) GetFullUri() string {
	return fmt.Sprintf("%s%s?versionId=%s",
		s.GetUri(), s.GetPath(), s.VersionId)
}

// DestinationObject implements Destination
type DestinationObject struct {
	BucketUri string
	Key       string
}

func (d DestinationObject) GetBucketUri() string {
	return d.BucketUri
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
