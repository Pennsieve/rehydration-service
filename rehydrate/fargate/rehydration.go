package main

import (
	"github.com/pennsieve/rehydration-service/fargate/objects"
	"github.com/pennsieve/rehydration-service/fargate/utils"
)

// SourceObject implements Source
type SourceObject struct {
	DatasetUri string
	Size       int64
	Name       string
	VersionId  string
	Path       string
	CopySource string
}

func NewSourceObject(datasetUri string, size int64, name string, versionId string, path string) (*SourceObject, error) {
	copySource, err := utils.VersionedCopySource(datasetUri, versionId)
	if err != nil {
		return nil, err
	}
	return &SourceObject{DatasetUri: datasetUri, Size: size, Name: name, VersionId: versionId, Path: path, CopySource: copySource}, nil
}

func (s *SourceObject) GetSize() int64 {
	return s.Size
}

func (s *SourceObject) GetName() string {
	return s.Name
}

func (s *SourceObject) GetPath() string {
	return s.Path
}

func (s *SourceObject) GetCopySource() string {
	return s.CopySource
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
	Src  objects.Source
	Dest objects.Destination
}

func NewRehydration(src *SourceObject, dest DestinationObject) *Rehydration {
	return &Rehydration{src, dest}
}
