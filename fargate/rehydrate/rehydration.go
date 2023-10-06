package main

import "fmt"

// SrcObject implements Src
type SrcObject struct {
	DatasetUri     string
	DatasetVersion int32
	Size           int64
	Name           string
	VersionId      string
	Path           string
}

// TODO: should this Uri returned also include the path?
func (s SrcObject) GetUri() string {
	return s.DatasetUri
}

func (s SrcObject) GetSize() int64 {
	return s.Size
}

func (s SrcObject) GetName() string {
	return s.Name
}

func (s SrcObject) GetPath() string {
	return s.Path
}

func (s SrcObject) GetFullUri() string {
	return fmt.Sprintf("%s%s?versionId=%s",
		s.GetUri(), s.GetPath(), s.VersionId)

}

// DestObject implements Dest
type DestObject struct {
	BucketUri string
	Key       string
}

func (d DestObject) GetBucketUri() string {
	return d.BucketUri
}

func (d DestObject) GetKey() string {
	return d.Key
}

// rehydration composite object - Src and Dest
type Rehydration struct {
	Src  Src
	Dest Dest
}

func NewRehydration(src SrcObject, dest DestObject) *Rehydration {
	return &Rehydration{src, dest}
}
