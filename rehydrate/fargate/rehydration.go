package main

import (
	"github.com/pennsieve/rehydration-service/fargate/utils"
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

// LogGroups transforms this Rehydration into a collection of structured logging properties to be used by a slog.Logger.
// Example usage: slog.Info("file rehydration complete", r.LogGroups()...)
// or slog.Error("file rehydration failed", r.LogGroups(slog.Any("error", err))...)
func (r *Rehydration) LogGroups(additionalArgs ...any) []any {
	groups := []any{SourceLogGroup(r.Src), DestinationLogGroup(r.Dest)}
	groups = append(groups, additionalArgs)
	return groups
}
