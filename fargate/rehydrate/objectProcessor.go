package main

import (
	"context"
	"log/slog"
)

// defines a generic object processor
type ObjectProcessor interface {
	Copy(context.Context, Source, Destination) error // copy a source object to a destination
}

// source
type Source interface {
	GetVersionedUri() string
	GetSize() int64
	GetName() string
	GetPath() string
	GetDatasetUri() string
}

func SourceLogGroup(s Source) slog.Attr {
	return slog.Group("source",
		slog.String("versionedURI", s.GetVersionedUri()),
		slog.Int64("size", s.GetSize()),
		slog.String("name", s.GetName()),
		slog.String("path", s.GetPath()),
		slog.String("datasetURI", s.GetDatasetUri()))
}

// destination
type Destination interface {
	GetBucket() string
	GetKey() string
}

func DestinationLogGroup(d Destination) slog.Attr {
	return slog.Group("destination",
		slog.String("bucket", d.GetBucket()),
		slog.String("key", d.GetKey()))
}
