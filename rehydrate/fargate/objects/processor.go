package objects

import (
	"context"
	"log/slog"
)

// defines a generic object processor
type Processor interface {
	Copy(context.Context, Source, Destination) error // copy a source object to a destination
}

// source
type Source interface {
	GetSize() int64
	GetName() string
	GetPath() string
	// GetCopySource returns a string to be used as the CopySource in AWS CopyObject or PartUploadCopy requests.
	GetCopySource() string
}

// SourceLogGroup transforms a Source into a slog.Attr Group to be used for structured logging.
// Example usage: slog.Info("copying source", SourceLogGroup(s)) or to create a re-usable logger that
// will always include info about the particular Source s from an existing slog.Logger:
// sourceLogger := logger.With(SourceLogGroup(s))
func SourceLogGroup(s Source) slog.Attr {
	return slog.Group("source",
		slog.String("copySource", s.GetCopySource()),
		slog.Int64("size", s.GetSize()),
		slog.String("name", s.GetName()),
		slog.String("path", s.GetPath()))
}

// destination
type Destination interface {
	GetBucket() string
	GetKey() string
}

// DestinationLogGroup transforms a Destination into a slog.Attr to be used for structured logging.
// Example usage: slog.Info("destination set", DestinationLogGroup(d)) or
// to create a re-usable logger that will always include info about the particular Destination d from an existing slog.Logger:
// destinationLogger := logger.With(DestinationLogGroup(s))
func DestinationLogGroup(d Destination) slog.Attr {
	return slog.Group("destination",
		slog.String("bucket", d.GetBucket()),
		slog.String("key", d.GetKey()))
}
