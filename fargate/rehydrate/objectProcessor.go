package main

import "context"

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

// destination
type Destination interface {
	GetBucket() string
	GetKey() string
}
