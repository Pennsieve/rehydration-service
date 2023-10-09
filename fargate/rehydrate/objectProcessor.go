package main

import "context"

// defines a generic object processor
type ObjectProcessor interface {
	Copy(context.Context, Source, Destination) error // copy a source object to a destination
}

// source
type Source interface {
	GetUri() string
	GetSize() int64
	GetName() string
	GetPath() string
	GetFullUri() string
}

// destination
type Destination interface {
	GetBucketUri() string
	GetKey() string
}
