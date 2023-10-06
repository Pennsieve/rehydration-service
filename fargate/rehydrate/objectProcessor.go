package main

// defines a generic object processor
type ObjectProcessor interface {
	Copy(Src, Dest) error // copy a source object to a destination
}

// source
type Src interface {
	GetUri() string
	GetSize() int64
	GetName() string
	GetPath() string
	GetFullUri() string
}

// destination
type Dest interface {
	GetBucketUri() string
	GetKey() string
}
