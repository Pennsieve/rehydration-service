package awsclient

import (
	"github.com/aws/aws-sdk-go-v2/aws"
)

type ClientBuilder[T, O any] func(config aws.Config, optFns ...func(*O)) *T

// Supplier is intended to create an AWS service client on the first call to Supplier.Get and
// then return that instance on any subsequent calls to Supplier.Get. In order to share AWS service
// clients as much as possible.
//
// This Supplier is NOT goroutine safe, but the returned AWS clients are (per AWS documentation), so
// Supplier.Get should only be called in tha "main" goroutine, but the returned client can be shared among goroutines.
type Supplier[T, O any] struct {
	builder  ClientBuilder[T, O]
	instance *T
}

func NewSupplier[T, O any](builder ClientBuilder[T, O]) *Supplier[T, O] {
	return &Supplier[T, O]{
		builder: builder,
	}
}

func (f *Supplier[T, O]) Get(awsConfig aws.Config, optFns ...func(*O)) *T {
	if f.instance == nil {
		f.instance = f.builder(awsConfig, optFns...)
	}
	return f.instance
}
