package awsclient

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ses"
)

type ClientBuilder[T any] func(config aws.Config) *T

// Factory is intended to create an AWS service client on the first call to Factory.Get and
// then return that instance on any subsequent calls to Factory.Get. In order to share AWS service
// clients as much as possible.
//
// This Factory is not goroutine safe, but the returned AWS clients are (per AWS documentation), so
// Factory.Get should not be called from multiple goroutines, but the returned client can be shared among routines.
type Factory[T any] struct {
	builder  ClientBuilder[T]
	instance *T
}

func NewFactory[T any](builder ClientBuilder[T]) *Factory[T] {
	return &Factory[T]{
		builder: builder,
	}
}

func (f *Factory[T]) Get(awsConfig aws.Config) *T {
	if f.instance == nil {
		f.instance = f.builder(awsConfig)
	}
	return f.instance
}

// Some adapters for the various AWS services to get around the fact that the NewFromConfig functions have a
// second, varg argument of different types for the different services.

var S3ClientBuilder = func(config aws.Config) *s3.Client {
	return s3.NewFromConfig(config)
}

var DyDBClientBuilder = func(config aws.Config) *dynamodb.Client {
	return dynamodb.NewFromConfig(config)
}

var SESClientBuilder = func(config aws.Config) *ses.Client {
	return ses.NewFromConfig(config)
}
