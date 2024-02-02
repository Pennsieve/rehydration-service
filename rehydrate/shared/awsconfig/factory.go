package awsconfig

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

type Factory struct {
	awsConfig *aws.Config
}

func NewFactory() *Factory {
	return &Factory{}
}

func (f *Factory) Get(ctx context.Context) (*aws.Config, error) {
	if f.awsConfig == nil {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("error loading default AWS config: %w", err)
		}
		f.awsConfig = &cfg
	}
	return f.awsConfig, nil
}

func (f *Factory) Set(awsConfig *aws.Config) {
	f.awsConfig = awsConfig
}
