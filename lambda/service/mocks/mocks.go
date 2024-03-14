package mocks

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/ecs"

	"github.com/pennsieve/rehydration-service/service/runner"
)

type MockECSTaskRunner struct{}

func (r *MockECSTaskRunner) Run(ctx context.Context) (*ecs.RunTaskOutput, error) {

	return nil, nil
}

func NewMockECSTaskRunner() runner.Runner {
	return &MockECSTaskRunner{}
}
