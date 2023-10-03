package mocks

import (
	"context"

	"github.com/pennsieve/rehydration-service/service/runner"
)

type MockECSTaskRunner struct{}

func (r *MockECSTaskRunner) Run(ctx context.Context) error {

	return nil
}

func NewMockECSTaskRunner() runner.Runner {
	return &MockECSTaskRunner{}
}
