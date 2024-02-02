package runner

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/ecs"
)

type ECSTaskRunner struct {
	Client *ecs.Client
	Input  *ecs.RunTaskInput
}

func NewECSTaskRunner(client *ecs.Client, input *ecs.RunTaskInput) Runner {
	return &ECSTaskRunner{client, input}
}

func (r *ECSTaskRunner) Run(ctx context.Context) (*ecs.RunTaskOutput, error) {
	return r.Client.RunTask(ctx, r.Input)
}
