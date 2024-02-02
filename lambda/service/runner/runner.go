package runner

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
)

type Runner interface {
	Run(context.Context) (*ecs.RunTaskOutput, error)
}
