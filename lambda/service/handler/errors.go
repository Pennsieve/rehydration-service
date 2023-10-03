package handler

import "errors"

var ErrUnmarshaling = errors.New("error unmarshaling body")
var ErrRunningFargateTask = errors.New("error running Rehydrate fargate task")
