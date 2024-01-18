package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/pennsieve/rehydration-service/service/models"
	"github.com/pennsieve/rehydration-service/service/runner"
)

var logger = logging.Default

func RehydrationServiceHandler(ctx context.Context, request events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	handlerName := "RehydrationServiceHandler"
	if lc, ok := lambdacontext.FromContext(ctx); ok {
		logger = logger.With(slog.String("awsRequestID", lc.AwsRequestID))
	}
	userClaim := authorizer.ParseClaims(request.RequestContext.Authorizer.Lambda).UserClaim
	logger = logger.With(slog.Any("userID", userClaim.Id), slog.String("userNodeID", userClaim.NodeId))

	var dataset models.Dataset
	if err := json.Unmarshal([]byte(request.Body), &dataset); err != nil {
		logger.Error("json.Unmarshal() error", "error", err)
		return events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       handlerName,
		}, ErrUnmarshaling
	}
	logger = logger.With(slog.Any("datasetID", dataset.ID), slog.Any("datasetVersionID", dataset.VersionID))

	// Get from SSM
	TaskDefinitionArn := os.Getenv("TASK_DEF_ARN")
	subIdStr := os.Getenv("SUBNET_IDS")
	SubNetIds := strings.Split(subIdStr, ",")
	cluster := os.Getenv("CLUSTER_ARN")
	SecurityGroup := os.Getenv("SECURITY_GROUP")
	envValue := os.Getenv("ENV")
	TaskDefContainerName := os.Getenv("TASK_DEF_CONTAINER_NAME")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("config.LoadDefaultConfig() error", "error", err)
		os.Exit(1)
	}

	client := ecs.NewFromConfig(cfg)
	logger.Info("Initiating new Rehydrate Fargate Task.")
	datasetIDKey := "DATASET_ID"
	datasetIDValue := strconv.Itoa(int(dataset.ID))
	datasetVersionIDKey := "DATASET_VERSION_ID"
	datasetVersionIDValue := strconv.Itoa(int(dataset.VersionID))
	envKey := "ENV"

	runTaskIn := &ecs.RunTaskInput{
		TaskDefinition: aws.String(TaskDefinitionArn),
		Cluster:        aws.String(cluster),
		NetworkConfiguration: &types.NetworkConfiguration{
			AwsvpcConfiguration: &types.AwsVpcConfiguration{
				Subnets:        SubNetIds,
				SecurityGroups: []string{SecurityGroup},
				AssignPublicIp: types.AssignPublicIpEnabled,
			},
		},
		Overrides: &types.TaskOverride{
			ContainerOverrides: []types.ContainerOverride{
				{
					Name: &TaskDefContainerName,
					Environment: []types.KeyValuePair{
						{
							Name:  &datasetIDKey,
							Value: &datasetIDValue,
						},
						{
							Name:  &datasetVersionIDKey,
							Value: &datasetVersionIDValue,
						},
						{
							Name:  &envKey,
							Value: &envValue,
						},
					},
				},
			},
		},
		LaunchType: types.LaunchTypeFargate,
	}

	runner := runner.NewECSTaskRunner(client, runTaskIn)
	if err := runner.Run(ctx); err != nil {
		logger.Error("runner.Run() error", "error", err)
		return events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       handlerName,
		}, ErrRunningFargateTask
	}

	return events.APIGatewayV2HTTPResponse{
		StatusCode: 202,
		Body:       fmt.Sprintf("%s: Fargate task accepted", handlerName),
	}, nil
}
