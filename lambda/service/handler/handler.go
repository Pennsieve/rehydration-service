package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

func RehydrationServiceHandler(ctx context.Context, request events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	handlerName := "RehydrationServiceHandler"
	if lc, ok := lambdacontext.FromContext(ctx); ok {
		log.Println("awsRequestID", lc.AwsRequestID)
	}

	var dataset models.Dataset
	if err := json.Unmarshal([]byte(request.Body), &dataset); err != nil {
		log.Println(err.Error())
		return events.APIGatewayV2HTTPResponse{
			StatusCode: 500,
			Body:       handlerName,
		}, ErrUnmarshaling
	}

	// Get from SSM
	TaskDefinitionArn := os.Getenv("TASK_DEF_ARN")
	subIdStr := os.Getenv("SUBNET_IDS")
	SubNetIds := strings.Split(subIdStr, ",")
	cluster := os.Getenv("CLUSTER_ARN")
	SecurityGroup := os.Getenv("SECURITY_GROUP")
	envValue := os.Getenv("ENV")

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	client := ecs.NewFromConfig(cfg)
	log.Println("Initiating new Rehydrate Fargate Task.")
	taskName := "rehydrate-fargate-task"
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
					Name: &taskName,
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
		log.Println(err)
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
