package models

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"os"
	"strconv"
	"strings"
)

const ecsTaskDatasetIDKey = "DATASET_ID"
const ecsTaskDatasetVersionIDKey = "DATASET_VERSION_ID"
const ecsTaskEnvKey = "ENV"

type ECSTaskConfig struct {
	TaskDefinitionARN    string
	SubnetIDS            []string
	Cluster              string
	SecurityGroup        string
	TaskDefContainerName string
	Environment          string
}

func TaskConfigFromEnvironment() (*ECSTaskConfig, error) {
	taskDefinitionArn, err := nonEmptyFromEnvVar("TASK_DEF_ARN")
	if err != nil {
		return nil, err
	}
	subIdStr, err := nonEmptyFromEnvVar("SUBNET_IDS")
	if err != nil {
		return nil, err
	}
	subNetIds := strings.Split(subIdStr, ",")
	cluster, err := nonEmptyFromEnvVar("CLUSTER_ARN")
	if err != nil {
		return nil, err
	}
	securityGroup, err := nonEmptyFromEnvVar("SECURITY_GROUP")
	if err != nil {
		return nil, err
	}
	taskDefContainerName, err := nonEmptyFromEnvVar("TASK_DEF_CONTAINER_NAME")
	if err != nil {
		return nil, err
	}
	envValue, err := nonEmptyFromEnvVar("ENV")
	if err != nil {
		return nil, err
	}

	return &ECSTaskConfig{
		TaskDefinitionARN:    taskDefinitionArn,
		SubnetIDS:            subNetIds,
		Cluster:              cluster,
		SecurityGroup:        securityGroup,
		TaskDefContainerName: taskDefContainerName,
		Environment:          envValue,
	}, nil
}

func nonEmptyFromEnvVar(key string) (string, error) {
	if value, set := os.LookupEnv(key); !set {
		return "", fmt.Errorf("required environment variable %s is not set", key)
	} else if len(value) == 0 {
		return "", fmt.Errorf("empty value set for environment variable %s", key)
	} else {
		return value, nil
	}
}

func (t *ECSTaskConfig) RunTaskInput(dataset Dataset) *ecs.RunTaskInput {
	datasetID := strconv.Itoa(dataset.ID)
	datasetVersionID := strconv.Itoa(dataset.VersionID)
	return &ecs.RunTaskInput{
		TaskDefinition: aws.String(t.TaskDefinitionARN),
		Cluster:        aws.String(t.Cluster),
		NetworkConfiguration: &types.NetworkConfiguration{
			AwsvpcConfiguration: &types.AwsVpcConfiguration{
				Subnets:        t.SubnetIDS,
				SecurityGroups: []string{t.SecurityGroup},
				AssignPublicIp: types.AssignPublicIpEnabled,
			},
		},
		Overrides: &types.TaskOverride{
			ContainerOverrides: []types.ContainerOverride{
				{
					Name: aws.String(t.TaskDefContainerName),
					Environment: []types.KeyValuePair{
						{
							Name:  aws.String(ecsTaskDatasetIDKey),
							Value: aws.String(datasetID),
						},
						{
							Name:  aws.String(ecsTaskDatasetVersionIDKey),
							Value: aws.String(datasetVersionID),
						},
						{
							Name:  aws.String(ecsTaskEnvKey),
							Value: aws.String(t.Environment),
						},
					},
				},
			},
		},
		LaunchType: types.LaunchTypeFargate,
	}
}
