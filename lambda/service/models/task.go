package models

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	sharedmodels "github.com/pennsieve/rehydration-service/shared/models"
	"github.com/pennsieve/rehydration-service/shared/tracking"
	"os"
	"strconv"
	"strings"
)

type ECSTaskConfig struct {
	TaskDefinitionARN    string
	SubnetIDS            []string
	Cluster              string
	SecurityGroup        string
	TaskDefContainerName string
	Environment          string
	IdempotencyTableName string
	TrackingTableName    string
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
	envValue, err := nonEmptyFromEnvVar(sharedmodels.ECSTaskEnvKey)
	if err != nil {
		return nil, err
	}
	idempotencyTable, err := nonEmptyFromEnvVar(idempotency.TableNameKey)
	if err != nil {
		return nil, err
	}
	trackingTable, err := nonEmptyFromEnvVar(tracking.TableNameKey)
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
		IdempotencyTableName: idempotencyTable,
		TrackingTableName:    trackingTable,
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

func (t *ECSTaskConfig) RunTaskInput(dataset sharedmodels.Dataset, user sharedmodels.User) *ecs.RunTaskInput {
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
							Name:  aws.String(sharedmodels.ECSTaskDatasetIDKey),
							Value: aws.String(datasetID),
						},
						{
							Name:  aws.String(sharedmodels.ECSTaskDatasetVersionIDKey),
							Value: aws.String(datasetVersionID),
						},
						{
							Name:  aws.String(sharedmodels.ECSTaskUserNameKey),
							Value: aws.String(user.Name),
						},
						{
							Name:  aws.String(sharedmodels.ECSTaskUserEmailKey),
							Value: aws.String(user.Email),
						},
						{
							Name:  aws.String(sharedmodels.ECSTaskEnvKey),
							Value: aws.String(t.Environment),
						},
						{
							Name:  aws.String(idempotency.TableNameKey),
							Value: aws.String(t.IdempotencyTableName),
						},
						{
							Name:  aws.String(tracking.TableNameKey),
							Value: aws.String(t.TrackingTableName),
						},
					},
				},
			},
		},
		LaunchType: types.LaunchTypeFargate,
	}
}
