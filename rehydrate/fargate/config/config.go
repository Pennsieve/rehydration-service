package config

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve"
	"github.com/pennsieve/rehydration-service/fargate/objects"
	"github.com/pennsieve/rehydration-service/fargate/utils"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"github.com/pennsieve/rehydration-service/shared/models"
	"log/slog"
	"os"
	"strconv"
)

type Config struct {
	AWSConfig        aws.Config
	Env              *Env
	Logger           *slog.Logger
	pennsieveClient  *pennsieve.Client
	idempotencyStore idempotency.Store
	objectProcessor  objects.Processor
}

func NewConfig(awsConfig aws.Config, env *Env) *Config {
	logger := logging.Default.With(
		slog.Group("dataset", slog.Int("id", env.Dataset.ID), slog.Int("versionId", env.Dataset.VersionID)),
		slog.Group("user", slog.String("name", env.User.Name), slog.String("email", env.User.Email)))
	return &Config{
		AWSConfig: awsConfig,
		Env:       env,
		Logger:    logger,
	}
}

func (c *Config) PennsieveClient() *pennsieve.Client {
	if c.pennsieveClient == nil {
		c.pennsieveClient = pennsieve.NewClient(pennsieve.APIParams{ApiHost: c.Env.PennsieveHost})
	}
	return c.pennsieveClient
}

func (c *Config) IdempotencyStore() (idempotency.Store, error) {
	if c.idempotencyStore == nil {
		store, err := idempotency.NewStore(c.AWSConfig, c.Logger, c.Env.IdempotencyTable)
		if err != nil {
			return nil, err
		}
		c.idempotencyStore = store
	}
	return c.idempotencyStore, nil
}

// SetIdempotencyStore is for use in tests that would like to override the real store with a mock implementation
func (c *Config) SetIdempotencyStore(store idempotency.Store) {
	c.idempotencyStore = store
}

func (c *Config) ObjectProcessor(thresholdSize int64) objects.Processor {
	if c.objectProcessor == nil {
		c.objectProcessor = objects.NewRehydrator(s3.NewFromConfig(c.AWSConfig), thresholdSize, c.Logger)
	}
	return c.objectProcessor
}

// SetObjectProcessor is for use in tests that would like to override the real processor with a mock implementation
func (c *Config) SetObjectProcessor(objectProcessor objects.Processor) {
	c.objectProcessor = objectProcessor
}

type Env struct {
	Dataset          *models.Dataset
	User             *models.User
	TaskEnv          string
	PennsieveHost    string
	IdempotencyTable string
}

func LookupEnv() (*Env, error) {
	env := os.Getenv(models.ECSTaskEnvKey)
	if len(env) == 0 {
		return nil, fmt.Errorf("env var %s value is empty", models.ECSTaskEnvKey)
	}
	pennsieveHost := utils.GetApiHost(env)
	table := os.Getenv(idempotency.TableNameKey)
	if len(table) == 0 {
		return nil, fmt.Errorf("env var %s value is empty",
			idempotency.TableNameKey)
	}
	dataset, err := datasetFromEnv()
	if err != nil {
		return nil, err
	}
	user, err := userFromEnv()
	if err != nil {
		return nil, err
	}
	return &Env{
		Dataset:          dataset,
		User:             user,
		TaskEnv:          env,
		PennsieveHost:    pennsieveHost,
		IdempotencyTable: table,
	}, nil
}

func datasetFromEnv() (*models.Dataset, error) {
	datasetIdString := os.Getenv(models.ECSTaskDatasetIDKey)
	datasetId, err := strconv.Atoi(datasetIdString)
	if err != nil {
		return nil, fmt.Errorf("error converting env var %s value [%s] to int: %w",
			models.ECSTaskDatasetIDKey, datasetIdString, err)
	}
	datasetVersionIdString := os.Getenv(models.ECSTaskDatasetVersionIDKey)
	versionId, err := strconv.Atoi(datasetVersionIdString)
	if err != nil {
		return nil, fmt.Errorf("error converting env var %s value [%s] to int: %w",
			models.ECSTaskDatasetVersionIDKey, datasetVersionIdString, err)
	}
	return &models.Dataset{
		ID:        datasetId,
		VersionID: versionId,
	}, nil
}

func userFromEnv() (*models.User, error) {
	userName := os.Getenv(models.ECSTaskUserNameKey)
	if len(userName) == 0 {
		return nil, fmt.Errorf("env var %s value is empty",
			models.ECSTaskUserNameKey)
	}
	userEmail := os.Getenv(models.ECSTaskUserEmailKey)
	if len(userEmail) == 0 {
		return nil, fmt.Errorf("env var %s value is empty",
			models.ECSTaskUserEmailKey)
	}
	return &models.User{
		Name:  userName,
		Email: userEmail,
	}, nil
}
