package config

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/pennsieve/pennsieve-go/pkg/pennsieve"
	"github.com/pennsieve/rehydration-service/fargate/objects"
	"github.com/pennsieve/rehydration-service/fargate/utils"
	"github.com/pennsieve/rehydration-service/shared"
	"github.com/pennsieve/rehydration-service/shared/awsclient"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"github.com/pennsieve/rehydration-service/shared/models"
	"github.com/pennsieve/rehydration-service/shared/notification"
	"github.com/pennsieve/rehydration-service/shared/s3cleaner"
	"github.com/pennsieve/rehydration-service/shared/tracking"
	"log/slog"
	"strconv"
)

type Config struct {
	Env                *Env
	Logger             *slog.Logger
	pennsieveClient    *pennsieve.Client
	idempotencyStore   idempotency.Store
	objectProcessor    objects.Processor
	trackingStore      tracking.Store
	emailer            notification.Emailer
	cleaner            s3cleaner.Cleaner
	s3ClientSupplier   *awsclient.Supplier[s3.Client, s3.Options]
	dyDBClientSupplier *awsclient.Supplier[dynamodb.Client, dynamodb.Options]
	sesClientSupplier  *awsclient.Supplier[ses.Client, ses.Options]
}

func NewConfig(awsConfig aws.Config, env *Env) *Config {
	logger := logging.Default.With(
		slog.Group("dataset", slog.Int("id", env.Dataset.ID), slog.Int("versionId", env.Dataset.VersionID)),
		slog.Group("user", slog.String("name", env.User.Name), slog.String("email", env.User.Email)))
	return &Config{
		Env:                env,
		Logger:             logger,
		s3ClientSupplier:   awsclient.NewSupplier(s3.NewFromConfig, awsConfig),
		dyDBClientSupplier: awsclient.NewSupplier(dynamodb.NewFromConfig, awsConfig),
		sesClientSupplier:  awsclient.NewSupplier(ses.NewFromConfig, awsConfig),
	}
}

func (c *Config) PennsieveClient() *pennsieve.Client {
	if c.pennsieveClient == nil {
		c.pennsieveClient = pennsieve.NewClient(pennsieve.APIParams{ApiHost: c.Env.PennsieveHost})
	}
	return c.pennsieveClient
}

func (c *Config) IdempotencyStore() idempotency.Store {
	if c.idempotencyStore == nil {
		store := idempotency.NewStore(c.dyDBClientSupplier.Get(), c.Logger, c.Env.IdempotencyTable)
		c.idempotencyStore = store
	}
	return c.idempotencyStore
}

// SetIdempotencyStore is for use in tests that would like to override the real store with a mock implementation
func (c *Config) SetIdempotencyStore(store idempotency.Store) {
	c.idempotencyStore = store
}

func (c *Config) TrackingStore() tracking.Store {
	if c.trackingStore == nil {
		store := tracking.NewStore(c.dyDBClientSupplier.Get(), c.Logger, c.Env.TrackingTable)
		c.trackingStore = store
	}
	return c.trackingStore
}

// SetTrackingStore is for use in tests that would like to override the real store with a mock implementation
func (c *Config) SetTrackingStore(store tracking.Store) {
	c.trackingStore = store
}

func (c *Config) ObjectProcessor(thresholdSize int64) objects.Processor {
	if c.objectProcessor == nil {
		s3Client := c.s3ClientSupplier.Get()
		c.objectProcessor = objects.NewRehydrator(s3Client, thresholdSize, c.Logger)
	}
	return c.objectProcessor
}

// SetObjectProcessor is for use in tests that would like to override the real processor with a mock implementation
func (c *Config) SetObjectProcessor(objectProcessor objects.Processor) {
	c.objectProcessor = objectProcessor
}

func (c *Config) Emailer() (notification.Emailer, error) {
	if c.emailer == nil {
		emailer, err := notification.NewEmailer(c.sesClientSupplier.Get(), c.Env.PennsieveDomain, c.Env.AWSRegion)
		if err != nil {
			return nil, err
		}
		c.emailer = emailer
	}
	return c.emailer, nil
}

// SetEmailer is for use in tests that would like to override the real emailer with a mock implementation
func (c *Config) SetEmailer(emailer notification.Emailer) {
	c.emailer = emailer
}

func (c *Config) Cleaner() s3cleaner.Cleaner {
	if c.cleaner == nil {
		cleaner := s3cleaner.NewCleaner(c.s3ClientSupplier.Get(), c.Env.RehydrationBucket)
		c.cleaner = cleaner
	}
	return c.cleaner
}

// SetCleaner is for use in tests that would like to override the real S3 Cleaner with a mock implementation
func (c *Config) SetCleaner(cleaner s3cleaner.Cleaner) {
	c.cleaner = cleaner
}

type Env struct {
	Dataset           *models.Dataset
	User              *models.User
	TaskEnv           string
	PennsieveHost     string
	IdempotencyTable  string
	TrackingTable     string
	PennsieveDomain   string
	AWSRegion         string
	RehydrationBucket string
}

func LookupEnv() (*Env, error) {
	env, err := shared.NonEmptyFromEnvVar(models.ECSTaskEnvKey)
	if err != nil {
		return nil, err
	}
	pennsieveHost := utils.GetApiHost(env)
	idempotencyTable, err := shared.NonEmptyFromEnvVar(idempotency.TableNameKey)
	if err != nil {
		return nil, err
	}
	trackingTable, err := shared.NonEmptyFromEnvVar(tracking.TableNameKey)
	if err != nil {
		return nil, err
	}
	pennsieveDomain, err := shared.NonEmptyFromEnvVar(notification.PennsieveDomainKey)
	if err != nil {
		return nil, err
	}
	awsRegion, err := shared.NonEmptyFromEnvVar(shared.AWSRegionKey)
	if err != nil {
		return nil, err
	}
	rehydrationBucket, err := shared.NonEmptyFromEnvVar(shared.RehydrationBucketKey)
	if err != nil {
		return nil, err
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
		Dataset:           dataset,
		User:              user,
		TaskEnv:           env,
		PennsieveHost:     pennsieveHost,
		IdempotencyTable:  idempotencyTable,
		TrackingTable:     trackingTable,
		PennsieveDomain:   pennsieveDomain,
		AWSRegion:         awsRegion,
		RehydrationBucket: rehydrationBucket,
	}, nil
}

func datasetFromEnv() (*models.Dataset, error) {
	datasetIdString, err := shared.NonEmptyFromEnvVar(models.ECSTaskDatasetIDKey)
	if err != nil {
		return nil, err
	}
	datasetId, err := strconv.Atoi(datasetIdString)
	if err != nil {
		return nil, fmt.Errorf("error converting env var %s value [%s] to int: %w",
			models.ECSTaskDatasetIDKey, datasetIdString, err)
	}
	datasetVersionIdString, err := shared.NonEmptyFromEnvVar(models.ECSTaskDatasetVersionIDKey)
	if err != nil {
		return nil, err
	}
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
	userName, err := shared.NonEmptyFromEnvVar(models.ECSTaskUserNameKey)
	if err != nil {
		return nil, err
	}
	userEmail, err := shared.NonEmptyFromEnvVar(models.ECSTaskUserEmailKey)
	if err != nil {
		return nil, err
	}
	return &models.User{
		Name:  userName,
		Email: userEmail,
	}, nil
}
