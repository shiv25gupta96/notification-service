package s3

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config holds S3 connection parameters.
type Config struct {
	// Bucket is the S3 bucket name where template bodies are stored.
	Bucket string

	// Region overrides the default AWS region. If empty the SDK resolves it
	// from the environment (AWS_DEFAULT_REGION, instance metadata, etc.).
	Region string

	// EndpointURL allows overriding the S3 endpoint for local development
	// (e.g., LocalStack). Leave empty in production.
	EndpointURL string
}

// FromEnv constructs a Config from environment variables:
//
//	S3_BUCKET          required
//	AWS_REGION         optional, default resolved by SDK
//	S3_ENDPOINT_URL    optional, used for local dev / integration tests
func FromEnv() (Config, error) {
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		return Config{}, fmt.Errorf("s3 config: S3_BUCKET must not be empty")
	}
	return Config{
		Bucket:      bucket,
		Region:      os.Getenv("AWS_REGION"),
		EndpointURL: os.Getenv("S3_ENDPOINT_URL"),
	}, nil
}

// NewClient creates an AWS S3 client using the SDK's default credential chain
// (IAM role, environment variables, ~/.aws/credentials).
// EndpointURL is honoured for LocalStack / integration test overrides.
func NewClient(cfg Config) (*s3.Client, error) {
	loadOpts := []func(*config.LoadOptions) error{}
	if cfg.Region != "" {
		loadOpts = append(loadOpts, config.WithRegion(cfg.Region))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3 client: load AWS config: %w", err)
	}

	clientOpts := []func(*s3.Options){}
	if cfg.EndpointURL != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = &cfg.EndpointURL
			// Force path-style addressing for LocalStack compatibility.
			o.UsePathStyle = true
		})
	}

	return s3.NewFromConfig(awsCfg, clientOpts...), nil
}
