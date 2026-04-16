package template

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// BodyStore abstracts the object store that holds immutable template HTML bodies.
// Each version is stored at a deterministic key and never mutated after upload.
type BodyStore interface {
	// Put uploads body to key with the given content type. Uploads are
	// idempotent: re-uploading the same key with identical content is safe.
	Put(ctx context.Context, key string, body []byte, contentType string) error

	// Get downloads and returns the body at key.
	// Returns ErrNotFound when the key does not exist.
	Get(ctx context.Context, key string) ([]byte, error)
}

// s3API is the minimal subset of the AWS S3 client interface used here.
// Defined at point-of-use so tests can inject a stub without importing the full SDK.
type s3API interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3BodyStore implements BodyStore using AWS S3.
// All objects are stored in a single bucket; keys are prefixed by the service.
type S3BodyStore struct {
	client s3API
	bucket string
}

// NewS3BodyStore creates a store that writes to bucket using client.
func NewS3BodyStore(client s3API, bucket string) *S3BodyStore {
	return &S3BodyStore{client: client, bucket: bucket}
}

// Put uploads body to S3 at key. The upload sets the object's Content-Type
// and marks it as immutable via Cache-Control so CloudFront/CDN caches
// the body indefinitely (templates are never mutated — only new versions created).
func (s *S3BodyStore) Put(ctx context.Context, key string, body []byte, contentType string) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(body),
		ContentType:  aws.String(contentType),
		CacheControl: aws.String("public, max-age=31536000, immutable"),
	})
	if err != nil {
		return fmt.Errorf("s3 put %q: %w", key, err)
	}
	return nil
}

// Get downloads the body at key. Returns ErrNotFound for a 404 response.
func (s *S3BodyStore) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// The AWS SDK v2 wraps 404s in *smithy.OperationError. We surface
		// ErrNotFound for the common case so callers can respond with 404.
		if isS3NotFound(err) {
			return nil, fmt.Errorf("%w: s3://%s/%s", ErrNotFound, s.bucket, key)
		}
		return nil, fmt.Errorf("s3 get %q: %w", key, err)
	}
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3 read body %q: %w", key, err)
	}
	return body, nil
}

// isS3NotFound reports whether err is an S3 NoSuchKey (404) error.
// In AWS SDK v2 the error type lives in the service's types sub-package.
func isS3NotFound(err error) bool {
	var noSuchKey *types.NoSuchKey
	return errors.As(err, &noSuchKey)
}
