package s3

import (
	"bytes"
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// minioClient writes to a bucket of an S3-compatible server (MinIO locally).
type minioClient struct {
	bucket string
	client *minio.Client
}

// newMinioClient connects to c.Endpoint and creates the bucket if it does not exist yet.
func newMinioClient(ctx context.Context, c *Config) (*minioClient, error) {
	client, err := minio.New(c.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""),
		Secure: c.UseSSL,
		Region: c.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("can't create S3 client : %w", err)
	}

	mc := &minioClient{
		bucket: c.Bucket,
		client: client,
	}

	if err := mc.ensureBucket(ctx, c.Region); err != nil {
		return nil, err
	}
	return mc, nil
}

// ensureBucket creates the bucket when missing, which also proves the server is reachable.
func (c *minioClient) ensureBucket(ctx context.Context, region string) error {
	exists, err := c.client.BucketExists(ctx, c.bucket)
	if err != nil {
		return fmt.Errorf("can't check S3 bucket %q : %w", c.bucket, err)
	}

	if exists {
		return nil
	}

	if err := c.client.MakeBucket(ctx, c.bucket, minio.MakeBucketOptions{Region: region}); err != nil {
		return fmt.Errorf("can't create S3 bucket %q : %w", c.bucket, err)
	}
	return nil
}

// WriteFile stores data under name in the bucket.
func (c *minioClient) WriteFile(ctx context.Context, name string, data []byte) error {
	_, err := c.client.PutObject(ctx, c.bucket, name, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("can't write %q to S3 : %w", name, err)
	}
	return nil
}

// Close does nothing: the S3 client holds no resource to release.
func (c *minioClient) Close() error {
	return nil
}
