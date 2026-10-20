package s3

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// gcsClient writes to a bucket through the native GCS API.
type gcsClient struct {
	bucket string
	client *storage.Client
}

// newGCSClient builds a GCS client authenticated with the Google application
// default credentials; opts are added after them (the tests use it to target an emulator).
func newGCSClient(ctx context.Context, bucket string, opts ...option.ClientOption) (*gcsClient, error) {
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("can't create GCS client : %w", err)
	}

	return &gcsClient{
		bucket: bucket,
		client: client,
	}, nil
}

// WriteFile stores data under name in the bucket.
func (c *gcsClient) WriteFile(ctx context.Context, name string, data []byte) error {
	writer := c.client.Bucket(c.bucket).Object(name).NewWriter(ctx)

	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return fmt.Errorf("can't write %q to GCS : %w", name, err)
	}

	// The upload is only completed, and its errors reported, on Close.
	if err := writer.Close(); err != nil {
		return fmt.Errorf("can't complete upload of %q to GCS : %w", name, err)
	}
	return nil
}

// Close closes the client.
func (c *gcsClient) Close() error {
	return c.client.Close()
}
