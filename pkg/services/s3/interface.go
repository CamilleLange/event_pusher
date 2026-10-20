package s3

import "context"

// Ensure the implementations satisfy the interface at compilation time.
var (
	_ Client = (*gcsClient)(nil)
	_ Client = (*minioClient)(nil)
)

// Client is the interface of all object storage implementations.
type Client interface {
	// WriteFile stores data under name in the configured bucket, replacing any existing object.
	WriteFile(ctx context.Context, name string, data []byte) error

	// Close releases the client resources.
	Close() error
}
