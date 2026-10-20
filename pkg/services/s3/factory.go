package s3

import (
	"context"
	"fmt"
)

// New builds the object storage client selected by c.ClientType.
func New(c *Config) (Client, error) {
	if c.Bucket == "" {
		return nil, ErrEmptyBucket
	}

	ctx := context.Background()
	switch c.ClientType {
	case ClientTypeS3:
		return newMinioClient(ctx, c)
	case ClientTypeGCS:
		return newGCSClient(ctx, c.Bucket)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownClientType, c.ClientType)
	}
}
