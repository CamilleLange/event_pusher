package redis

import (
	"context"
	"fmt"

	r "github.com/go-redis/redis/v8"
)

// New builds the Redis client and checks that the server is reachable.
// It fails if c.BatchSize is not positive.
func New(c *Config) (*Client, error) {
	if c.BatchSize <= 0 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidBatchSize, c.BatchSize)
	}

	client := r.NewClient(&r.Options{
		Addr:     c.Addr,
		Password: c.Password,
		DB:       c.DB,
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("can't connect to Redis : %w", err)
	}

	return &Client{client: client, batchSize: c.BatchSize}, nil
}
