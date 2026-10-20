package eventpusher

import (
	"fmt"
	"log/slog"
	"test/event_pusher/pkg/services/nats"
	"test/event_pusher/pkg/services/redis"
	"test/event_pusher/pkg/services/s3"
)

// New builds the event pusher application.
func New(cfg *Config, logger *slog.Logger) (*App, error) {
	nc, err := nats.New(&cfg.Nats)
	if err != nil {
		return nil, fmt.Errorf("can't create nats client : %w", err)
	}

	rc, err := redis.New(&cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("can't create redis client : %w", err)
	}

	sc, err := s3.New(&cfg.S3)
	if err != nil {
		return nil, fmt.Errorf("can't create s3 client : %w", err)
	}

	return &App{
		cfg:         cfg,
		logger:      logger,
		natsClient:  nc,
		redisClient: rc,
		s3Client:    sc,
	}, nil
}
