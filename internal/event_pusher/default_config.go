package eventpusher

import (
	"test/event_pusher/internal/telemetry"
	"test/event_pusher/pkg/services/nats"
	"test/event_pusher/pkg/services/redis"
	"test/event_pusher/pkg/services/s3"
)

// DefaultConfig targets the infrastructure of the local docker compose.
func DefaultConfig() *Config {
	return &Config{
		LogLevel: "info",
		Telemetry: telemetry.Config{
			Enabled:  true,
			Exporter: telemetry.ExporterOTLP,
			Endpoint: "http://jaeger:4318",
		},
		RedisStreamLimit: 1000,
		Nats:             *nats.DefaultConfig(),
		Redis:            *redis.DefaultConfig(),
		S3:               *s3.DefaultConfig(),
	}
}
