package producer

import (
	"test/event_pusher/pkg/services/nats"
	"time"
)

// DefaultConfig targets the NATS of the local docker compose.
func DefaultConfig() *Config {
	return &Config{
		LogLevel:     "info",
		Nats:         *nats.DefaultConfig(),
		TickInterval: 10 * time.Millisecond,
	}
}
