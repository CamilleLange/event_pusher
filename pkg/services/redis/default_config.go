package redis

// DefaultConfig targets the Redis of the local docker compose.
func DefaultConfig() *Config {
	return &Config{
		Addr:      "localhost:6379",
		BatchSize: 1000,
	}
}
