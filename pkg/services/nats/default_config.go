package nats

// DefaultConfig targets the NATS of the local docker compose.
func DefaultConfig() *Config {
	return &Config{
		URL: "nats://localhost:4222",
	}
}
