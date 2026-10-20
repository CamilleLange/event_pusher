package s3

// DefaultConfig targets the MinIO of the local docker compose. Credentials are
// left empty on purpose: they come from the environment or the config file.
func DefaultConfig() *Config {
	return &Config{
		ClientType: ClientTypeS3,
		Bucket:     "events",
		Endpoint:   "localhost:9000",
		Region:     "us-east-1",
	}
}
