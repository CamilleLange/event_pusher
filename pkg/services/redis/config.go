// Package redis holds the configuration of the Redis (streams) connection.
package redis

import "github.com/spf13/pflag"

// Config is the Redis connection configuration.
type Config struct {
	Addr     string `mapstructure:"Addr"`
	Password string `mapstructure:"Password"`
	DB       int    `mapstructure:"DB"`

	// BatchSize is the number of entries Empty reads or deletes per command.
	BatchSize int `mapstructure:"BatchSize"`
}

// FlagSet declares the Redis flags, defaulting to defaultConfig.
func FlagSet(defaultConfig Config) *pflag.FlagSet {
	fs := pflag.NewFlagSet("redis", pflag.ContinueOnError)
	fs.String("Addr", defaultConfig.Addr, "Redis address (host:port)")
	fs.String("Password", defaultConfig.Password, "Redis password")
	fs.Int("DB", defaultConfig.DB, "Redis database index")
	fs.Int("BatchSize", defaultConfig.BatchSize, "entries read or deleted per command when emptying a stream")
	return fs
}
