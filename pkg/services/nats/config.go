// Package nats holds the configuration of the NATS (JetStream) connection.
package nats

import "github.com/spf13/pflag"

// Config is the NATS connection configuration.
type Config struct {
	URL string `mapstructure:"URL"`
}

// FlagSet declares the NATS flags, defaulting to defaultConfig.
func FlagSet(defaultConfig Config) *pflag.FlagSet {
	fs := pflag.NewFlagSet("nats", pflag.ContinueOnError)
	fs.String("URL", defaultConfig.URL, "NATS server URL")
	return fs
}
