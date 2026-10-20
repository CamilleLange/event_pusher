// Package producer holds the configuration of the `producer` command.
package producer

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"test/event_pusher/internal/config/confighelper"
	"test/event_pusher/pkg/services/nats"
)

// Config is the configuration of the `producer` command.
type Config struct {
	LogLevel     string        `mapstructure:"LogLevel"`
	TickInterval time.Duration `mapstructure:"TickInterval"`

	Nats nats.Config `mapstructure:"Nats"`
}

// FlagSet declares the `producer` flags, defaulting to defaultConfig.
func FlagSet(defaultConfig Config) *pflag.FlagSet {
	fs := pflag.NewFlagSet("producer", pflag.ContinueOnError)

	fs.String("LogLevel", defaultConfig.LogLevel, "log level (debug, info, warn, error)")
	fs.Duration("TickInterval", defaultConfig.TickInterval, "duration between two ticks")

	fs.AddFlagSet(confighelper.PrefixFlagSet(nats.FlagSet(defaultConfig.Nats), "Nats"))

	return fs
}

// FromViper decodes the `producer` configuration from v.
func FromViper(v *viper.Viper) (*Config, error) {
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("decode producer config: %w", err)
	}
	return &cfg, nil
}
