// Package eventpusher holds the configuration of the `service` command.
package eventpusher

import (
	"fmt"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"test/event_pusher/internal/config/confighelper"
	"test/event_pusher/internal/telemetry"
	"test/event_pusher/pkg/services/nats"
	"test/event_pusher/pkg/services/redis"
	"test/event_pusher/pkg/services/s3"
)

// Config is the configuration of the `service` command.
type Config struct {
	LogLevel         string           `mapstructure:"LogLevel"`
	Telemetry        telemetry.Config `mapstructure:"Telemetry"`
	RedisStreamLimit int              `mapstructure:"RedisStreamLimit"`

	Nats  nats.Config  `mapstructure:"Nats"`
	Redis redis.Config `mapstructure:"Redis"`
	S3    s3.Config    `mapstructure:"S3"`
}

// FlagSet declares the `service` flags, defaulting to defaultConfig.
func FlagSet(defaultConfig Config) *pflag.FlagSet {
	fs := pflag.NewFlagSet("service", pflag.ContinueOnError)

	fs.String("LogLevel", defaultConfig.LogLevel, "log level (debug, info, warn, error)")

	fs.AddFlagSet(confighelper.PrefixFlagSet(telemetry.FlagSet(defaultConfig.Telemetry), "Telemetry"))

	fs.Int("RedisStreamLimit", defaultConfig.RedisStreamLimit, "the limit of each redis stream (also the number of lines of each file in the S3)")

	fs.AddFlagSet(confighelper.PrefixFlagSet(nats.FlagSet(defaultConfig.Nats), "Nats"))
	fs.AddFlagSet(confighelper.PrefixFlagSet(redis.FlagSet(defaultConfig.Redis), "Redis"))
	fs.AddFlagSet(confighelper.PrefixFlagSet(s3.FlagSet(defaultConfig.S3), "S3"))

	return fs
}

// FromViper decodes the `service` configuration from v.
func FromViper(v *viper.Viper) (*Config, error) {
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("decode service config: %w", err)
	}
	return &cfg, nil
}
