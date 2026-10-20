// Package confighelper holds the shared plumbing of the configuration system:
// flag namespacing and the single viper loading entry point.
package confighelper

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// EnvPrefix prefixes every environment variable read by the configuration
// (e.g. EVENT_PUSHER_NATS_URL for the Nats.URL key).
const EnvPrefix = "EVENT_PUSHER"

// PrefixFlagSet returns a copy of fs with every flag renamed to
// "<prefix>.<name>", so that the flag tree mirrors the Config struct tree.
// Shorthands are dropped: they would collide once composed.
func PrefixFlagSet(fs *pflag.FlagSet, prefix string) *pflag.FlagSet {
	prefixed := pflag.NewFlagSet(prefix, pflag.ContinueOnError)
	fs.VisitAll(func(f *pflag.Flag) {
		clone := *f
		clone.Name = prefix + "." + f.Name
		clone.Shorthand = ""
		prefixed.AddFlag(&clone)
	})
	return prefixed
}

// NewViper builds a viper instance reading, by decreasing priority, the CLI
// flags, the EVENT_PUSHER_* environment variables, the optional config file at
// configPath and the flag defaults.
func NewViper(flags *pflag.FlagSet, configPath string) (*viper.Viper, error) {
	v := viper.New()
	if err := v.BindPFlags(flags); err != nil {
		return nil, fmt.Errorf("bind flags: %w", err)
	}

	v.SetEnvPrefix(EnvPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	if configPath == "" {
		return v, nil
	}

	v.SetConfigFile(configPath)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config file %s: %w", configPath, err)
	}
	return v, nil
}

// ConfigFileFlag is the root persistent flag holding the optional config file path.
const ConfigFileFlag = "config"
