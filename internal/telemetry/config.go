package telemetry

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Exporter identifies which OTel exporter to use.
type Exporter string

const (
	ExporterStdout Exporter = "stdout"
	ExporterOTLP   Exporter = "otlp"
)

// Config holds the runtime telemetry configuration.
// Defined at the root level (not under a service) because tracing applies to all services.
type Config struct {
	Enabled       bool     `mapstructure:"enabled"`
	Exporter      Exporter `mapstructure:"exporter"`
	Endpoint      string   `mapstructure:"endpoint"`
	SamplingRatio float64  `mapstructure:"sampling_ratio"`
}

// FlagSet declares the `producer` flags, defaulting to defaultConfig.
func FlagSet(defaultConfig Config) *pflag.FlagSet {
	fs := pflag.NewFlagSet("telemetry", pflag.ContinueOnError)

	fs.Bool("Enabled", false, "Enable telemetry.")
	fs.String("Exporter", string(ExporterStdout), "Choose wich exporter to use. Allowed values are 'stdout' and 'otlp'")
	fs.String("Endpoint", "", "Endpoint where to send the telemetry data, only used in case of otlp exporter.")
	fs.Float64("SamplingRatio", 1, "Controls the sampling ratio of telemetry data to send, only used in cas of otlp exporter.")

	return fs
}

// FromViper reads the telemetry config from viper (key: "telemetry").
func FromViper() Config {
	var root struct {
		Telemetry Config `mapstructure:"telemetry"`
	}
	_ = viper.Unmarshal(&root)
	cfg := root.Telemetry
	return cfg
}
