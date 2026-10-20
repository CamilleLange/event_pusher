package confighelper

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
)

func TestPrefixFlagSet(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.StringP("URL", "u", "nats://default", "url")

	prefixed := PrefixFlagSet(fs, "Nats")

	f := prefixed.Lookup("Nats.URL")
	if f == nil {
		t.Fatal("flag Nats.URL not found")
	}

	if f.DefValue != "nats://default" {
		t.Errorf("default = %q, want %q", f.DefValue, "nats://default")
	}

	if f.Shorthand != "" {
		t.Errorf("shorthand = %q, want none", f.Shorthand)
	}

	if prefixed.Lookup("URL") != nil {
		t.Error("unprefixed flag URL must not be kept")
	}
}

func TestNewViper(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configFile, []byte("Nats:\n  URL: nats://file\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		args       []string
		env        string
		configPath string
		want       string
		wantErr    bool
	}{
		{name: "flag default", want: "nats://default"},
		{name: "config file overrides default", configPath: configFile, want: "nats://file"},
		{name: "env overrides config file", env: "nats://env", configPath: configFile, want: "nats://env"},
		{name: "flag overrides env", args: []string{"--Nats.URL=nats://flag"}, env: "nats://env", configPath: configFile, want: "nats://flag"},
		{name: "missing config file", configPath: filepath.Join(t.TempDir(), "missing.yaml"), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.env != "" {
				t.Setenv(EnvPrefix+"_NATS_URL", tt.env)
			}

			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			fs.String("Nats.URL", "nats://default", "url")
			if err := fs.Parse(tt.args); err != nil {
				t.Fatal(err)
			}

			v, err := NewViper(fs, tt.configPath)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if got := v.GetString("Nats.URL"); got != tt.want {
				t.Errorf("Nats.URL = %q, want %q", got, tt.want)
			}
		})
	}
}
