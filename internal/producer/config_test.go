package producer

import (
	"testing"

	"test/event_pusher/internal/config/confighelper"
)

func TestFromViper(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    func(*Config)
		wantErr bool
	}{
		{name: "defaults", want: func(*Config) {}},
		{
			name: "nested flags",
			args: []string{"--Nats.URL=nats://nats:4222", "--LogLevel=debug"},
			want: func(c *Config) {
				c.Nats.URL = "nats://nats:4222"
				c.LogLevel = "debug"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := FlagSet(*DefaultConfig())
			if err := fs.Parse(tt.args); err != nil {
				t.Fatal(err)
			}

			v, err := confighelper.NewViper(fs, "")
			if err != nil {
				t.Fatal(err)
			}

			got, err := FromViper(v)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}

			want := DefaultConfig()
			tt.want(want)
			if *got != *want {
				t.Errorf("config = %+v, want %+v", *got, *want)
			}
		})
	}
}
