package producer

import (
	"bytes"
	"context"
	"testing"

	"test/event_pusher/internal/config/confighelper"
)

func TestNewCommand(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{name: "runs until context is cancelled"},
		{name: "invalid log level", args: []string{"--LogLevel=verbose"}, wantErr: true},
		{name: "missing config file", args: []string{"--config=/does/not/exist.yaml"}, wantErr: true},
		{name: "unexpected argument", args: []string{"extra"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewCommand()
			cmd.Flags().String(confighelper.ConfigFileFlag, "", "")
			cmd.SetArgs(tt.args)
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err := cmd.ExecuteContext(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
