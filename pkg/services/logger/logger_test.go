package logger

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name      string
		level     string
		wantDebug bool
		wantInfo  bool
		wantErr   bool
	}{
		{name: "debug", level: "debug", wantDebug: true, wantInfo: true},
		{name: "info upper case", level: "INFO", wantInfo: true},
		{name: "error", level: "error"},
		{name: "invalid", level: "verbose", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			l, err := New(&buf, tt.level)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			ctx := context.Background()
			if got := l.Enabled(ctx, slog.LevelDebug); got != tt.wantDebug {
				t.Errorf("debug enabled = %v, want %v", got, tt.wantDebug)
			}

			if got := l.Enabled(ctx, slog.LevelInfo); got != tt.wantInfo {
				t.Errorf("info enabled = %v, want %v", got, tt.wantInfo)
			}
		})
	}
}
