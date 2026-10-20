package eventpusher

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestApp_Run_StopsOnContextCancel(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	app, err := New(DefaultConfig(), logger)
	if err != nil {
		t.Fatalf("app creation shouldn't fail : %v", err)
	}

	if err := app.Run(ctx); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	for _, msg := range []string{"service started", "service stopped"} {
		if !strings.Contains(buf.String(), msg) {
			t.Errorf("log %q missing in %q", msg, buf.String())
		}
	}
}
