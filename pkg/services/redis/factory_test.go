package redis

import (
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name      string
		addr      func(t *testing.T) string
		batchSize int
		wantErr   error
		wantAny   bool
	}{
		{name: "reachable server", addr: func(t *testing.T) string { return miniredis.RunT(t).Addr() }, batchSize: 1000},
		{name: "unreachable server", addr: func(*testing.T) string { return "127.0.0.1:1" }, batchSize: 1000, wantAny: true},
		{name: "zero batch size", addr: func(t *testing.T) string { return miniredis.RunT(t).Addr() }, batchSize: 0, wantErr: ErrInvalidBatchSize},
		{name: "negative batch size", addr: func(t *testing.T) string { return miniredis.RunT(t).Addr() }, batchSize: -1, wantErr: ErrInvalidBatchSize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := New(&Config{Addr: tt.addr(t), BatchSize: tt.batchSize})

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}

			if (err != nil) != tt.wantAny {
				t.Fatalf("err = %v, wantAny %v", err, tt.wantAny)
			}

			if tt.wantAny {
				return
			}

			if err := client.Close(); err != nil {
				t.Errorf("Close() = %v", err)
			}
		})
	}
}
