package nats

import "testing"

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		url     func(t *testing.T) string
		wantErr bool
	}{
		{name: "reachable server", url: startServer},
		{name: "unreachable server", url: func(*testing.T) string { return "nats://127.0.0.1:1" }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := New(&Config{URL: tt.url(t)})
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.wantErr {
				return
			}

			if err := client.Close(); err != nil {
				t.Errorf("Close() = %v", err)
			}
		})
	}
}
