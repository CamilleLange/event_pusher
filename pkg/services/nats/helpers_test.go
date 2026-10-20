package nats

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// startServer runs an in-memory NATS server with JetStream and returns its client URL.
func startServer(t *testing.T) string {
	t.Helper()

	srv, err := server.NewServer(&server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}

	go srv.Start()
	t.Cleanup(srv.Shutdown)

	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready")
	}
	return srv.ClientURL()
}

// newTestClient connects a client to a fresh server.
func newTestClient(t *testing.T) *Client {
	t.Helper()

	client, err := New(&Config{URL: startServer(t)})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Cleanup(func() { _ = client.Close() })
	return client
}
