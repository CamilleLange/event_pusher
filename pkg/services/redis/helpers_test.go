package redis

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
)

// testBatchSize is small so that tests span several batches with few messages.
const testBatchSize = 10

// newTestClient connects a client to a fresh in-memory Redis.
func newTestClient(t *testing.T) *Client {
	t.Helper()

	srv := miniredis.RunT(t)

	client, err := New(&Config{Addr: srv.Addr(), BatchSize: testBatchSize})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Cleanup(func() { _ = client.Close() })
	return client
}
