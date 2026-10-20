package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	r "github.com/go-redis/redis/v8"
)

func TestRedisClient_InsertCount(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		insert func(c *Client) error
		want   int
	}{
		{name: "nothing inserted", insert: func(*Client) error { return nil }, want: 0},
		{name: "single messages", insert: func(c *Client) error {
			if err := c.Insert(ctx, "events", []byte("one")); err != nil {
				return err
			}
			return c.Insert(ctx, "events", []byte("two"))
		}, want: 2},
		{name: "batch", insert: func(c *Client) error {
			return c.InsertBatch(ctx, "events", []json.RawMessage{[]byte(`{"a"}`), []byte(`{"b"}`), []byte(`{"c"}`)})
		}, want: 3},
		{name: "empty batch", insert: func(c *Client) error { return c.InsertBatch(ctx, "events", nil) }, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t)

			if err := tt.insert(c); err != nil {
				t.Fatal(err)
			}

			got, err := c.Count(ctx, "events")
			if err != nil {
				t.Fatal(err)
			}

			if got != tt.want {
				t.Errorf("Count() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestRedisClient_Create(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(t)

	if err := c.Create(ctx, "events"); err != nil {
		t.Fatalf("Create() = %v", err)
	}

	exists, err := c.client.Exists(ctx, "events").Result()
	if err != nil {
		t.Fatal(err)
	}

	if exists != 1 {
		t.Error("stream does not exist after Create")
	}

	if n, _ := c.Count(ctx, "events"); n != 0 {
		t.Errorf("Count() = %d, want an empty stream", n)
	}

	// Messages can be inserted afterwards, and Create keeps them.
	if err := c.Insert(ctx, "events", []byte("one")); err != nil {
		t.Fatal(err)
	}

	if err := c.Create(ctx, "events"); err != nil {
		t.Fatalf("second Create() = %v", err)
	}

	if n, _ := c.Count(ctx, "events"); n != 1 {
		t.Errorf("Count() = %d after second Create, want 1", n)
	}
}

func TestRedisClient_Empty(t *testing.T) {
	ctx := context.Background()

	// More than one read batch and one delete chunk, to cover the loops.
	var many []json.RawMessage
	var wantMany []string
	for i := range testBatchSize*2 + 5 {
		m := fmt.Sprintf("message-%d", i)
		many = append(many, []byte(m))
		wantMany = append(wantMany, m)
	}

	tests := []struct {
		name  string
		batch []json.RawMessage
		want  []string
	}{
		{name: "missing stream"},
		{name: "ordered messages", batch: []json.RawMessage{[]byte("one"), []byte("two"), []byte("three")}, want: []string{"one", "two", "three"}},
		{name: "several read batches", batch: many, want: wantMany},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t)

			if err := c.InsertBatch(ctx, "events", tt.batch); err != nil {
				t.Fatal(err)
			}

			messages, err := c.Empty(ctx, "events")
			if err != nil {
				t.Fatalf("Empty() = %v", err)
			}

			got := make([]string, 0, len(messages))
			for _, m := range messages {
				got = append(got, string(m))
			}

			if !slices.Equal(got, tt.want) {
				t.Errorf("messages = %v, want %v", got, tt.want)
			}

			if n, _ := c.Count(ctx, "events"); n != 0 {
				t.Errorf("Count() = %d after Empty, want 0", n)
			}
		})
	}
}

func TestRedisClient_Empty_Errors(t *testing.T) {
	t.Run("context cancelled keeps the entries", func(t *testing.T) {
		c := newTestClient(t)

		if err := c.Insert(context.Background(), "events", []byte("one")); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		messages, err := c.Empty(ctx, "events")
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Empty() = %v, want context.Canceled", err)
		}

		if messages != nil {
			t.Errorf("messages = %v, want nil", messages)
		}

		if n, _ := c.Count(context.Background(), "events"); n != 1 {
			t.Errorf("Count() = %d, want the entry kept", n)
		}
	})

	t.Run("entry without payload deletes nothing", func(t *testing.T) {
		c := newTestClient(t)
		ctx := context.Background()

		if err := c.Insert(ctx, "events", []byte("valid")); err != nil {
			t.Fatal(err)
		}

		if err := c.client.XAdd(ctx, &r.XAddArgs{Stream: "events", Values: map[string]any{"other": "x"}}).Err(); err != nil {
			t.Fatal(err)
		}

		messages, err := c.Empty(ctx, "events")
		if !errors.Is(err, ErrMissingPayload) {
			t.Errorf("Empty() = %v, want ErrMissingPayload", err)
		}

		if messages != nil {
			t.Errorf("messages = %v, want nil", messages)
		}

		if n, _ := c.Count(ctx, "events"); n != 2 {
			t.Errorf("Count() = %d, want both entries kept", n)
		}
	})
}

func TestRedisClient_Errors_WhenServerIsDown(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(t)
	_ = c.Close()

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "Count", run: func() error { _, err := c.Count(ctx, "s"); return err }},
		{name: "Create", run: func() error { return c.Create(ctx, "s") }},
		{name: "Insert", run: func() error { return c.Insert(ctx, "s", []byte("x")) }},
		{name: "InsertBatch", run: func() error { return c.InsertBatch(ctx, "s", []json.RawMessage{[]byte("x")}) }},
		{name: "Empty", run: func() error { _, err := c.Empty(ctx, "s"); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(); err == nil {
				t.Error("expected an error")
			}
		})
	}
}
