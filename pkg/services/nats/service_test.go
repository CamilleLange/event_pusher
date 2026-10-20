package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

type received struct {
	subject string
	data    string
}

// listenAsync runs Listen in the background and waits until its consumer is
// ready; the returned func stops it and returns Listen's result.
func listenAsync(t *testing.T, c *Client, subject string, handler Handler) func() error {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Listen(ctx, subject, handler) }()

	// Listen only sees messages published after its consumer exists: wait for it.
	time.Sleep(200 * time.Millisecond)

	return func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(testTimeout):
			t.Fatal("Listen did not return after cancel")
			return nil
		}
	}
}

func TestNatsClient_PublishListen(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	if err := c.EnsureStream(ctx, "EVENTS", "events.>"); err != nil {
		t.Fatal(err)
	}

	got := make(chan received, 10)
	stop := listenAsync(t, c, "events.click", func(_ context.Context, subject string, data []byte) error {
		got <- received{subject: subject, data: string(data)}
		return nil
	})

	for _, payload := range []string{"one", "two"} {
		if err := c.Publish(ctx, "events.click", []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}

	// Published on the same stream but not on the followed subject.
	if err := c.Publish(ctx, "events.view", []byte("ignored")); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"one", "two"} {
		select {
		case msg := <-got:
			if msg.subject != "events.click" || msg.data != want {
				t.Errorf("got %+v, want events.click/%s", msg, want)
			}
		case <-time.After(testTimeout):
			t.Fatalf("message %q not received", want)
		}
	}

	if err := stop(); err != nil {
		t.Errorf("Listen() = %v, want nil", err)
	}

	select {
	case msg := <-got:
		t.Errorf("unexpected message %+v", msg)
	default:
	}
}

func TestNatsClient_Listen_RedeliversOnHandlerError(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	if err := c.EnsureStream(ctx, "EVENTS", "events.>"); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int32
	delivered := make(chan struct{})
	stop := listenAsync(t, c, "events.retry", func(context.Context, string, []byte) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient failure")
		}

		close(delivered)
		return nil
	})
	defer func() { _ = stop() }()

	if err := c.Publish(ctx, "events.retry", []byte("payload")); err != nil {
		t.Fatal(err)
	}

	select {
	case <-delivered:
	case <-time.After(testTimeout):
		t.Fatalf("message not redelivered after handler error (attempts: %d)", attempts.Load())
	}
}

func TestNatsClient_Errors(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	if err := c.EnsureStream(ctx, "EVENTS", "events.>"); err != nil {
		t.Fatal(err)
	}

	noop := func(context.Context, string, []byte) error { return nil }
	tests := []struct {
		name string
		run  func() error
		want error
	}{
		{
			name: "listen without handler",
			run:  func() error { return c.Listen(ctx, "events.x", nil) },
			want: ErrNilHandler,
		},
		{
			name: "listen on a subject without stream",
			run:  func() error { return c.Listen(ctx, "nostream.x", noop) },
		},
		{
			name: "publish on a subject without stream",
			run:  func() error { return c.Publish(ctx, "nostream.x", []byte("x")) },
		},
		{
			name: "stream without subjects",
			run:  func() error { return c.EnsureStream(ctx, "BAD", "") },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if err == nil {
				t.Fatal("expected an error")
			}

			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Errorf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestNatsClient_EnsureStream_Idempotent(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	for range 2 {
		if err := c.EnsureStream(ctx, "EVENTS", "events.>"); err != nil {
			t.Fatalf("EnsureStream() = %v", err)
		}
	}
}
