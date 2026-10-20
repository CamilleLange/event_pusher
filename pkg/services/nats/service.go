package nats

import (
	"context"
	"fmt"
	"time"

	n "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// nakDelay is how long NATS waits before redelivering a message whose handler failed.
const nakDelay = time.Second

// Handler processes one message received by Listen. Returning an error asks
// NATS to redeliver the message after a short delay.
type Handler func(ctx context.Context, subject string, data []byte) error

// Client is a thin wrapper over a NATS connection and its JetStream context.
type Client struct {
	conn *n.Conn
	js   jetstream.JetStream
}

// EnsureStream creates the JetStream stream name capturing subjects, or updates
// it when it already exists. A subject must belong to a stream to be published
// to or listened on.
func (c *Client) EnsureStream(ctx context.Context, name string, subjects ...string) error {
	cfg := jetstream.StreamConfig{
		Name:     name,
		Subjects: subjects,
	}

	if _, err := c.js.CreateOrUpdateStream(ctx, cfg); err != nil {
		return fmt.Errorf("ensure stream %q: %w", name, err)
	}
	return nil
}

// Publish stores data in the stream capturing subject and waits for its acknowledgement.
func (c *Client) Publish(ctx context.Context, subject string, data []byte) error {
	if _, err := c.js.Publish(ctx, subject, data); err != nil {
		return fmt.Errorf("publish on %q: %w", subject, err)
	}
	return nil
}

// LastMessage find the latest message from the given subject.
func (c *Client) LastMessage(ctx context.Context, subject string) ([]byte, error) {
	streamName, err := c.js.StreamNameBySubject(ctx, subject)
	if err != nil {
		return nil, fmt.Errorf("find stream of %q: %w", subject, err)
	}

	stream, err := c.js.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("error finding stream %s: %v", streamName, err)
	}

	msg, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return nil, fmt.Errorf("error retrieving direct message: %v", err)
	}

	return msg.Data, nil
}

// Listen follows subject and calls handler for every message published on it
// after the call. It blocks until ctx is cancelled and returns nil on a clean stop.
//
// The consumer is ephemeral: it is dropped when Listen returns, so messages
// published while nobody listens are not replayed. A message is acknowledged
// when handler returns nil, redelivered otherwise.
func (c *Client) Listen(ctx context.Context, subject string, handler Handler) error {
	if handler == nil {
		return ErrNilHandler
	}

	streamName, err := c.js.StreamNameBySubject(ctx, subject)
	if err != nil {
		return fmt.Errorf("find stream of %q: %w", subject, err)
	}

	cfg := jetstream.ConsumerConfig{
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
	}
	consumer, err := c.js.CreateConsumer(ctx, streamName, cfg)
	if err != nil {
		return fmt.Errorf("create consumer on stream %q: %w", streamName, err)
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		dispatch(ctx, handler, msg)
	})
	if err != nil {
		return fmt.Errorf("consume stream %q: %w", streamName, err)
	}
	defer consumeCtx.Stop()

	<-ctx.Done()
	return nil
}

// Close drains the connection: pending messages are flushed, then it closes.
func (c *Client) Close() error {
	return c.conn.Drain()
}

// dispatch hands msg to handler, then acks or naks it. An ack/nak that fails to
// reach the server is not fatal: NATS redelivers the message once its ack wait expires.
func dispatch(ctx context.Context, handler Handler, msg jetstream.Msg) {
	if err := handler(ctx, msg.Subject(), msg.Data()); err != nil {
		_ = msg.NakWithDelay(nakDelay)
		return
	}
	_ = msg.Ack()
}
