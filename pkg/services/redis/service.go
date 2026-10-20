package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	r "github.com/go-redis/redis/v8"
)

const (
	// payloadField is the entry field holding the message bytes.
	payloadField = "data"

	// createEntryID is the throwaway entry used by Create to materialize an empty stream.
	createEntryID = "0-1"
)

// Client is a thin wrapper over a Redis connection, exposing streams as
// queues of raw messages.
type Client struct {
	client    *r.Client
	batchSize int
}

// Count returns the number of entries in stream, -1 if it does not exist.
func (c *Client) Count(ctx context.Context, stream string) (int, error) {
	n, err := c.client.XLen(ctx, stream).Result()
	if err != nil {
		return -1, fmt.Errorf("count stream %q: %w", stream, err)
	}
	return int(n), nil
}

// Create makes stream exist, empty. It does nothing if the stream already
// exists. Redis creates streams on first insert anyway: this is only useful to
// have an empty one beforehand.
func (c *Client) Create(ctx context.Context, stream string) error {
	exists, err := c.client.Exists(ctx, stream).Result()
	if err != nil {
		return fmt.Errorf("create stream %q: %w", stream, err)
	}

	if exists > 0 {
		return nil
	}

	// A stream outlives the deletion of its last entry: add one and remove it.
	_, err = c.client.TxPipelined(ctx, func(p r.Pipeliner) error {
		p.XAdd(ctx, &r.XAddArgs{Stream: stream, ID: createEntryID, Values: payloadValues(nil)})
		p.XDel(ctx, stream, createEntryID)
		return nil
	})
	if err != nil {
		return fmt.Errorf("create stream %q: %w", stream, err)
	}
	return nil
}

// Insert appends message to stream, creating the stream if needed.
func (c *Client) Insert(ctx context.Context, stream string, message json.RawMessage) error {
	args := &r.XAddArgs{Stream: stream, Values: payloadValues(message)}
	if err := c.client.XAdd(ctx, args).Err(); err != nil {
		return fmt.Errorf("insert in stream %q: %w", stream, err)
	}
	return nil
}

// InsertBatch appends every message of batch to stream, in order, in a single
// transaction: either all of them are inserted or none.
func (c *Client) InsertBatch(ctx context.Context, stream string, batch []json.RawMessage) error {
	if len(batch) == 0 {
		return nil
	}

	_, err := c.client.TxPipelined(ctx, func(p r.Pipeliner) error {
		for _, message := range batch {
			p.XAdd(ctx, &r.XAddArgs{Stream: stream, Values: payloadValues(message)})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("insert batch in stream %q: %w", stream, err)
	}
	return nil
}

// Empty removes every message of stream and returns them, oldest first, nil if
// the stream is empty or missing. The whole stream is held in memory.
//
// Messages are deleted only once all of them have been read, in a single
// transaction: if Empty fails, nothing is deleted. Messages inserted while it
// runs may be left in the stream for the next call.
func (c *Client) Empty(ctx context.Context, stream string) ([][]byte, error) {
	var (
		messages [][]byte
		ids      []string
	)

	start := "-"
	for {
		entries, err := c.client.XRangeN(ctx, stream, start, "+", int64(c.batchSize)).Result()
		if err != nil {
			return nil, fmt.Errorf("read stream %q: %w", stream, err)
		}

		if len(entries) == 0 {
			break
		}

		for _, entry := range entries {
			payload, ok := entry.Values[payloadField].(string)
			if !ok {
				return nil, fmt.Errorf("empty stream %q, entry %s: %w", stream, entry.ID, ErrMissingPayload)
			}

			messages = append(messages, []byte(payload))
			ids = append(ids, entry.ID)
		}

		// Exclusive start: resume right after the last entry read.
		start = "(" + entries[len(entries)-1].ID
	}

	if err := c.deleteEntries(ctx, stream, ids); err != nil {
		return nil, err
	}
	return messages, nil
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.client.Close()
}

// payloadValues builds the entry fields carrying message.
func payloadValues(message []byte) map[string]any {
	return map[string]any{payloadField: message}
}

// deleteEntries removes the entries ids from stream in one transaction.
func (c *Client) deleteEntries(ctx context.Context, stream string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	_, err := c.client.TxPipelined(ctx, func(p r.Pipeliner) error {
		for chunk := range slices.Chunk(ids, c.batchSize) {
			p.XDel(ctx, stream, chunk...)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("delete from stream %q: %w", stream, err)
	}
	return nil
}
