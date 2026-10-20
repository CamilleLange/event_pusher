package eventpusher

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"test/event_pusher/pkg/model"
	"test/event_pusher/pkg/services/nats"
	"time"
)

func (a *App) HandleStreamFullEvent(ctx context.Context, subject string, data []byte) error {
	a.logger.InfoContext(ctx,
		"message received from nats",
		slog.String("stream", nats.EVENT_PUSHER_STREAM),
		slog.String("subject", nats.STREAM_FULL_SUBJECT),
	)

	var msg model.StreamFullEvent
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("can't unmarshal stream full message : %w", err)
	}

	a.logger.InfoContext(ctx,
		"emptying the redis stream",
		slog.String("redis_stream", msg.Name),
		slog.String("stream", nats.EVENT_PUSHER_STREAM),
		slog.String("subject", nats.STREAM_FULL_SUBJECT),
	)

	messages, err := a.redisClient.Empty(ctx, msg.Name)
	if err != nil {
		return fmt.Errorf("can't empty the redis stream %s : %w", msg.Name, err)
	}

	filename := fmt.Sprintf("%s_%s", msg.Name, time.Now().UTC().Format(time.RFC3339Nano))

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	// build the file from the messages
	for _, message := range messages {
		message = append(message, '\n')

		if _, err := zw.Write(message); err != nil {
			a.logger.WarnContext(ctx, "can't compress in gzip", slog.String("err", err.Error()))
		}
	}

	if err := zw.Close(); err != nil {
		return fmt.Errorf("can't close the gzip writer : %w", err)
	}

	a.logger.InfoContext(ctx,
		"sending the file to S3",
		slog.String("file_name", filename),
		slog.String("stream", nats.EVENT_PUSHER_STREAM),
		slog.String("subject", nats.STREAM_FULL_SUBJECT),
	)

	if err := a.s3Client.WriteFile(ctx, filename, buf.Bytes()); err != nil {
		return fmt.Errorf("can't write data to s3 : %w", err)
	}

	a.AvailableRedisStreams = append(a.AvailableRedisStreams, msg.Name)

	data, err = json.Marshal(a.AvailableRedisStreams)
	if err != nil {
		return fmt.Errorf("can't marshal available stream list : %w", err)
	}

	a.logger.InfoContext(ctx,
		"publishing new available redis stream list",
		slog.Any("current_stream_list", a.AvailableRedisStreams),
		slog.String("stream", nats.EVENT_PUSHER_STREAM),
		slog.String("subject", nats.STREAM_FULL_SUBJECT),
	)

	if err := a.natsClient.Publish(ctx, nats.AVAILABLE_STREAM_LIST_SUBJECT, data); err != nil {
		return fmt.Errorf("can't publish new AvailableStreamListEvent : %w", err)
	}

	return nil
}
