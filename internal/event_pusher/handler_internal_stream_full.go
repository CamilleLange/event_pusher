package eventpusher

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"log/slog"
	"test/event_pusher/pkg/model"
	"test/event_pusher/pkg/services/nats"
	"time"

	"github.com/bytedance/sonic"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

func (a *App) HandleStreamFullEvent(ctx context.Context, subject string, data []byte) error {
	ctx, span := a.tracer.Start(ctx, "HandleStreamFullEvent")
	defer span.End()
	span.SetAttributes(
		attribute.String("stream", nats.EVENT_PUSHER_STREAM),
		attribute.String("subject", nats.STREAM_FULL_SUBJECT),
	)

	a.logger.InfoContext(ctx,
		"message received from nats",
		slog.String("stream", nats.EVENT_PUSHER_STREAM),
		slog.String("subject", nats.STREAM_FULL_SUBJECT),
	)

	var msg model.StreamFullEvent
	if err := sonic.Unmarshal(data, &msg); err != nil {
		err = fmt.Errorf("can't unmarshal stream full message : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	span.SetAttributes(attribute.String("redis_stream", msg.Name))
	a.logger.InfoContext(ctx,
		"emptying the redis stream",
		slog.String("redis_stream", msg.Name),
		slog.String("stream", nats.EVENT_PUSHER_STREAM),
		slog.String("subject", nats.STREAM_FULL_SUBJECT),
	)

	messages, err := a.redisClient.Empty(ctx, msg.Name)
	if err != nil {
		err = fmt.Errorf("can't empty the redis stream %s : %w", msg.Name, err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	filename := fmt.Sprintf("%s_%s", msg.Name, time.Now().UTC().Format(time.RFC3339Nano))

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	// build the file from the messages
	for _, message := range messages {
		message = append(message, '\n')

		if _, err := zw.Write(message); err != nil {
			err = fmt.Errorf("can't compress in gzip : %w", err)

			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return err
		}
	}

	if err := zw.Close(); err != nil {
		err = fmt.Errorf("can't close the gzip writer : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	a.logger.InfoContext(ctx,
		"sending the file to S3",
		slog.String("file_name", filename),
		slog.String("stream", nats.EVENT_PUSHER_STREAM),
		slog.String("subject", nats.STREAM_FULL_SUBJECT),
	)

	if err := a.s3Client.WriteFile(ctx, filename, buf.Bytes()); err != nil {
		err = fmt.Errorf("can't write data to s3 : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	a.AvailableRedisStreams = append(a.AvailableRedisStreams, msg.Name)

	a.logger.InfoContext(ctx,
		"publishing new available redis stream list",
		slog.Any("current_stream_list", a.AvailableRedisStreams),
		slog.String("stream", nats.EVENT_PUSHER_STREAM),
		slog.String("subject", nats.STREAM_FULL_SUBJECT),
	)

	if err := a.publishAvailableStreamListEvent(ctx); err != nil {
		err = fmt.Errorf("can't publish AvailableStreamListEvent : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	span.SetStatus(codes.Ok, "StreamFullEvent handled")

	return nil
}
