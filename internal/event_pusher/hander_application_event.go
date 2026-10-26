package eventpusher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"test/event_pusher/pkg/services/nats"

	"github.com/bytedance/sonic"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

func (a *App) HandleApplicationEvent(ctx context.Context, subject string, data []byte) error {
	ctx, span := a.tracer.Start(ctx, "HandlerApplicationEvent")
	defer span.End()
	span.SetAttributes(
		attribute.String("stream", nats.APPLICATION_STREAM),
		attribute.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)

	a.logger.InfoContext(ctx,
		"message received from nats",
		slog.String("stream", nats.APPLICATION_STREAM),
		slog.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)

	msg := make([]json.RawMessage, 0)
	if err := sonic.Unmarshal(data, &msg); err != nil {
		err = fmt.Errorf("can't unmarshal message : %w", err)

		a.logger.WarnContext(ctx, "can't unmarshal message", slog.String("err", err.Error()))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	span.SetAttributes(attribute.Int("event_to_insert", len(msg)))
	a.logger.InfoContext(ctx,
		"unmarshal done",
		slog.Int("event_to_insert", len(msg)),
		slog.String("stream", nats.APPLICATION_STREAM),
		slog.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)

	currentStream := a.getCurrentSteam()

	count, err := a.redisClient.Count(ctx, currentStream)
	if err != nil {
		err = fmt.Errorf("can't count the number of element in the stream : %w", err)

		a.logger.WarnContext(ctx, "can't count the number of element in the stream", slog.String("err", err.Error()))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	span.SetAttributes(
		attribute.String("redis_stream", currentStream),
		attribute.Int("redis_stream_count", count),
	)
	a.logger.InfoContext(ctx,
		"pre insert check",
		slog.String("redis_stream", currentStream),
		slog.Int("count", count),
		slog.String("stream", nats.APPLICATION_STREAM),
		slog.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)

	if err := a.storeInRedis(ctx, count, msg, currentStream); err != nil {
		err = fmt.Errorf("can't store in redis : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	span.SetStatus(codes.Ok, "application event handled successfully")

	return nil
}

func (a *App) storeInRedis(ctx context.Context, count int, msg []json.RawMessage, currentStream string) error {
	ctx, span := a.tracer.Start(ctx, "storeInRedis")
	defer span.End()
	span.SetAttributes(
		attribute.String("stream", nats.APPLICATION_STREAM),
		attribute.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)

	limit := a.cfg.RedisStreamLimit

	if count+len(msg) < limit {
		if err := a.redisClient.InsertBatch(ctx, currentStream, msg); err != nil {
			err = fmt.Errorf("can't insert whole batch in redis stream : %w", err)

			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return err
		}

		a.logger.InfoContext(ctx,
			"batch inserted !",
			slog.String("redis_stream", currentStream),
			slog.String("stream", nats.APPLICATION_STREAM),
			slog.String("subject", nats.APPLICATION_EVENT_SUBJECT),
		)
	} else if count < limit {
		elToSave := limit - count // here we know that elToSave < len(msg)

		if err := a.partialRedisInsert(ctx, currentStream, msg, elToSave); err != nil {
			err = fmt.Errorf("can't do a partial redis insert : %w", err)

			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return err
		}

	} else {
		if err := a.notifyStreamFull(ctx, currentStream); err != nil {
			err = fmt.Errorf("can't notify stream is full : %w", err)

			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return err
		}

		return ErrStreamFull
	}

	span.SetStatus(codes.Ok, "data stored in redis")

	return nil
}

func (a *App) partialRedisInsert(ctx context.Context, currentStream string, msg []json.RawMessage, elToSave int) error {
	ctx, span := a.tracer.Start(ctx, "partialRedisInsert")
	defer span.End()
	span.SetAttributes(
		attribute.String("old_redis_stream", currentStream),
		attribute.String("stream", nats.APPLICATION_STREAM),
		attribute.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)

	if err := a.redisClient.InsertBatch(ctx, currentStream, msg[:elToSave]); err != nil {
		err = fmt.Errorf("can't insert each partial batch in the redis stream : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	if err := a.notifyStreamFull(ctx, currentStream); err != nil {
		err = fmt.Errorf("can't notify stream is full : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	// stream was refreshed.
	currentStream = a.getCurrentSteam()
	span.SetAttributes(attribute.String("new_redis_stream", currentStream))

	if err := a.redisClient.InsertBatch(ctx, currentStream, msg[elToSave:]); err != nil {
		err = fmt.Errorf("can't insert each partial batch in the redis stream : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	a.logger.InfoContext(ctx,
		"batch inserted !",
		slog.String("redis_stream", currentStream),
		slog.String("stream", nats.APPLICATION_STREAM),
		slog.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)

	span.SetStatus(codes.Ok, "partial insert success")

	return nil
}

func (a *App) notifyStreamFull(ctx context.Context, streamName string) error {
	ctx, span := a.tracer.Start(ctx, "notifyStreamFull")
	defer span.End()
	span.SetAttributes(
		attribute.String("full_redis_stream", streamName),
		attribute.String("stream", nats.APPLICATION_STREAM),
		attribute.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)

	a.logger.InfoContext(ctx,
		"stream full",
		slog.String("full_redis_stream", streamName),
		slog.String("stream", nats.APPLICATION_STREAM),
		slog.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)

	a.AvailableRedisStreams = slices.DeleteFunc(a.AvailableRedisStreams, func(n string) bool {
		return n == streamName
	})

	// Fail safe to make sure we never send an empty list of available streams.
	if len(a.AvailableRedisStreams) == 0 {
		if err := a.createRedisStream(ctx); err != nil {
			err = fmt.Errorf("can't create a new redis stream : %w", err)

			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return err
		}

		a.CurrentStreamIndex.Store(0)
	}

	if err := a.publishAvailableStreamListEvent(ctx); err != nil {
		err = fmt.Errorf("can't publish AvailableStreamListEvent : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	if err := a.publishStreamFullEvent(ctx, streamName, nats.EVENT_PUSHER_STREAM, nats.STREAM_FULL_SUBJECT); err != nil {
		err = fmt.Errorf("can't publish new StreamFullEvent : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	span.SetStatus(codes.Ok, "successfully notify stream is full")

	return nil
}
