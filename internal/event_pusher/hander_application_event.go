package eventpusher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"test/event_pusher/pkg/model"
	"test/event_pusher/pkg/services/nats"
)

func (a *App) HandleApplicationEvent(ctx context.Context, subject string, data []byte) error {
	a.logger.InfoContext(ctx,
		"message received from nats",
		slog.String("stream", nats.APPLICATION_STREAM),
		slog.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)

	msg := make([]json.RawMessage, 0)
	if err := json.Unmarshal(data, &msg); err != nil {
		a.logger.WarnContext(ctx, "can't unmarshal message", slog.String("err", err.Error()))
		return fmt.Errorf("can't unmarshal message : %w", err)
	}

	a.logger.InfoContext(ctx,
		"unmarshal done",
		slog.Int("event_to_insert", len(msg)),
		slog.String("stream", nats.APPLICATION_STREAM),
		slog.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)

	currentStream := a.getCurrentSteam()

	count, err := a.redisClient.Count(ctx, currentStream)
	if err != nil {
		a.logger.WarnContext(ctx, "can't count the number of element in the stream", slog.String("err", err.Error()))
		return fmt.Errorf("can't count the number of element in the stream : %w", err)
	}

	a.logger.InfoContext(ctx,
		"pre insert check",
		slog.String("redis_stream", currentStream),
		slog.Int("count", count),
		slog.String("stream", nats.APPLICATION_STREAM),
		slog.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)

	limit := a.cfg.RedisStreamLimit

	if count+len(msg) < limit {
		if err := a.redisClient.InsertBatch(ctx, currentStream, msg); err != nil {
			return fmt.Errorf("can't insert whole batch in redis stream : %w", err)
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
			return fmt.Errorf("can't do a partial redis insert : %w", err)
		}

	} else {
		if err := a.notifyStreamFull(ctx, currentStream); err != nil {
			return fmt.Errorf("can't notify stream is full : %w", err)
		}

		return ErrStreamFull
	}

	return nil
}

func (a *App) partialRedisInsert(ctx context.Context, currentStream string, msg []json.RawMessage, elToSave int) error {
	if err := a.redisClient.InsertBatch(ctx, currentStream, msg[:elToSave]); err != nil {
		return fmt.Errorf("can't insert each partial batch in the redis stream : %w", err)
	}

	if err := a.notifyStreamFull(ctx, currentStream); err != nil {
		return fmt.Errorf("can't notify stream is full : %w", err)
	}

	// stream was refreshed.
	currentStream = a.getCurrentSteam()

	if err := a.redisClient.InsertBatch(ctx, currentStream, msg[elToSave:]); err != nil {
		return fmt.Errorf("can't insert each partial batch in the redis stream : %w", err)
	}

	a.logger.InfoContext(ctx,
		"batch inserted !",
		slog.String("redis_stream", currentStream),
		slog.String("stream", nats.APPLICATION_STREAM),
		slog.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)
	return nil
}

func (a *App) notifyStreamFull(ctx context.Context, streamName string) error {
	a.logger.InfoContext(ctx,
		"stream full",
		slog.String("redis_stream", streamName),
		slog.String("stream", nats.APPLICATION_STREAM),
		slog.String("subject", nats.APPLICATION_EVENT_SUBJECT),
	)

	a.AvailableRedisStreams = slices.DeleteFunc(a.AvailableRedisStreams, func(n string) bool {
		return n == streamName
	})

	// Fail safe to make sure we never send an empty list of available streams.
	if len(a.AvailableRedisStreams) == 0 {
		if err := a.createRedisStream(ctx); err != nil {
			return fmt.Errorf("can't create a new redis stream : %w", err)
		}

		a.CurrentStreamIndex.Store(0)
	}

	data, err := json.Marshal(a.AvailableRedisStreams)
	if err != nil {
		return fmt.Errorf("can't marshal updated available stream list : %w", err)
	}

	if err := a.natsClient.Publish(ctx, nats.AVAILABLE_STREAM_LIST_SUBJECT, data); err != nil {
		a.logger.WarnContext(ctx, "can't publish stream full message", slog.String("err", err.Error()))
	}

	data, err = json.Marshal(model.StreamFullEvent{
		Name: streamName,
	})
	if err != nil {
		a.logger.WarnContext(ctx, "can't marshal stream full message", slog.String("err", err.Error()))
	}

	if err := a.natsClient.Publish(ctx, nats.STREAM_FULL_SUBJECT, data); err != nil {
		return fmt.Errorf("can't publish stream full message : %w", err)
	}

	return nil
}
