package eventpusher

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"test/event_pusher/pkg/model"
	"test/event_pusher/pkg/services/nats"

	"github.com/bytedance/sonic"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// HandleAvailableStreamListEvent by updating local list and finding the new current index, always trusting the event list.
func (a *App) HandleAvailableStreamListEvent(ctx context.Context, subject string, data []byte) error {
	ctx, span := a.tracer.Start(ctx, "HandleAvailableStreamListEvent")
	defer span.End()
	span.SetAttributes(
		attribute.StringSlice("old_available_stream_list", a.AvailableRedisStreams),
		attribute.String("stream", nats.EVENT_PUSHER_STREAM),
		attribute.String("subject", nats.AVAILABLE_STREAM_LIST_SUBJECT),
	)

	a.logger.InfoContext(ctx,
		"message received from nats",
		slog.String("stream", nats.EVENT_PUSHER_STREAM),
		slog.String("subject", nats.AVAILABLE_STREAM_LIST_SUBJECT),
	)

	var availablestreamList model.AvailableStreamListEvent
	if err := sonic.Unmarshal(data, &availablestreamList); err != nil {
		err = fmt.Errorf("can't unmarshal event : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	newCurrentIndex := slices.Index(availablestreamList, a.getCurrentSteam())
	if newCurrentIndex == -1 {
		// previous stream is no more available, take the first one
		newCurrentIndex = 0
	}

	a.CurrentStreamIndex.Store(int64(newCurrentIndex))
	a.AvailableRedisStreams = availablestreamList

	span.SetAttributes(attribute.StringSlice("current_available_stream_list", a.AvailableRedisStreams))
	a.logger.InfoContext(ctx,
		"redis stream list updated",
		slog.Any("current_available_stream_list", a.AvailableRedisStreams),
		slog.String("stream", nats.EVENT_PUSHER_STREAM),
		slog.String("subject", nats.AVAILABLE_STREAM_LIST_SUBJECT),
	)

	span.SetStatus(codes.Ok, "AvailableStreamListEvent handled")
	return nil
}
