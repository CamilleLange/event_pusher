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

// HandleAvailableStreamListEvent by updating local list and finding the new current index, always trusting the event list.
func (a *App) HandleAvailableStreamListEvent(ctx context.Context, subject string, data []byte) error {
	a.logger.InfoContext(ctx,
		"message received from nats",
		slog.String("stream", nats.EVENT_PUSHER_STREAM),
		slog.String("subject", nats.AVAILABLE_STREAM_LIST_SUBJECT),
	)

	var availablestreamList model.AvailableStreamListEvent
	if err := json.Unmarshal(data, &availablestreamList); err != nil {
		return fmt.Errorf("can't unmarshal event : %w", err)
	}

	newCurrentIndex := slices.Index(availablestreamList, a.getCurrentSteam())
	if newCurrentIndex == -1 {
		// previous stream is no more available, take the first one
		newCurrentIndex = 0
	}

	a.CurrentStreamIndex.Store(int64(newCurrentIndex))
	a.AvailableRedisStreams = availablestreamList

	a.logger.InfoContext(ctx,
		"redis stream list updated",
		slog.Any("current_stream_list", a.AvailableRedisStreams),
		slog.String("stream", nats.EVENT_PUSHER_STREAM),
		slog.String("subject", nats.AVAILABLE_STREAM_LIST_SUBJECT),
	)
	return nil
}
