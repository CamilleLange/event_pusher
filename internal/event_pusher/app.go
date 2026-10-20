// Package eventpusher is the application behind the `service` command.
package eventpusher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"test/event_pusher/pkg/services/nats"
	"test/event_pusher/pkg/services/redis"
	"test/event_pusher/pkg/services/s3"
)

// App runs the event pusher service until its context is cancelled.
type App struct {
	cfg         *Config
	logger      *slog.Logger
	natsClient  *nats.Client
	redisClient *redis.Client
	s3Client    s3.Client

	AvailableRedisStreams []string
	CurrentStreamIndex    atomic.Int64

	createdRedisStream atomic.Int64
}

// Run blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	a.logger.InfoContext(ctx, "service started",
		slog.String("nats_url", a.cfg.Nats.URL),
		slog.String("redis_addr", a.cfg.Redis.Addr),
		slog.String("s3_endpoint", a.cfg.S3.Endpoint),
	)

	if err := a.SetupNatsHandlers(ctx); err != nil {
		return fmt.Errorf("can't setup nats handlers : %w", err)
	}

	if err := a.InitAvailableStreamList(ctx); err != nil {
		return fmt.Errorf("can't init available stream list : %w", err)
	}

	go a.Listen(ctx, nats.AVAILABLE_STREAM_LIST_SUBJECT, a.HandleAvailableStreamListEvent)
	go a.Listen(ctx, nats.APPLICATION_EVENT_SUBJECT, a.HandleApplicationEvent)
	go a.Listen(ctx, nats.STREAM_FULL_SUBJECT, a.HandleStreamFullEvent)

	<-ctx.Done()

	if err := a.Close(ctx); err != nil {
		a.logger.Error("can't close all connections", slog.String("err", err.Error()))
	}

	a.logger.InfoContext(ctx, "service stopped")
	return nil
}

func (a *App) SetupNatsHandlers(ctx context.Context) error {
	if err := a.natsClient.EnsureStream(ctx, nats.EVENT_PUSHER_STREAM, nats.STREAM_FULL_SUBJECT, nats.AVAILABLE_STREAM_LIST_SUBJECT); err != nil {
		return fmt.Errorf("can't ensure the internal stream to sync with the other instances : %w", err)
	}

	if err := a.natsClient.EnsureStream(ctx, nats.APPLICATION_STREAM, nats.APPLICATION_EVENT_SUBJECT); err != nil {
		return fmt.Errorf("can't get the stream to get application events : %w", err)
	}

	return nil
}

func (a *App) Listen(ctx context.Context, subject string, handler nats.Handler) {
	err := a.natsClient.Listen(ctx, subject, handler)
	if err != nil {
		err = fmt.Errorf("can't listen available stream list subject : %w", err)
		a.logger.Error("error while listenning",
			slog.String("err", err.Error()),
			slog.String("subject", nats.AVAILABLE_STREAM_LIST_SUBJECT),
		)
	}
}

func (a *App) Close(ctx context.Context) error {
	if err := a.natsClient.Close(); err != nil {
		return fmt.Errorf("can't close app's natsClient : %w", err)
	}

	if err := a.redisClient.Close(); err != nil {
		return fmt.Errorf("can't close app's redisClient : %w", err)
	}

	if err := a.s3Client.Close(); err != nil {
		return fmt.Errorf("can't close app's s3Client : %w", err)
	}

	return nil
}

func (a *App) InitAvailableStreamList(ctx context.Context) error {
	msg, err := a.natsClient.LastMessage(ctx, nats.AVAILABLE_STREAM_LIST_SUBJECT)
	if err != nil && len(msg) == 0 {
		if err := a.createRedisStream(ctx); err != nil {
			return fmt.Errorf("can't create a redis stream : %w", err)
		}
		a.CurrentStreamIndex.Store(0)
	}

	return nil
}

// getCurrentSteam from App.AvailableRedisStreams slice.
// The current stream is set by the atomic index.
func (a *App) getCurrentSteam() string {
	return a.AvailableRedisStreams[a.CurrentStreamIndex.Load()]
}

// getNextAvailableStream from App.AvailableRedisStreams slice.
// The current stream is set by the atomic index + 1.
func (a *App) getNextAvailableStream(ctx context.Context) (string, error) {
	nextIndex := int(a.CurrentStreamIndex.Load() + 1)

	if nextIndex >= len(a.AvailableRedisStreams) {
		// Not enough redis streams available, creating a new one.
		if err := a.createRedisStream(ctx); err != nil {
			return "", fmt.Errorf("can't create a redis stream : %w", err)
		}
	}

	a.CurrentStreamIndex.Store(int64(nextIndex))
	return a.AvailableRedisStreams[nextIndex], nil
}

func (a *App) createRedisStream(ctx context.Context) error {
	newStreamName := a.getNewRedisStreamName()

	if err := a.redisClient.Create(ctx, newStreamName); err != nil {
		return fmt.Errorf("can't create first available stream : %w", err)
	}

	a.AvailableRedisStreams = append(a.AvailableRedisStreams, newStreamName)

	data, err := json.Marshal(a.AvailableRedisStreams)
	if err != nil {
		return fmt.Errorf("can't marshal available stream list : %w", err)
	}

	if err := a.natsClient.Publish(ctx, nats.AVAILABLE_STREAM_LIST_SUBJECT, data); err != nil {
		return fmt.Errorf("can't publish new AvailableStreamListEvent : %w", err)
	}

	return nil
}

func (a *App) getNewRedisStreamName() string {
	currentStreamNumber := int(a.createdRedisStream.Load())

	s := "stream_" + strconv.Itoa(currentStreamNumber)

	a.createdRedisStream.Add(1)

	return s
}
