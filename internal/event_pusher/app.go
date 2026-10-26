// Package eventpusher is the application behind the `service` command.
package eventpusher

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"test/event_pusher/pkg/model"
	"test/event_pusher/pkg/services/nats"
	"test/event_pusher/pkg/services/redis"
	"test/event_pusher/pkg/services/s3"

	"github.com/bytedance/sonic"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// App runs the event pusher service until its context is cancelled.
type App struct {
	cfg    *Config
	logger *slog.Logger
	tracer trace.Tracer

	natsClient  *nats.Client
	redisClient *redis.Client
	s3Client    s3.Client

	AvailableRedisStreams []string
	CurrentStreamIndex    atomic.Int64

	createdRedisStream atomic.Int64
}

// Run blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	ctx, span := a.tracer.Start(ctx, "Run")
	span.SetAttributes(
		attribute.String("nats_url", a.cfg.Nats.URL),
		attribute.String("redis_addr", a.cfg.Redis.Addr),
		attribute.String("s3_endpoint", a.cfg.S3.Endpoint),
	)

	a.logger.InfoContext(ctx, "service started",
		slog.String("nats_url", a.cfg.Nats.URL),
		slog.String("redis_addr", a.cfg.Redis.Addr),
		slog.String("s3_endpoint", a.cfg.S3.Endpoint),
	)

	if err := a.SetupNatsHandlers(ctx); err != nil {
		err = fmt.Errorf("can't setup nats handlers : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()

		return err
	}

	if err := a.InitAvailableStreamList(ctx); err != nil {
		err = fmt.Errorf("can't init available stream list : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()

		return err
	}

	go a.Listen(ctx, nats.AVAILABLE_STREAM_LIST_SUBJECT, a.HandleAvailableStreamListEvent)
	go a.Listen(ctx, nats.APPLICATION_EVENT_SUBJECT, a.HandleApplicationEvent)
	go a.Listen(ctx, nats.STREAM_FULL_SUBJECT, a.HandleStreamFullEvent)

	span.SetStatus(codes.Ok, "app is running")
	span.End()

	<-ctx.Done()

	if err := a.Close(ctx); err != nil {
		a.logger.Error("can't close all connections", slog.String("err", err.Error()))
	}

	a.logger.InfoContext(ctx, "service stopped")
	return nil
}

func (a *App) SetupNatsHandlers(ctx context.Context) error {
	ctx, span := a.tracer.Start(ctx, "SetupNatsHandlers")
	defer span.End()

	if err := a.natsClient.EnsureStream(ctx, nats.EVENT_PUSHER_STREAM, nats.STREAM_FULL_SUBJECT, nats.AVAILABLE_STREAM_LIST_SUBJECT); err != nil {
		err = fmt.Errorf("can't ensure the internal stream to sync with the other instances : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	if err := a.natsClient.EnsureStream(ctx, nats.APPLICATION_STREAM, nats.APPLICATION_EVENT_SUBJECT); err != nil {
		err = fmt.Errorf("can't get the stream to get application events : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	span.SetStatus(codes.Ok, "nats handlers are setup")

	return nil
}

func (a *App) Listen(ctx context.Context, subject string, handler nats.Handler) {
	ctx, span := a.tracer.Start(ctx, "SetupNatsHandlers")
	span.SetAttributes(attribute.String("subject", subject))
	defer span.End()

	err := a.natsClient.Listen(ctx, subject, handler)
	if err != nil {
		err = fmt.Errorf("can't listen available stream list subject : %w", err)
		a.logger.Error("error while listenning",
			slog.String("err", err.Error()),
			slog.String("subject", nats.AVAILABLE_STREAM_LIST_SUBJECT),
		)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	span.SetStatus(codes.Ok, "Listen was successfull")
}

func (a *App) Close(ctx context.Context) error {
	ctx, span := a.tracer.Start(ctx, "Close")
	defer span.End()

	if err := a.natsClient.Close(); err != nil {
		err = fmt.Errorf("can't close app's natsClient : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	if err := a.redisClient.Close(); err != nil {
		err = fmt.Errorf("can't close app's redisClient : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	if err := a.s3Client.Close(); err != nil {
		err = fmt.Errorf("can't close app's s3Client : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	span.SetStatus(codes.Ok, "Close was successfull")

	return nil
}

func (a *App) InitAvailableStreamList(ctx context.Context) error {
	ctx, span := a.tracer.Start(ctx, "InitAvailableStreamList")
	defer span.End()
	span.SetAttributes(
		attribute.String("stream", nats.EVENT_PUSHER_STREAM),
		attribute.String("subject", nats.AVAILABLE_STREAM_LIST_SUBJECT),
	)

	msg, err := a.natsClient.LastMessage(ctx, nats.AVAILABLE_STREAM_LIST_SUBJECT)
	if err != nil && len(msg) == 0 {
		if err := a.createRedisStream(ctx); err != nil {
			err = fmt.Errorf("can't create a redis stream : %w", err)

			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return err
		}
		a.CurrentStreamIndex.Store(0)
	}

	span.SetStatus(codes.Ok, "AvailableStreamList is initialized")

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
	ctx, span := a.tracer.Start(ctx, "getNextAvailableStream")
	defer span.End()
	span.SetAttributes(
		attribute.String("stream", nats.EVENT_PUSHER_STREAM),
		attribute.String("subject", nats.AVAILABLE_STREAM_LIST_SUBJECT),
	)

	nextIndex := int(a.CurrentStreamIndex.Load() + 1)

	if nextIndex >= len(a.AvailableRedisStreams) {
		// Not enough redis streams available, creating a new one.
		if err := a.createRedisStream(ctx); err != nil {
			err = fmt.Errorf("can't create a redis stream : %w", err)

			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return "", err
		}
	}

	a.CurrentStreamIndex.Store(int64(nextIndex))
	next := a.AvailableRedisStreams[nextIndex]

	span.SetAttributes(attribute.String("next_redis_stream", next))
	span.SetStatus(codes.Ok, "success")

	return next, nil
}

func (a *App) createRedisStream(ctx context.Context) error {
	ctx, span := a.tracer.Start(ctx, "publishAvailableStreamList")
	defer span.End()

	newStreamName := a.getNewRedisStreamName()

	span.SetAttributes(attribute.String("redis_stream", newStreamName))

	if err := a.redisClient.Create(ctx, newStreamName); err != nil {
		err = fmt.Errorf("can't create first available stream : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	a.AvailableRedisStreams = append(a.AvailableRedisStreams, newStreamName)

	if err := a.publishAvailableStreamListEvent(ctx); err != nil {
		err = fmt.Errorf("can't publish AvailableStreamListEvent : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	return nil
}

func (a *App) getNewRedisStreamName() string {
	currentStreamNumber := int(a.createdRedisStream.Load())

	s := "stream_" + strconv.Itoa(currentStreamNumber)

	a.createdRedisStream.Add(1)

	return s
}

func (a *App) publishAvailableStreamListEvent(ctx context.Context) error {
	ctx, span := a.tracer.Start(ctx, "publishAvailableStreamList")
	defer span.End()
	span.SetAttributes(
		attribute.String("stream", nats.EVENT_PUSHER_STREAM),
		attribute.String("subject", nats.AVAILABLE_STREAM_LIST_SUBJECT),
	)

	data, err := sonic.Marshal(a.AvailableRedisStreams)
	if err != nil {
		err = fmt.Errorf("can't marshal updated available stream list : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	if err := a.natsClient.Publish(ctx, nats.AVAILABLE_STREAM_LIST_SUBJECT, data); err != nil {
		err = fmt.Errorf("can't publish available stream list message : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	span.SetStatus(codes.Ok, "AvailableStreamListEvent published")

	return nil
}

func (a *App) publishStreamFullEvent(ctx context.Context, redisStreamName, stream, subject string) error {
	ctx, span := a.tracer.Start(ctx, "publishStreamFullEvent")
	defer span.End()
	span.SetAttributes(
		attribute.String("stream", stream),
		attribute.String("subject", subject),
	)

	data, err := sonic.Marshal(model.StreamFullEvent{
		Name: redisStreamName,
	})
	if err != nil {
		err = fmt.Errorf("can't marshal stream full message : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	if err := a.natsClient.Publish(ctx, nats.STREAM_FULL_SUBJECT, data); err != nil {
		err = fmt.Errorf("can't publish stream full message : %w", err)

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	span.SetStatus(codes.Ok, "StreamFullEvent published")
	return nil
}
