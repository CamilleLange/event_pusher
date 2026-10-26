// Package producer is the application behind the `producer` command.
package producer

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"math/big"
	"test/event_pusher/pkg/services/nats"
	"time"

	"github.com/bytedance/sonic"
)

// App runs the producer until its context is cancelled.
type App struct {
	cfg        *Config
	logger     *slog.Logger
	natsClient *nats.Client

	MsgCounter int
}

// New builds the producer application.
func New(cfg *Config, logger *slog.Logger) (*App, error) {
	nc, err := nats.New(&cfg.Nats)
	if err != nil {
		return nil, fmt.Errorf("can't create nats client : %w", err)
	}

	return &App{
		cfg:        cfg,
		logger:     logger,
		natsClient: nc,
	}, nil
}

// Run blocks until ctx is cancelled.
func (a App) Run(ctx context.Context) error {
	a.logger.InfoContext(ctx, "producer started",
		slog.String("nats_url", a.cfg.Nats.URL),
	)

	if err := a.StartInjectingMsg(ctx); err != nil {
		return fmt.Errorf("messages injection fails : %w", err)
	}

	<-ctx.Done()

	a.logger.InfoContext(ctx, "producer stopped")
	return nil
}

func (a *App) StartInjectingMsg(ctx context.Context) error {
	if err := a.natsClient.EnsureStream(ctx, nats.APPLICATION_STREAM, nats.APPLICATION_EVENT_SUBJECT); err != nil {
		return fmt.Errorf("can't get the stream to inject messages : %w", err)
	}

	a.logger.InfoContext(ctx, "ready to inject messages")

	ticker := time.NewTicker(a.cfg.TickInterval)

	a.logger.InfoContext(ctx, "injecting in progress...")
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				a.sendMessage(ctx, t)
				a.logger.InfoContext(ctx, "message injected !")
			}
		}
	}()

	return nil
}

func (a *App) sendMessage(ctx context.Context, t time.Time) {
	a.MsgCounter++

	batchSize, _ := rand.Int(rand.Reader, big.NewInt(100))
	if batchSize.Int64() == 0 {
		batchSize = big.NewInt(1)
	}

	payload := make([]msg, 0, batchSize.Int64())

	for batchId := range batchSize.Int64() {
		payload = append(payload, msg{
			Timestamp:      t.Format(time.RFC3339Nano),
			Message:        "producer fired !",
			ProducerStatus: "healthy",
			MessagesSend:   a.MsgCounter,
			BatchId:        batchId,
		})
	}

	data, err := sonic.Marshal(payload)
	if err != nil {
		a.logger.WarnContext(ctx, "can't marshal message batch", slog.String("err", err.Error()))
	}

	if err := a.natsClient.Publish(ctx, nats.APPLICATION_EVENT_SUBJECT, data); err != nil {
		a.logger.WarnContext(ctx, "can't publish message", slog.Time("at", t))
	}

}

type msg struct {
	Timestamp      string `json:"timestamp"`
	Message        string `json:"message"`
	ProducerStatus string `json:"producer_status"`
	MessagesSend   int    `json:"messages_send"`
	BatchId        int64  `json:"batch_id"`
}
