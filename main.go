package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"test/event_pusher/cmd"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := cmd.Execute(ctx)
	stop()

	if err != nil {
		os.Exit(1)
	}
}
