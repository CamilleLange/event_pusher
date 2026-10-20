// Package cmd declares the event_pusher CLI and its subcommands.
package cmd

import (
	"context"

	"github.com/spf13/cobra"

	eventpusher "test/event_pusher/cmd/event_pusher"
	"test/event_pusher/cmd/producer"
	"test/event_pusher/internal/config/confighelper"
)

// NewRootCommand builds the event_pusher root command with its subcommands.
func NewRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:          "event_pusher",
		Short:        "Event pusher over NATS JetStream, Redis streams and S3",
		SilenceUsage: true,
	}
	root.PersistentFlags().String(confighelper.ConfigFileFlag, "", "optional config file (json, yaml, toml...)")
	root.AddCommand(producer.NewCommand(), eventpusher.NewCommand())
	return root
}

// Execute runs the CLI; ctx is cancelled on shutdown signals.
func Execute(ctx context.Context) error {
	return NewRootCommand().ExecuteContext(ctx)
}
