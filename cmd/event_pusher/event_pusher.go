// Package eventpusher declares the `service` command.
package eventpusher

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"test/event_pusher/internal/config/confighelper"
	app "test/event_pusher/internal/event_pusher"
	"test/event_pusher/pkg/services/logger"
)

// NewCommand builds the `service` command.
func NewCommand() *cobra.Command {
	flags := app.FlagSet(*app.DefaultConfig())
	cmd := &cobra.Command{
		Use:   "service",
		Short: "Run the event pusher service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, flags)
		},
	}
	cmd.Flags().AddFlagSet(flags)
	return cmd
}

// run loads the configuration from flags, env and config file, then runs the app.
func run(cmd *cobra.Command, flags *pflag.FlagSet) error {
	configPath, err := cmd.Flags().GetString(confighelper.ConfigFileFlag)
	if err != nil {
		return fmt.Errorf("get config file flag: %w", err)
	}

	v, err := confighelper.NewViper(flags, configPath)
	if err != nil {
		return fmt.Errorf("load service config: %w", err)
	}

	cfg, err := app.FromViper(v)
	if err != nil {
		return err
	}

	log, err := logger.New(cmd.ErrOrStderr(), cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("build logger: %w", err)
	}

	app, err := app.New(cfg, log)
	if err != nil {
		return fmt.Errorf("build app: %w", err)
	}

	return app.Run(cmd.Context())
}
