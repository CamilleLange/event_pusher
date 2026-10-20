// Package logger builds the structured logger shared by the commands.
package logger

import (
	"fmt"
	"io"
	"log/slog"
)

// New returns a JSON slog logger writing to w, filtered at level
// ("debug", "info", "warn" or "error", case-insensitive).
func New(w io.Writer, level string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("parse log level %q: %w", level, err)
	}

	opts := slog.HandlerOptions{Level: lvl}
	return slog.New(slog.NewJSONHandler(w, &opts)), nil
}
