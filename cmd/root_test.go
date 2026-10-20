package cmd

import (
	"slices"
	"testing"

	"test/event_pusher/internal/config/confighelper"
)

func TestNewRootCommand(t *testing.T) {
	root := NewRootCommand()

	if root.PersistentFlags().Lookup(confighelper.ConfigFileFlag) == nil {
		t.Errorf("persistent flag --%s missing", confighelper.ConfigFileFlag)
	}

	var names []string
	for _, c := range root.Commands() {
		names = append(names, c.Name())
	}

	for _, want := range []string{"producer", "service"} {
		if !slices.Contains(names, want) {
			t.Errorf("subcommand %q missing in %v", want, names)
		}
	}
}
