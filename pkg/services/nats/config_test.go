package nats

import "testing"

func TestFlagSet_UsesDefaultConfig(t *testing.T) {
	fs := FlagSet(*DefaultConfig())

	tests := []struct {
		flag string
		want string
	}{
		{flag: "URL", want: "nats://localhost:4222"},
	}
	for _, tt := range tests {
		t.Run(tt.flag, func(t *testing.T) {
			f := fs.Lookup(tt.flag)
			if f == nil {
				t.Fatalf("flag %q not declared", tt.flag)
			}

			if f.DefValue != tt.want {
				t.Errorf("default = %q, want %q", f.DefValue, tt.want)
			}
		})
	}
}
