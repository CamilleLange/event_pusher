package redis

import "testing"

func TestFlagSet_UsesDefaultConfig(t *testing.T) {
	fs := FlagSet(*DefaultConfig())

	tests := []struct {
		flag string
		want string
	}{
		{flag: "Addr", want: "localhost:6379"},
		{flag: "Password", want: ""},
		{flag: "DB", want: "0"},
		{flag: "BatchSize", want: "1000"},
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
