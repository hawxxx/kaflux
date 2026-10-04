package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSessionLifetimeConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, yaml, env string
		want            time.Duration
		invalid         bool
	}{
		{name: "default", want: 8 * time.Hour},
		{name: "yaml", yaml: "15m", want: 15 * time.Minute},
		{name: "override", yaml: "15m", env: "1h", want: time.Hour},
		{name: "minimum", yaml: "5m", want: 5 * time.Minute},
		{name: "maximum", yaml: "24h", want: 24 * time.Hour},
		{name: "too short", yaml: "4m59s", invalid: true},
		{name: "too long", yaml: "24h1s", invalid: true},
		{name: "zero", yaml: "0", invalid: true},
		{name: "negative", yaml: "-1h", invalid: true},
		{name: "malformed", env: "tomorrow", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.yaml")
			data := ""
			if tc.yaml != "" {
				data = "runtime:\n  sessionLifetime: '" + tc.yaml + "'\n"
			}
			if err := os.WriteFile(p, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if tc.env != "" {
				t.Setenv("KAFLUX_SESSION_LIFETIME", tc.env)
			} else {
				old, ok := os.LookupEnv("KAFLUX_SESSION_LIFETIME")
				os.Unsetenv("KAFLUX_SESSION_LIFETIME")
				t.Cleanup(func() {
					if ok {
						os.Setenv("KAFLUX_SESSION_LIFETIME", old)
					}
				})
			}
			c, err := Load(p)
			if tc.invalid {
				if err == nil {
					t.Fatal("invalid lifetime accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.SessionLifetime != tc.want {
				t.Fatalf("got %v want %v", c.SessionLifetime, tc.want)
			}
		})
	}
}
