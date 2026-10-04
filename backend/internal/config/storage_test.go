package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStorageSelection(t *testing.T) {
	cases := []struct {
		name, backend, url, sqlite string
		demo                       bool
		want                       string
		fail                       bool
	}{
		{name: "real default", want: "sqlite"},
		{name: "demo default", demo: true, want: "memory"},
		{name: "legacy PostgreSQL URL", url: "postgres://example.invalid/kaflux", want: "postgres"},
		{name: "explicit SQLite", backend: "sqlite", sqlite: "./state.db", want: "sqlite"},
		{name: "PostgreSQL requires URL", backend: "postgres", fail: true},
		{name: "SQLite rejects URL", backend: "sqlite", url: "postgres://example.invalid/kaflux", fail: true},
		{name: "memory rejects URL", backend: "memory", url: "postgres://example.invalid/kaflux", demo: true, fail: true},
		{name: "real memory forbidden", backend: "memory", fail: true},
		{name: "unknown backend", backend: "other", fail: true},
		{name: "SQLite memory forbidden", backend: "sqlite", sqlite: ":memory:", fail: true},
		{name: "SQLite URI forbidden", backend: "sqlite", sqlite: "file:shared.db?mode=memory", fail: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KAFLUX_STORAGE_BACKEND", tc.backend)
			t.Setenv("KAFLUX_DATABASE_URL", tc.url)
			t.Setenv("KAFLUX_SQLITE_PATH", tc.sqlite)
			if tc.demo {
				t.Setenv("KAFLUX_DEMO", "true")
			} else {
				t.Setenv("KAFLUX_DEMO", "false")
			}
			p := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(p, []byte("{}"), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(p)
			if tc.fail {
				if err == nil {
					t.Fatal("invalid storage configuration accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.StorageBackend != tc.want {
				t.Fatalf("backend %q, want %q", c.StorageBackend, tc.want)
			}
			if tc.want == "sqlite" && c.SQLitePath == "" {
				t.Fatal("missing durable SQLite path")
			}
			if tc.name == "real default" && c.SQLitePath != "/var/lib/kaflux/kaflux.db" {
				t.Fatalf("unexpected default SQLite path %q", c.SQLitePath)
			}
		})
	}
}

func TestStorageYAMLAndEnvironmentOverride(t *testing.T) {
	t.Setenv("KAFLUX_DATABASE_URL", "")
	t.Setenv("KAFLUX_DEMO", "false")
	t.Setenv("KAFLUX_STORAGE_BACKEND", "sqlite")
	t.Setenv("KAFLUX_SQLITE_PATH", "/tmp/override.db")
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte("runtime:\n  storageBackend: postgres\n  sqlitePath: /tmp/from-yaml.db\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.StorageBackend != "sqlite" || c.SQLitePath != "/tmp/override.db" {
		t.Fatal("environment did not override YAML")
	}
}
