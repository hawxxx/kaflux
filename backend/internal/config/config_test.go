package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestYAMLSecretsAndEnvironmentOverride(t *testing.T) {
	t.Setenv("TEST_DATABASE", "postgres://test.invalid/db")
	t.Setenv("TEST_HASH", "hash-reference")
	t.Setenv("KAFLUX_LISTEN", ":9000")
	t.Setenv("KAFLUX_DEMO", "false")
	p := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(p, []byte("runtime:\n  listen: ':8080'\n  demo: true\n  databaseURLEnv: TEST_DATABASE\n  adminPasswordHashEnv: TEST_HASH\nclusters:\n  - id: local\n    name: Local\n    seeds: [kafka:9092]\n    allowPlaintext: true\n    passwordEnv: TEST_PASSWORD\n"), 0600)
	if err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":9000" || c.Demo || c.DatabaseURL != "postgres://test.invalid/db" || c.AdminPasswordHash != "hash-reference" {
		t.Fatalf("environment precedence/resolution failed: listen=%s demo=%v", c.Listen, c.Demo)
	}
	if len(c.Clusters) != 1 || c.Clusters[0].PasswordEnv != "TEST_PASSWORD" || !c.Clusters[0].AllowPlaintext {
		t.Fatal("cluster configuration not loaded")
	}
}

func TestRejectUnknownAndMissingSecretReference(t *testing.T) {
	for _, input := range []string{"runtime:\n  demmo: true\n", "runtime:\n  databaseURLEnv: NEVER_SET_KAFLUX_TEST_SECRET\n"} {
		p := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(p, []byte(input), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(p); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}

func TestExplicitMissingFileAndInvalidBoolean(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Fatal("explicit missing configuration accepted")
	}
	t.Setenv("KAFLUX_DEMO", "maybe")
	if _, err := Load(""); err == nil {
		t.Fatal("invalid demo setting accepted")
	}
}
