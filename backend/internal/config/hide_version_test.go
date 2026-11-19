package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHideVersionFromFileAndEnvironment(t *testing.T) {
	t.Setenv("KAFLUX_DEMO", "true")
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte("runtime:\n  hideVersion: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil || !c.HideVersion {
		t.Fatalf("file setting ignored: %v %v", c.HideVersion, err)
	}
	t.Setenv("KAFLUX_HIDE_VERSION", "false")
	if c, err = Load(p); err != nil || c.HideVersion {
		t.Fatalf("environment override ignored: %v %v", c.HideVersion, err)
	}
	t.Setenv("KAFLUX_HIDE_VERSION", "maybe")
	if _, err = Load(p); err == nil {
		t.Fatal("invalid boolean accepted")
	}
}
