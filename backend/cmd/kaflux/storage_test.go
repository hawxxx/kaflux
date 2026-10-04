package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/config"
)

func TestRealStorageStillRequiresAuthentication(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		cfg := config.Config{StorageBackend: backend}
		if validateAuthentication(cfg) == nil {
			t.Fatalf("%s accepted missing authentication", backend)
		}
		cfg.AdminPasswordHash = "configured"
		if err := validateAuthentication(cfg); err != nil {
			t.Fatal(err)
		}
	}
	if err := validateAuthentication(config.Config{Demo: true}); err != nil {
		t.Fatal(err)
	}
}

func TestOpenSQLiteStore(t *testing.T) {
	ctx := context.Background()
	s, err := openStore(ctx, config.Config{StorageBackend: "sqlite", SQLitePath: filepath.Join(t.TempDir(), "kaflux.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !s.IsPersistent() {
		t.Fatal("SQLite store is not durable")
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
