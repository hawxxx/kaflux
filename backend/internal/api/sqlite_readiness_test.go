package api

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/store"
)

func TestSQLiteReadinessChecksDurableDatabase(t *testing.T) {
	s, err := store.NewSQLite(context.Background(), filepath.Join(t.TempDir(), "kaflux.db"))
	if err != nil {
		t.Fatal(err)
	}
	a := New(Options{Store: s})
	ready := httptest.NewRecorder()
	a.ServeHTTP(ready, httptest.NewRequest("GET", "/ready", nil))
	if ready.Code != 200 {
		t.Fatalf("open SQLite readiness: %d", ready.Code)
	}
	s.Close()
	unavailable := httptest.NewRecorder()
	a.ServeHTTP(unavailable, httptest.NewRequest("GET", "/ready", nil))
	if unavailable.Code != 503 {
		t.Fatalf("closed SQLite readiness: %d", unavailable.Code)
	}
}
