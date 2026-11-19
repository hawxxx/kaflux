package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/buildinfo"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

func TestVersionIsPublicAndReportsBuild(t *testing.T) {
	previous := buildinfo.Version
	t.Cleanup(func() { buildinfo.Version = previous })
	buildinfo.Version = "v0.6.2"
	s, _ := store.New(context.Background(), "")
	a := New(Options{Store: s, AdminHash: "configured"})
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/version", nil))
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var body struct {
		Data struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Data.Version != "0.6.2" {
		t.Fatalf("version %q err %v", body.Data.Version, err)
	}
	if strings.Contains(w.Body.String(), "go1") || w.Header().Get("Content-Security-Policy") == "" {
		t.Fatalf("version response leaks runtime details or skips security headers: %s", w.Body.String())
	}
}

func TestVersionCanBeHidden(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Store: s, HideVersion: true})
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/version", nil))
	if w.Code != 404 || strings.Contains(w.Body.String(), buildinfo.Current()) {
		t.Fatalf("hidden version answered %d: %s", w.Code, w.Body.String())
	}
}
