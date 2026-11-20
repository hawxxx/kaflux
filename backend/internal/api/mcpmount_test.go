package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/store"
)

func TestMCPMountedBeforeSessionCheck(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	defer s.Close()
	a := New(Options{Store: s})
	// Without an MCP handler, /mcp is a plain 404, signed in or not.
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/mcp", nil))
	if w.Code != 404 {
		t.Fatalf("unmounted /mcp: %d", w.Code)
	}
	demo := New(Options{Store: s, Demo: true, StaticDir: t.TempDir()})
	w = httptest.NewRecorder()
	demo.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/mcp", nil))
	if w.Code != 404 {
		t.Fatalf("unmounted /mcp with a session: %d", w.Code)
	}
	a.SetMCP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTeapot) }))
	for _, p := range []string{"/mcp", "/mcp/", "/api/v1/mcp"} {
		w = httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest(http.MethodPost, p, nil))
		if (w.Code == http.StatusTeapot) != (p == "/mcp") {
			t.Errorf("%s: %d", p, w.Code)
		}
	}
}

func TestExtraMetricsAppended(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	defer s.Close()
	a := New(Options{Store: s, ExtraMetrics: func(w io.Writer) { fmt.Fprint(w, "kaflux_extra 1\n") }})
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if body := w.Body.String(); !strings.Contains(body, "kaflux_configured_clusters 0\n") || !strings.HasSuffix(body, "kaflux_extra 1\n") {
		t.Fatalf("metrics %q", body)
	}
}
