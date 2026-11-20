package api

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

func TestNoDirectGrantAccess(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") || f == "mcpcontext.go" {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			if strings.Contains(line, "a.o.Grants") {
				t.Errorf("%s:%d reads a.o.Grants directly; use a.grants(r)", f, i+1)
			}
		}
	}
}

type foreignKey struct{}

func TestSessionIgnoresForgedContext(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo"}}})
	forged := auth.Session{User: auth.User{ID: "x", Roles: []string{"administrator"}}, CSRF: "c"}

	req := httptest.NewRequest("GET", "/api/v1/clusters", nil)
	req.Header.Set("Authorization", "Bearer x")
	w := httptest.NewRecorder()
	a.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Fatalf("bearer-only request = %d, want 401", w.Code)
	}

	req = httptest.NewRequest("GET", "/api/v1/clusters", nil)
	req = req.WithContext(context.WithValue(req.Context(), foreignKey{}, mcpSession{session: forged}))
	w = httptest.NewRecorder()
	a.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Fatalf("forged context key honored: %d", w.Code)
	}
}

func TestMCPSessionNarrowsGrantsAndSkipsCSRF(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo"}}})
	sess := auth.Session{User: auth.User{ID: "u", Roles: []string{"administrator"}}, CSRF: "c"}
	readOnly := []auth.Grant{{Role: "administrator", Cluster: "demo", Action: "read", Pattern: "*"}}

	req := WithMCPSession(httptest.NewRequest("GET", "/api/v1/clusters", nil), sess, readOnly)
	if got := a.grants(req); len(got) != 1 || got[0].Action != "read" {
		t.Fatalf("grants(r) = %+v", got)
	}
	if got := a.grants(httptest.NewRequest("GET", "/", nil)); len(got) != 1 || got[0].Action != "*" {
		t.Fatalf("default grants = %+v", got)
	}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("mcp session GET = %d %s", w.Code, w.Body.String())
	}

	// No CSRF header: allowed past CSRF, then denied by the narrowed grants.
	req = WithMCPSession(httptest.NewRequest("POST", "/api/v1/clusters/demo/topics", strings.NewReader(`{"name":"t","partitions":1,"replicationFactor":1}`)), sess, readOnly)
	w = httptest.NewRecorder()
	a.ServeHTTP(w, req)
	if w.Code == 403 && strings.Contains(w.Body.String(), "csrf_failed") {
		t.Fatal("CSRF enforced for MCP session")
	}
	if w.Code != 403 || !strings.Contains(w.Body.String(), "forbidden") {
		t.Fatalf("narrowed create = %d %s, want 403 forbidden", w.Code, w.Body.String())
	}
}
