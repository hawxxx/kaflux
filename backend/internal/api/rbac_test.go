package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

// viewerAPI mirrors the read-only role in config.example.yaml.
func viewerAPI(t *testing.T) *API {
	t.Helper()
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo"}}, Grants: []auth.Grant{
		{Role: "viewer", Cluster: "*", Action: "read", Pattern: "*"},
		{Role: "viewer", Cluster: "*", Action: "schema-read", Pattern: "*"},
		{Role: "viewer", Cluster: "*", Action: "connector-read", Pattern: "*"},
	}})
	a.demo.User.Roles = []string{"viewer"}
	return a
}

func TestReadOnlyRoleCannotMutate(t *testing.T) {
	a := viewerAPI(t)
	for _, c := range []struct{ method, path, body string }{
		{"POST", "/api/v1/clusters/demo/topics", `{"name":"x","partitions":1,"replicationFactor":1}`},
		{"DELETE", "/api/v1/clusters/demo/topics/orders.created", ""},
		{"PUT", "/api/v1/clusters/demo/topics/orders.created/config", `{}`},
		{"POST", "/api/v1/clusters/demo/topics/orders.created/truncate", `{}`},
		{"POST", "/api/v1/clusters/demo/consumer-groups/billing/reset-offsets", `{}`},
		{"POST", "/api/v1/clusters/demo/messages", `{"topic":"orders.created","value":"x"}`},
		{"POST", "/api/v1/clusters/demo/rebalances", `{}`},
		{"POST", "/api/v1/clusters/demo/acls", `{}`},
		{"PUT", "/api/v1/clusters/demo/name", `{"name":"x"}`},
		{"GET", "/api/v1/audit", ""},
		{"GET", "/api/v1/admin/sessions", ""},
	} {
		r := httptest.NewRequest(c.method, c.path, strings.NewReader(c.body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		if w.Code != 403 || !strings.Contains(w.Body.String(), `"forbidden"`) {
			t.Fatalf("%s %s: viewer got %d %s", c.method, c.path, w.Code, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/clusters/demo/topics", nil))
	if w.Code != 200 {
		t.Fatalf("viewer cannot read topics: %d", w.Code)
	}
}

func TestSessionReportsPermissionsPerCluster(t *testing.T) {
	a := viewerAPI(t)
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/auth/session", nil))
	var envelope struct {
		Data struct {
			Permissions       map[string][]string `json:"permissions"`
			CanManageSessions bool                `json:"canManageSessions"`
		}
	}
	if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil || w.Code != 200 {
		t.Fatalf("session %d %s", w.Code, w.Body.String())
	}
	if got := envelope.Data.Permissions["demo"]; !slices.Equal(got, []string{"read", "schema-read", "connector-read"}) {
		t.Fatalf("demo permissions %v", got)
	}
	if slices.Contains(envelope.Data.Permissions["*"], "manage-sessions") || envelope.Data.CanManageSessions {
		t.Fatal("viewer reported as session manager")
	}
}

func TestProvidersListOnlyConfiguredSignIn(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Store: s, LDAP: []auth.LDAPConfig{{ID: "directory"}}})
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/auth/providers", nil))
	if strings.Contains(w.Body.String(), `"local"`) || !strings.Contains(w.Body.String(), `"directory"`) {
		t.Fatalf("providers %s", w.Body.String())
	}
}

func TestOIDCCallbackFailureReturnsToLogin(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Store: s, OIDC: map[string]*auth.OIDC{"authentik": {}}})
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/auth/oidc/authentik/callback?state=forged&code=x", nil))
	if w.Code != 302 || w.Header().Get("Location") != "/login?error=sso_state" {
		t.Fatalf("callback %d %q", w.Code, w.Header().Get("Location"))
	}
}
