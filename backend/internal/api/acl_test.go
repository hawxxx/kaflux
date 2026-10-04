package api

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"net/http/httptest"
	"strings"
	"testing"
)

const aclTestBody = `{"acl":{"resourceType":"TOPIC","resourceName":"orders.","patternType":"PREFIXED","principal":"User:test-operator","host":"*","operation":"READ","permission":"ALLOW"},"confirmation":true}`

func TestACLLifecycleAndScopedAuthorization(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}})
	request := func(method, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, "/api/v1/clusters/demo/acls", strings.NewReader(body))
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		a.ServeHTTP(w, r)
		return w
	}
	created := request("POST", aclTestBody)
	if created.Code != 200 {
		t.Fatalf("create %d %s", created.Code, created.Body.String())
	}
	listed := request("GET", "")
	if listed.Code != 200 || !strings.Contains(listed.Body.String(), `"principal":"User:test-operator"`) || !strings.Contains(listed.Body.String(), `"raw"`) {
		t.Fatalf("list %d %s", listed.Code, listed.Body.String())
	}
	a.o.Grants = []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "read", Pattern: "payments.*"}}
	a.demo.User.Roles = []string{"viewer"}
	listed = request("GET", "")
	if listed.Code != 200 || strings.Contains(listed.Body.String(), "test-operator") {
		t.Fatalf("scoped list leak %d %s", listed.Code, listed.Body.String())
	}
	denied := request("DELETE", aclTestBody)
	if denied.Code != 403 {
		t.Fatal("viewer ACL deletion accepted")
	}
	a.o.Grants = []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "manage-acls", Pattern: "orders.?"}}
	denied = request("DELETE", strings.Replace(aclTestBody, `"orders."`, `"orders.a"`, 1))
	if denied.Code != 403 {
		t.Fatal("prefix exceeds resource grant")
	}
	a.o.Grants = []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "manage-acls", Pattern: "orders.*"}}
	deleted := request("DELETE", aclTestBody)
	if deleted.Code != 200 {
		t.Fatalf("delete %d %s", deleted.Code, deleted.Body.String())
	}
	events, _ := s.Audits(context.Background())
	if len(events) != 4 {
		t.Fatalf("want audited mutation intent/result, got %d", len(events))
	}
}
