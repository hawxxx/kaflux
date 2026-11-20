package api

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

func TestAdminAuditRecordsMCPProviderAndSourceIP(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}})
	session := auth.Session{ID: "mcp:tok-1", User: auth.User{ID: "owner-1/ci-bot", Username: "Owner", Roles: []string{"administrator"}, Provider: "mcp"}}
	grants := []auth.Grant{{Role: "administrator", Cluster: "*", Action: "*", Pattern: "*"}}
	r := httptest.NewRequest("POST", "/api/v1/clusters/demo/acls", strings.NewReader(aclTestBody))
	r.RemoteAddr = "203.0.113.9:4711"
	w := httptest.NewRecorder()
	a.ServeHTTP(w, WithMCPSession(r, session, grants))
	if w.Code != 200 {
		t.Fatalf("create %d %s", w.Code, w.Body.String())
	}
	// A console session keeps the cluster kind as provider but gains the source IP.
	r = httptest.NewRequest("POST", "/api/v1/clusters/demo/acls", strings.NewReader(strings.Replace(aclTestBody, "orders.", "billing.", 1)))
	r.RemoteAddr = "198.51.100.7:5000"
	r.Header.Set("X-CSRF-Token", a.demo.CSRF)
	w = httptest.NewRecorder()
	a.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("console create %d %s", w.Code, w.Body.String())
	}
	audits, err := s.Audits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var mcpRows, consoleRows int
	for _, x := range audits {
		if x.Action != "create-acl" {
			continue
		}
		switch x.Actor {
		case "owner-1/ci-bot":
			mcpRows++
			if x.Provider != "mcp" || x.SourceIP != "203.0.113.9" {
				t.Fatalf("mcp audit %+v", x)
			}
		case a.demo.User.ID:
			consoleRows++
			if x.Provider != "development-simulator" || x.SourceIP != "198.51.100.7" {
				t.Fatalf("console audit %+v", x)
			}
		}
	}
	if mcpRows == 0 || consoleRows == 0 {
		t.Fatalf("missing audit rows: mcp %d console %d in %+v", mcpRows, consoleRows, audits)
	}
}
