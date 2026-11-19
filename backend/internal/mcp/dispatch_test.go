package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/api"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

func testPrincipal() Principal {
	return Principal{
		Session:    auth.Session{ID: "owner-session", User: auth.User{ID: "owner-1", Username: "Owner", Roles: []string{"administrator"}, Provider: "local"}},
		Grants:     []auth.Grant{{Role: "administrator", Cluster: "*", Action: "*", Pattern: "*"}},
		TokenID:    "tok-1",
		TokenName:  "ci-bot",
		RemoteAddr: "203.0.113.9:4711",
	}
}

func mustTool(t *testing.T, name string) Tool {
	t.Helper()
	tool, ok := Lookup(name)
	if !ok {
		t.Fatalf("tool %s missing", name)
	}
	return tool
}

func jsonHandler(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

func TestDoFillsPathAndQuery(t *testing.T) {
	var gotPath, gotRaw, gotQuery, gotMethod, gotBody, gotType string
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotRaw, gotQuery, gotMethod = r.URL.Path, r.URL.EscapedPath(), r.URL.RawQuery, r.Method
		b := make([]byte, 4096)
		n, _ := r.Body.Read(b)
		gotBody, gotType = string(b[:n]), r.Header.Get("Content-Type")
		jsonHandler(200, `{"data":{}}`)(w, r)
	})
	d := NewDispatcher(h, Limits{})
	if _, e := d.Do(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_get"), Call{Cluster: "prod eu", Args: map[string]any{"topic": "orders v2"}}); e != nil {
		t.Fatalf("topic get: %+v", e)
	}
	if gotPath != "/api/v1/clusters/prod eu/topics/orders v2" || gotRaw != "/api/v1/clusters/prod%20eu/topics/orders%20v2" || gotMethod != "GET" {
		t.Fatalf("path %q raw %q method %q", gotPath, gotRaw, gotMethod)
	}
	if _, e := d.Do(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_list"), Call{Args: map[string]any{"cluster": "local", "internal": false, "sort": "urp", "search": "a&b"}}); e != nil {
		t.Fatalf("topic list: %+v", e)
	}
	for _, want := range []string{"internal=false", "sort=urp", "q=a%26b", "page=1", "pageSize=50"} {
		if !strings.Contains(gotQuery, want) {
			t.Fatalf("query %q lacks %q", gotQuery, want)
		}
	}
	tool := mustTool(t, "kaflux_acl_create")
	body := map[string]any{"acl": map[string]any{"resourceType": "TOPIC"}, "confirmation": true}
	if _, e := d.Do(context.Background(), testPrincipal(), tool, Call{Cluster: "local", Args: map[string]any{"body": body, "confirmation": "mcp-token"}}); e != nil {
		t.Fatalf("acl create: %+v", e)
	}
	if gotMethod != "POST" || gotType != "application/json" || !strings.Contains(gotBody, `"confirmation":true`) || strings.Contains(gotBody, "mcp-token") {
		t.Fatalf("method %q type %q body %q", gotMethod, gotType, gotBody)
	}
	for name, call := range map[string]Call{
		"unknown arg":   {Cluster: "local", Args: map[string]any{"topic": "a", "bogus": 1}},
		"missing param": {Cluster: "local", Args: map[string]any{}},
		"slash":         {Cluster: "local", Args: map[string]any{"topic": "a/b"}},
		"dotdot":        {Cluster: "..", Args: map[string]any{"topic": "a"}},
		"object query":  {Cluster: "local", Args: map[string]any{"topic": map[string]any{}}},
	} {
		if _, e := d.Do(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_get"), call); e == nil || e.Code != "invalid_argument" {
			t.Fatalf("%s: want invalid_argument, got %+v", name, e)
		}
	}
}

func TestDoRefusesAuthAndAdminPaths(t *testing.T) {
	var hits atomic.Int32
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); jsonHandler(200, `{}`)(w, r) })
	d := NewDispatcher(h, Limits{})
	for _, tool := range []Tool{
		{Name: "bad_login", Method: "POST", Path: "/auth/login", Scope: "write"},
		{Name: "bad_session", Method: "GET", Path: "/auth/session", Scope: "read"},
		{Name: "bad_admin", Method: "GET", Path: "/admin/sessions", Scope: "read"},
		{Name: "bad_tokens", Method: "GET", Path: "/admin/mcp-tokens", Scope: "read"},
		{Name: "bad_root", Method: "GET", Path: "/admin", Scope: "read"},
		{Name: "bad_traversal", Method: "GET", Path: "/clusters/../admin/sessions", Scope: "read"},
	} {
		_, e := d.Do(context.Background(), testPrincipal(), tool, Call{Cluster: "local"})
		if e == nil || e.Code != "internal" || e.Status != 500 {
			t.Fatalf("%s: want internal refusal, got %+v", tool.Name, e)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("handler reached %d times", hits.Load())
	}
}

func TestDoMapsErrors(t *testing.T) {
	cases := []struct {
		tool, code   string
		status       int
		body         string
		hintContains []string
	}{
		{"kaflux_topic_delete", "forbidden", 403, `{"error":{"code":"forbidden","message":"Permission denied"}}`, []string{"kaflux_topic_delete", "DELETE", "local", "destructive"}},
		{"kaflux_topic_get", "not_found", 404, `{"error":{"code":"not_found","message":"Topic not found"}}`, []string{"kaflux_topic_list"}},
		{"kaflux_group_get", "not_found", 404, `{"error":{"code":"not_found","message":"Consumer group not found"}}`, []string{"kaflux_group_list"}},
		{"kaflux_topic_get", "broker_unavailable", 503, `{"error":{"code":"broker_unavailable","message":"Topic unavailable"}}`, []string{"retry"}},
		{"kaflux_topic_get", "internal", 500, `panic: secret internals`, []string{"retry"}},
	}
	for _, c := range cases {
		d := NewDispatcher(jsonHandler(c.status, c.body), Limits{})
		args := map[string]any{"topic": "orders", "group": "g1", "body": map[string]any{"confirmation": "orders"}}
		tool := mustTool(t, c.tool)
		for k := range args {
			if !argAllowed(tool, k) {
				delete(args, k)
			}
		}
		_, e := d.Do(context.Background(), testPrincipal(), tool, Call{Cluster: "local", Args: args})
		if e == nil || e.Code != c.code || e.Status != c.status {
			t.Fatalf("%s %d: got %+v", c.tool, c.status, e)
		}
		for _, s := range c.hintContains {
			if !strings.Contains(e.Hint, s) {
				t.Fatalf("%s %d: hint %q lacks %q", c.tool, c.status, e.Hint, s)
			}
		}
		if strings.Contains(e.Message, "secret") {
			t.Fatalf("internal detail leaked: %q", e.Message)
		}
	}
}

func groupsBody(n int, pad int) string {
	items := make([]string, n)
	for i := range items {
		items[i] = fmt.Sprintf(`{"id":"group-%03d","state":"Stable","pad":%q}`, i, strings.Repeat("x", pad))
	}
	return `{"data":[` + strings.Join(items, ",") + `],"meta":{"source":"test"}}`
}

func decodeList(t *testing.T, raw json.RawMessage) (ids []string, top map[string]json.RawMessage) {
	t.Helper()
	var v struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	_ = json.Unmarshal(raw, &top)
	for _, x := range v.Data {
		ids = append(ids, x.ID)
	}
	return ids, top
}

func TestDoPaginatesLists(t *testing.T) {
	d := NewDispatcher(jsonHandler(200, groupsBody(250, 0)), Limits{})
	tool := mustTool(t, "kaflux_group_list")
	res, e := d.Do(context.Background(), testPrincipal(), tool, Call{Cluster: "local"})
	if e != nil {
		t.Fatal(e)
	}
	ids, top := decodeList(t, res.Body)
	if res.Total != 250 || res.Returned != 50 || len(ids) != 50 || !res.Truncated || ids[0] != "group-000" {
		t.Fatalf("default page: total %d returned %d ids %d truncated %v", res.Total, res.Returned, len(ids), res.Truncated)
	}
	if string(top["nextCursor"]) != `"50"` || string(top["truncated"]) != "true" || string(top["meta"]) != `{"source":"test"}` {
		t.Fatalf("envelope %v", top)
	}
	res, e = d.Do(context.Background(), testPrincipal(), tool, Call{Cluster: "local", Args: map[string]any{"limit": float64(500), "cursor": "40"}})
	if e != nil {
		t.Fatal(e)
	}
	ids, top = decodeList(t, res.Body)
	if res.Returned != 200 || ids[0] != "group-040" || !res.Truncated || string(top["nextCursor"]) != `"240"` {
		t.Fatalf("clamped page: returned %d first %s next %s", res.Returned, ids[0], top["nextCursor"])
	}
	res, _ = d.Do(context.Background(), testPrincipal(), tool, Call{Cluster: "local", Args: map[string]any{"cursor": "240"}})
	ids, top = decodeList(t, res.Body)
	if res.Returned != 10 || res.Truncated || top["nextCursor"] != nil || string(top["truncated"]) != "false" {
		t.Fatalf("last page: returned %d truncated %v ids %d", res.Returned, res.Truncated, len(ids))
	}
	res, _ = d.Do(context.Background(), testPrincipal(), tool, Call{Cluster: "local", Args: map[string]any{"search": "GROUP-12"}})
	if ids, _ = decodeList(t, res.Body); res.Total != 10 || len(ids) != 10 || ids[0] != "group-120" {
		t.Fatalf("search: total %d ids %v", res.Total, ids)
	}
	if _, e = d.Do(context.Background(), testPrincipal(), tool, Call{Cluster: "local", Args: map[string]any{"cursor": "abc"}}); e == nil || e.Code != "invalid_argument" {
		t.Fatalf("bad cursor: %+v", e)
	}

	// Routes that page natively get limit/cursor mapped onto page/pageSize.
	var query string
	native := NewDispatcher(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.RawQuery
		jsonHandler(200, `{"data":[{"name":"t40"},{"name":"t41"}],"meta":{"total":42,"page":3,"pageSize":20}}`)(w, r)
	}), Limits{})
	res, e = native.Do(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_list"), Call{Cluster: "local", Args: map[string]any{"limit": float64(20), "cursor": "40", "search": "t"}})
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(query, "page=3") || !strings.Contains(query, "pageSize=20") || !strings.Contains(query, "q=t") {
		t.Fatalf("native query %q", query)
	}
	if res.Total != 42 || res.Returned != 2 || res.Truncated {
		t.Fatalf("native: total %d returned %d truncated %v", res.Total, res.Returned, res.Truncated)
	}
}

func TestDoTruncatesByBytes(t *testing.T) {
	const cap = 16 << 10
	// About 1 MiB of list items.
	d := NewDispatcher(jsonHandler(200, groupsBody(200, 5000)), Limits{MaxBytes: cap})
	res, e := d.Do(context.Background(), testPrincipal(), mustTool(t, "kaflux_group_list"), Call{Cluster: "local", Args: map[string]any{"limit": float64(200)}})
	if e != nil {
		t.Fatal(e)
	}
	ids, top := decodeList(t, res.Body)
	if len(res.Body) > cap || !res.Truncated || res.Returned != len(ids) || res.Returned == 0 || res.Returned >= 200 || res.Total != 200 {
		t.Fatalf("list: %d bytes truncated %v returned %d total %d", len(res.Body), res.Truncated, res.Returned, res.Total)
	}
	if string(top["nextCursor"]) != fmt.Sprintf(`"%d"`, res.Returned) {
		t.Fatalf("next cursor %s after %d", top["nextCursor"], res.Returned)
	}

	// A 1 MiB body with no list is cut to a valid JSON preview.
	blob := `{"data":{"config":"` + strings.Repeat(`a"\`, 350_000) + `"}}`
	blob = strings.ReplaceAll(blob, `a"\`, `a\"\\`)
	d = NewDispatcher(jsonHandler(200, blob), Limits{MaxBytes: cap})
	res, e = d.Do(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_get"), Call{Cluster: "local", Args: map[string]any{"topic": "orders"}})
	if e != nil {
		t.Fatal(e)
	}
	if len(res.Body) > cap || !res.Truncated || !json.Valid(res.Body) || !strings.Contains(string(res.Body), "narrow") {
		t.Fatalf("blob: %d bytes truncated %v valid %v", len(res.Body), res.Truncated, json.Valid(res.Body))
	}
}

func TestDoTimeout(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
		jsonHandler(200, `{"data":{}}`)(w, r)
	})
	d := NewDispatcher(h, Limits{Timeout: 100 * time.Millisecond})
	start := time.Now()
	_, e := d.Do(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_get"), Call{Cluster: "local", Args: map[string]any{"topic": "orders"}})
	if e == nil || e.Code != "timeout" || e.Status != 504 || !strings.Contains(e.Hint, "retry") {
		t.Fatalf("want timeout, got %+v", e)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("timeout took %v", elapsed)
	}
}

func TestDoSetsAuditIdentity(t *testing.T) {
	s, err := store.New(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	a := api.New(api.Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo"}}})
	d := NewDispatcher(a, Limits{})
	// Produce audits through the REST audit path that records the session
	// provider and source IP.
	topics, e := d.Do(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_list"), Call{Cluster: "demo", Args: map[string]any{"limit": float64(1)}})
	if e != nil {
		t.Fatalf("topic list: %+v", e)
	}
	var listed struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := json.Unmarshal(topics.Body, &listed); err != nil || len(listed.Data) == 0 {
		t.Fatalf("topics %s", topics.Body)
	}
	body := map[string]any{"topic": listed.Data[0].Name, "value": "hello"}
	if _, e := d.Do(context.Background(), testPrincipal(), mustTool(t, "kaflux_message_produce"), Call{Cluster: "demo", Args: map[string]any{"body": body}}); e != nil {
		t.Fatalf("produce: %+v", e)
	}
	audits, err := s.Audits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, x := range audits {
		if x.Action == "produce" {
			found = true
			if x.Actor != "owner-1/ci-bot" || x.Provider != "mcp" || x.SourceIP != "203.0.113.9" {
				t.Fatalf("audit identity %+v", x)
			}
		}
	}
	if !found {
		t.Fatalf("no audit recorded: %+v", audits)
	}

	// The derived session never carries the owner's console session handle or CSRF.
	seen := mcpSession(testPrincipal())
	if seen.User.ID != "owner-1/ci-bot" || seen.User.Provider != "mcp" || seen.User.Username != "Owner" || seen.ID == "owner-session" || seen.CSRF != "" {
		t.Fatalf("derived session %+v", seen)
	}
}
