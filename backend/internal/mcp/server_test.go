package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/hawxxx/kaflux/backend/internal/api"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/integrations"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

var allScopes = []string{"read", "messages", "write", "destructive"}

var adminGrants = []auth.Grant{{Role: "administrator", Cluster: "*", Action: "*", Pattern: "*"}}

// testEnv is the test-only construction path: a store, a REST API over the
// demo provider, the MCP handler mounted on it, and an HTTP server.
type testEnv struct {
	store *store.Store
	api   *api.API
	h     *Handler
	srv   *httptest.Server
	mu    sync.Mutex
	grant []auth.Grant
}

type envConfig struct {
	demo    bool
	writes  bool
	grants  []auth.Grant
	origins []string
	limits  Limits
	// integrations adds a fake schema registry "sr" and Connect "connect".
	integrations bool
}

func newEnv(t *testing.T, cfg envConfig) *testEnv {
	t.Helper()
	s, err := store.New(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.grants == nil {
		cfg.grants = adminGrants
	}
	e := &testEnv{store: s, grant: cfg.grants}
	var ext map[string]*integrations.Client
	if cfg.integrations {
		ext = fakeIntegrations(t)
	}
	e.api = api.New(api.Options{Demo: cfg.demo, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo"}}, Grants: cfg.grants, Integrations: ext, ExtraMetrics: WriteMetrics, Metrics: fakeMetrics, ClusterMetrics: func(string) http.Handler { return fakeMetrics }})
	e.h = NewHandler(Options{Store: s, Handler: e.api, Grants: e.grants, Writes: cfg.writes, AllowedOrigins: cfg.origins, Limits: cfg.limits, ConfirmKey: []byte("test-confirmation-key-0123456789"), Demo: cfg.demo})
	e.api.SetMCP(e.h)
	e.srv = httptest.NewServer(e.api)
	t.Cleanup(e.srv.Close)
	return e
}

// fakeMetrics stands in for the metrics backend.
var fakeMetrics = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"data":[]}`))
})

// fakeIntegrations serves minimal schema registry and Connect answers.
func fakeIntegrations(t *testing.T) map[string]*integrations.Client {
	t.Helper()
	svc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/subjects", "/connectors", "/subjects/orders-value/versions":
			_, _ = w.Write([]byte(`[]`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(svc.Close)
	out := map[string]*integrations.Client{}
	for id, kind := range map[string]string{"sr": "schemas", "connect": "connect"} {
		c, err := integrations.New(integrations.Config{ID: id, ClusterID: "demo", Kind: kind, URL: svc.URL})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		out[id] = c
	}
	return out
}

func (e *testEnv) grants() []auth.Grant {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.grant)
}

func (e *testEnv) setGrants(g []auth.Grant) {
	e.mu.Lock()
	e.grant = g
	e.mu.Unlock()
}

type tokenSpec struct {
	id       string
	roles    []string
	scopes   []string
	clusters []string
	expires  time.Time
}

// token stores an MCP token owned by alice and returns its secret.
func (e *testEnv) token(t *testing.T, spec tokenSpec) string {
	t.Helper()
	if spec.id == "" {
		spec.id = "tok1"
	}
	if spec.roles == nil {
		spec.roles = []string{"administrator"}
	}
	if spec.scopes == nil {
		spec.scopes = allScopes
	}
	if spec.expires.IsZero() {
		spec.expires = time.Now().Add(time.Hour)
	}
	secret, hash := store.NewMCPSecret()
	tok := store.MCPToken{ID: spec.id, Name: "agent-" + spec.id, OwnerID: "alice", OwnerProvider: "local", OwnerRoles: spec.roles, Scopes: spec.scopes, Clusters: spec.clusters, Expires: spec.expires, Created: time.Now()}
	if err := e.store.CreateMCPToken(context.Background(), tok, hash); err != nil {
		t.Fatal(err)
	}
	return secret
}

type bearerTransport struct {
	secret string
	extra  http.Header
}

func (b bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if b.secret != "" {
		r.Header.Set("Authorization", "Bearer "+b.secret)
	}
	for k, v := range b.extra {
		r.Header[k] = v
	}
	return http.DefaultTransport.RoundTrip(r)
}

func (e *testEnv) connect(t *testing.T, secret, query string) (*sdk.ClientSession, error) {
	t.Helper()
	tr := &sdk.StreamableClientTransport{Endpoint: e.srv.URL + "/mcp" + query, HTTPClient: &http.Client{Transport: bearerTransport{secret: secret}}, DisableStandaloneSSE: true, MaxRetries: -1}
	c := sdk.NewClient(&sdk.Implementation{Name: "kaflux-test", Version: "1"}, nil)
	cs, err := c.Connect(context.Background(), tr, nil)
	if err == nil {
		t.Cleanup(func() { _ = cs.Close() })
	}
	return cs, err
}

func (e *testEnv) mustConnect(t *testing.T, secret, query string) *sdk.ClientSession {
	t.Helper()
	cs, err := e.connect(t, secret, query)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return cs
}

func toolNames(t *testing.T, cs *sdk.ClientSession) []string {
	t.Helper()
	var out []string
	for tool, err := range cs.Tools(context.Background(), nil) {
		if err != nil {
			t.Fatalf("list tools: %v", err)
		}
		out = append(out, tool.Name)
	}
	return out
}

func call(t *testing.T, cs *sdk.ClientSession, name string, args map[string]any) *sdk.CallToolResult {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &sdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("call %s: %v", name, err)
	}
	return res
}

func resultText(r *sdk.CallToolResult) string {
	var b strings.Builder
	for _, c := range r.Content {
		if tc, ok := c.(*sdk.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

type toolErr struct {
	Code, Message, Hint string
	Status              int
}

func errorOf(t *testing.T, r *sdk.CallToolResult) toolErr {
	t.Helper()
	if !r.IsError {
		t.Fatalf("expected tool error, got %s", resultText(r))
	}
	var env struct{ Error toolErr }
	if err := json.Unmarshal([]byte(resultText(r)), &env); err != nil {
		t.Fatalf("error text is not JSON: %q", resultText(r))
	}
	return env.Error
}

const initBody = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"raw","version":"1"}}}`

// rawPost sends one JSON-RPC initialize to /mcp with the given headers.
func (e *testEnv) rawPost(t *testing.T, hdr http.Header) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/mcp", strings.NewReader(initBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range hdr {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func TestNoTokenIs401(t *testing.T) {
	e := newEnv(t, envConfig{})
	for name, hdr := range map[string]http.Header{
		"none":       {},
		"not-kfx":    {"Authorization": {"Bearer abc"}},
		"unknown":    {"Authorization": {"Bearer kfx_unknown"}},
		"basic":      {"Authorization": {"Basic YWRtaW46YWRtaW4="}},
		"empty-auth": {"Authorization": {"Bearer "}},
	} {
		resp, body := e.rawPost(t, hdr)
		if resp.StatusCode != 401 || !strings.HasPrefix(resp.Header.Get("WWW-Authenticate"), "Bearer resource_metadata=") {
			t.Errorf("%s: status %d auth %q", name, resp.StatusCode, resp.Header.Get("WWW-Authenticate"))
		}
		if !strings.Contains(body, `"unauthenticated"`) || strings.Contains(body, "expired") || strings.Contains(body, "revoked") {
			t.Errorf("%s: body %s", name, body)
		}
	}
	if _, err := e.connect(t, "", ""); err == nil {
		t.Fatal("SDK client connected without a token")
	}
}

func TestGetIs405(t *testing.T) {
	e := newEnv(t, envConfig{})
	secret := e.token(t, tokenSpec{})
	for _, m := range []string{http.MethodGet, http.MethodDelete} {
		req, _ := http.NewRequest(m, e.srv.URL+"/mcp", nil)
		req.Header.Set("Authorization", "Bearer "+secret)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 405 || resp.Header.Get("Allow") != "POST" {
			t.Errorf("%s: %d %q", m, resp.StatusCode, resp.Header.Get("Allow"))
		}
	}
}

func TestRevokedTokenDeniedNextCall(t *testing.T) {
	e := newEnv(t, envConfig{demo: true})
	secret := e.token(t, tokenSpec{})
	cs := e.mustConnect(t, secret, "")
	if r := call(t, cs, "kaflux_cluster_list", nil); r.IsError {
		t.Fatalf("first call: %s", resultText(r))
	}
	if err := e.store.RevokeMCPToken(context.Background(), "tok1"); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CallTool(context.Background(), &sdk.CallToolParams{Name: "kaflux_cluster_list"}); err == nil {
		t.Fatal("revoked token still served")
	}
	if resp, _ := e.rawPost(t, http.Header{"Authorization": {"Bearer " + secret}}); resp.StatusCode != 401 {
		t.Fatalf("revoked raw status %d", resp.StatusCode)
	}
}

func TestExpiredToken(t *testing.T) {
	e := newEnv(t, envConfig{demo: true})
	secret := e.token(t, tokenSpec{expires: time.Now().Add(-time.Minute)})
	if resp, body := e.rawPost(t, http.Header{"Authorization": {"Bearer " + secret}}); resp.StatusCode != 401 || strings.Contains(body, "expired") {
		t.Fatalf("expired: %d %s", resp.StatusCode, body)
	}
	if _, err := e.connect(t, secret, ""); err == nil {
		t.Fatal("expired token connected")
	}
}

func TestListToolsReadOnlyToken(t *testing.T) {
	e := newEnv(t, envConfig{demo: true, writes: true})
	cs := e.mustConnect(t, e.token(t, tokenSpec{scopes: []string{"read"}}), "")
	names := toolNames(t, cs)
	if !slices.Contains(names, "kaflux_topic_list") {
		t.Fatalf("read tool missing: %v", names)
	}
	for _, n := range names {
		tool, _ := Lookup(n)
		if tool.Scope != "read" {
			t.Errorf("read-only token sees %s (scope %s)", n, tool.Scope)
		}
	}
	reads := 0
	for _, tool := range Catalog() {
		if tool.Scope == "read" {
			reads++
		}
	}
	if len(names) != reads {
		t.Errorf("listed %d tools, want %d read tools", len(names), reads)
	}
}

func TestHiddenWriteToolCallDenied(t *testing.T) {
	e := newEnv(t, envConfig{demo: true, writes: true})
	cs := e.mustConnect(t, e.token(t, tokenSpec{scopes: []string{"read", "write"}}), "")
	for _, name := range []string{"kaflux_topic_delete", "kaflux_message_consume"} {
		r := call(t, cs, name, map[string]any{"cluster": "demo", "topic": "orders.created", "body": map[string]any{"confirmation": "orders.created"}})
		if te := errorOf(t, r); te.Code != "forbidden" || te.Status != 403 {
			t.Errorf("%s: %+v", name, te)
		}
	}
	// Nothing was deleted.
	r := call(t, cs, "kaflux_topic_get", map[string]any{"cluster": "demo", "topic": "orders.created"})
	if r.IsError {
		t.Fatalf("topic gone: %s", resultText(r))
	}
}

func TestToolsetsFilter(t *testing.T) {
	e := newEnv(t, envConfig{demo: true, writes: true})
	cs := e.mustConnect(t, e.token(t, tokenSpec{}), "?toolsets=topic,%20Group")
	names := toolNames(t, cs)
	if len(names) == 0 {
		t.Fatal("no tools")
	}
	sawGroup := false
	for _, n := range names {
		tool, _ := Lookup(n)
		if tool.Domain != "topic" && tool.Domain != "group" {
			t.Errorf("toolset filter let %s through", n)
		}
		sawGroup = sawGroup || tool.Domain == "group"
	}
	if !sawGroup {
		t.Error("group toolset missing")
	}
	if te := errorOf(t, call(t, cs, "kaflux_cluster_list", nil)); te.Status != 403 {
		t.Errorf("filtered tool call: %+v", te)
	}
}

func TestWritesDisabledHidesWriteTools(t *testing.T) {
	e := newEnv(t, envConfig{demo: true, writes: false})
	cs := e.mustConnect(t, e.token(t, tokenSpec{}), "")
	names := toolNames(t, cs)
	for _, n := range names {
		if tool, _ := Lookup(n); tool.IsWrite() {
			t.Errorf("write tool %s listed with writes disabled", n)
		}
	}
	if !slices.Contains(names, "kaflux_message_consume") {
		t.Error("messages tool missing for messages scope")
	}
	r := call(t, cs, "kaflux_topic_create", map[string]any{"cluster": "demo", "body": map[string]any{"name": "x.y", "partitions": 1, "replicationFactor": 1}})
	if te := errorOf(t, r); te.Status != 403 || !strings.Contains(te.Message, "writes are disabled") {
		t.Errorf("write with writes disabled: %+v", te)
	}
}

func TestPrepareThenCommitCreatesTopicInDemo(t *testing.T) {
	e := newEnv(t, envConfig{demo: true, writes: true})
	cs := e.mustConnect(t, e.token(t, tokenSpec{scopes: []string{"read", "write"}}), "")
	args := map[string]any{"cluster": "demo", "body": map[string]any{"name": "mcp.created", "partitions": 3, "replicationFactor": 3}}
	r := call(t, cs, "kaflux_topic_create", args)
	if r.IsError {
		t.Fatalf("prepare: %s", resultText(r))
	}
	var out PrepareOutput
	b, _ := json.Marshal(r.StructuredContent)
	if err := json.Unmarshal(b, &out); err != nil || out.Confirmation == "" || out.Plan.Tool != "kaflux_topic_create" {
		t.Fatalf("prepare output %s", b)
	}
	if g := call(t, cs, "kaflux_topic_get", map[string]any{"cluster": "demo", "topic": "mcp.created"}); !g.IsError {
		t.Fatal("prepare created the topic")
	}
	commit := map[string]any{"cluster": "demo", "body": args["body"], "confirmation": out.Confirmation}
	if r = call(t, cs, "kaflux_topic_create", commit); r.IsError {
		t.Fatalf("commit: %s", resultText(r))
	}
	if g := call(t, cs, "kaflux_topic_get", map[string]any{"cluster": "demo", "topic": "mcp.created"}); g.IsError {
		t.Fatalf("topic not created: %s", resultText(g))
	}
	// Changed arguments with the same confirmation are refused.
	changed := map[string]any{"cluster": "demo", "body": map[string]any{"name": "mcp.other", "partitions": 3, "replicationFactor": 3}, "confirmation": out.Confirmation}
	if te := errorOf(t, call(t, cs, "kaflux_topic_create", changed)); te.Code != "confirmation_mismatch" {
		t.Fatalf("changed args: %+v", te)
	}
}

func TestOriginRejected(t *testing.T) {
	e := newEnv(t, envConfig{demo: true, origins: []string{"https://Console.Example", "http://localhost:8080/"}})
	secret := e.token(t, tokenSpec{})
	authz := []string{"Bearer " + secret}
	host := strings.TrimPrefix(e.srv.URL, "http://")
	for _, c := range []struct {
		name, origin, host string
		want               int
	}{
		{"no origin", "", "", 200},
		{"allowlisted", "https://console.example", "", 200},
		{"allowlisted default port", "https://console.example:443", "", 200},
		{"allowlisted case and slash", "http://LOCALHOST:8080", "", 200},
		{"foreign", "https://evil.example", "", 403},
		{"rebinding: Host matches Origin", "http://evil.example", "evil.example", 403},
		{"same host as request", e.srv.URL, "", 403},
		{"scheme mismatch", "http://console.example", "", 403},
		{"port mismatch", "https://console.example:8443", "", 403},
		{"null", "null", "", 403},
		{"with path", "https://console.example/app", "", 403},
	} {
		hdr := http.Header{"Authorization": authz}
		if c.origin != "" {
			hdr.Set("Origin", c.origin)
		}
		req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/mcp", strings.NewReader(initBody))
		req.Header = hdr
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Host = host
		if c.host != "" {
			req.Host = c.host
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s: status %d, want %d", c.name, resp.StatusCode, c.want)
		}
	}
	// A repeated or blank Origin is malformed, not absent.
	for name, values := range map[string][]string{
		"repeated": {"https://console.example", "https://console.example"},
		"empty":    {""},
		"blank":    {"   "},
	} {
		if resp, _ := e.rawPost(t, http.Header{"Authorization": authz, "Origin": values}); resp.StatusCode != 403 {
			t.Errorf("%s Origin: %d", name, resp.StatusCode)
		}
	}
	// With no configured origins every browser request is refused.
	bare := newEnv(t, envConfig{demo: true})
	if resp, _ := bare.rawPost(t, http.Header{"Authorization": {"Bearer " + bare.token(t, tokenSpec{})}, "Origin": {bare.srv.URL}}); resp.StatusCode != 403 {
		t.Fatalf("origin with empty allowlist: %d", resp.StatusCode)
	}
}

func TestFailedAuthRateLimited(t *testing.T) {
	e := newEnv(t, envConfig{demo: true})
	secret := e.token(t, tokenSpec{})
	var mu sync.Mutex
	lookups := 0
	inner := e.h.lookup
	e.h.lookup = func(ctx context.Context, hash string) (store.MCPToken, error) {
		mu.Lock()
		lookups++
		mu.Unlock()
		return inner(ctx, hash)
	}
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return lookups
	}
	good := http.Header{"Authorization": {"Bearer " + secret}}
	bad := http.Header{"Authorization": {"Bearer kfx_wrong"}}
	for i := 0; i < MaxAuthFailuresPerMinute; i++ {
		if resp, _ := e.rawPost(t, bad); resp.StatusCode != 401 {
			t.Fatalf("failure %d: %d", i, resp.StatusCode)
		}
	}
	resp, _ := e.rawPost(t, bad)
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") != "60" {
		t.Fatalf("flood not limited: %d %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	// A valid token from the same (possibly shared proxy) address still
	// authenticates, with exactly one store lookup per request.
	before := count()
	for i := 0; i < 3; i++ {
		if resp, body := e.rawPost(t, good); resp.StatusCode != 200 {
			t.Fatalf("valid token from limited IP: %d %s", resp.StatusCode, body)
		}
	}
	if got := count() - before; got != 3 {
		t.Fatalf("valid tokens took %d lookups, want 3", got)
	}
	if resp, _ := e.rawPost(t, bad); resp.StatusCode != 429 {
		t.Fatalf("bad token after valid ones: %d", resp.StatusCode)
	}
	l := e.h.authFail
	if l.record("127.0.0.1", time.Now().Add(61*time.Second)) || l.record("10.0.0.9", time.Now()) {
		t.Fatal("limit outlived its window or spilled to another IP")
	}
}

func adminSession(t *testing.T, e *testEnv) auth.Session {
	t.Helper()
	s := auth.NewSession(auth.User{ID: "alice", Username: "alice", Roles: []string{"administrator"}, Provider: "local"})
	if err := e.store.SaveSession(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCookieIgnoredOnMCP(t *testing.T) {
	e := newEnv(t, envConfig{})
	s := adminSession(t, e)
	// The cookie is valid for REST ...
	req, _ := http.NewRequest(http.MethodGet, e.srv.URL+"/api/v1/clusters", nil)
	req.AddCookie(&http.Cookie{Name: "kaflux_session", Value: s.ID})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("cookie REST: %d", resp.StatusCode)
	}
	// ... but /mcp ignores it.
	if resp, _ := e.rawPost(t, http.Header{"Cookie": {"kaflux_session=" + s.ID}, "X-Csrf-Token": {s.CSRF}}); resp.StatusCode != 401 {
		t.Fatalf("cookie on /mcp: %d", resp.StatusCode)
	}
}

func TestBearerRejectedOnREST(t *testing.T) {
	e := newEnv(t, envConfig{})
	secret := e.token(t, tokenSpec{})
	for _, p := range []string{"/api/v1/clusters", "/api/v1/clusters/demo/topics", "/api/v1/auth/session"} {
		req, _ := http.NewRequest(http.MethodGet, e.srv.URL+p, nil)
		req.Header.Set("Authorization", "Bearer "+secret)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 401 {
			t.Errorf("%s with bearer: %d", p, resp.StatusCode)
		}
	}
}

func TestOwnerGrantRemovedDeniesImmediately(t *testing.T) {
	e := newEnv(t, envConfig{demo: true})
	cs := e.mustConnect(t, e.token(t, tokenSpec{}), "")
	args := map[string]any{"cluster": "demo"}
	if r := call(t, cs, "kaflux_topic_list", args); r.IsError {
		t.Fatalf("granted: %s", resultText(r))
	}
	e.setGrants([]auth.Grant{{Role: "someone-else", Cluster: "*", Action: "*", Pattern: "*"}})
	if te := errorOf(t, call(t, cs, "kaflux_topic_list", args)); te.Status != 403 {
		t.Fatalf("after grant removal: %+v", te)
	}
	e.setGrants([]auth.Grant{{Role: "administrator", Cluster: "other", Action: "*", Pattern: "*"}})
	if te := errorOf(t, call(t, cs, "kaflux_topic_list", args)); te.Status != 403 {
		t.Fatalf("grant moved to another cluster: %+v", te)
	}
}

func TestLargeNonListResultIsTextOnly(t *testing.T) {
	e := newEnv(t, envConfig{demo: true, limits: Limits{MaxBytes: 300}})
	cs := e.mustConnect(t, e.token(t, tokenSpec{}), "")
	r := call(t, cs, "kaflux_cluster_overview", map[string]any{"cluster": "demo"})
	if r.IsError {
		t.Fatalf("overview: %s", resultText(r))
	}
	if r.StructuredContent != nil {
		t.Fatalf("truncated non-list result carries structured content: %v", r.StructuredContent)
	}
	var v map[string]any
	if err := json.Unmarshal([]byte(resultText(r)), &v); err != nil || v["truncated"] != true || v["hint"] == nil {
		t.Fatalf("text %q", resultText(r))
	}
}

func TestListResultReportsCounts(t *testing.T) {
	e := newEnv(t, envConfig{demo: true})
	cs := e.mustConnect(t, e.token(t, tokenSpec{}), "")
	r := call(t, cs, "kaflux_topic_list", map[string]any{"cluster": "demo", "limit": 2})
	if r.IsError {
		t.Fatalf("list: %s", resultText(r))
	}
	sc, _ := r.StructuredContent.(map[string]any)
	if sc["returned"] != float64(2) || sc["truncated"] != true || sc["total"].(float64) < 3 || sc["nextCursor"] != "2" {
		t.Fatalf("structured %v", sc)
	}
	if !strings.HasPrefix(resultText(r), "Returned 2 of ") {
		t.Fatalf("text %q", resultText(r))
	}
}

func TestMetricsExposed(t *testing.T) {
	e := newEnv(t, envConfig{demo: true})
	secret := e.token(t, tokenSpec{id: "tok-metrics", scopes: []string{"read"}})
	cs := e.mustConnect(t, secret, "")
	call(t, cs, "kaflux_cluster_list", nil)
	call(t, cs, "kaflux_message_consume", map[string]any{"cluster": "demo", "topic": "orders.created"})
	e.rawPost(t, http.Header{})
	resp, err := http.Get(e.srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	text := string(b)
	for _, want := range []string{
		`kaflux_mcp_calls_total{tool="kaflux_cluster_list",result="ok"}`,
		`kaflux_mcp_calls_total{tool="kaflux_message_consume",result="denied"}`,
		`kaflux_mcp_denied_total{reason="scope"}`,
		`kaflux_mcp_denied_total{reason="unauthenticated"}`,
		"kaflux_mcp_call_seconds_count",
		"kaflux_configured_clusters 1",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("metrics lack %s", want)
		}
	}
	if strings.Contains(text, "tok-metrics") || strings.Contains(text, secret) {
		t.Error("metrics leak token identity")
	}
}

// persona is one caller in the authorization matrix: the configured grants,
// the owner's roles, and the token's scopes and cluster allowlist.
type persona struct {
	name     string
	grants   []auth.Grant
	roles    []string
	scopes   []string
	clusters []string
}

func roleGrants(role string, actions ...string) []auth.Grant {
	var out []auth.Grant
	for _, a := range actions {
		out = append(out, auth.Grant{Role: role, Cluster: "*", Action: a, Pattern: "*"})
	}
	return out
}

func matrixPersonas() []persona {
	out := []persona{
		{name: "viewer-only", grants: roleGrants("viewer", "read", "schema-read", "connector-read"), roles: []string{"viewer"}, scopes: allScopes},
		{name: "operator", grants: roleGrants("operator", "read", "schema-read", "connector-read", "consume", "produce", "create", "alter-config", "plan", "connector-update", "schema-update"), roles: []string{"operator"}, scopes: allScopes},
		{name: "admin", grants: adminGrants, roles: []string{"administrator"}, scopes: allScopes},
		{name: "token-narrowed", grants: adminGrants, roles: []string{"administrator"}, scopes: []string{"read", "write"}},
		{name: "other-cluster-allowlist", grants: adminGrants, roles: []string{"administrator"}, scopes: allScopes, clusters: []string{"other"}},
	}
	// One persona per mutating action, so a write tool whose REST route
	// authorizes with an unexpected action shows up as a disagreement.
	for _, a := range append(slices.Clone(ScopeActions["write"]), ScopeActions["destructive"]...) {
		out = append(out, persona{name: "only-" + a, grants: roleGrants("r", "read", "schema-read", "connector-read", a), roles: []string{"r"}, scopes: allScopes})
	}
	return out
}

// matrixValues are placeholder path values naming demo resources.
var matrixValues = map[string]any{
	"cluster": "demo", "topic": "orders.created", "group": "demo-order-service", "connector": "demo-connector",
	"job": "job-1", "metric": "bytes-in", "subject": "orders-value", "task": "0", "version": "1",
}

// adminCannotSucceed lists tools the admin persona cannot complete against
// the demo cluster with matrix arguments, and why.
var adminCannotSucceed = map[string]string{
	"kaflux_topic_bulk_delete":         "needs per-topic fingerprints returned by kaflux_topic_unused in the same run",
	"kaflux_rebalance_get":             matrixNoJob,
	"kaflux_rebalance_events":          matrixNoJob,
	"kaflux_rebalance_dry_run":         matrixNoJob,
	"kaflux_rebalance_pause":           matrixNoJob,
	"kaflux_rebalance_resume":          matrixNoJob,
	"kaflux_rebalance_cancel":          matrixNoJob,
	"kaflux_rebalance_cancel_rollback": matrixNoJob,
	"kaflux_rebalance_throttle":        matrixNoJob,
	"kaflux_rebalance_execute":         matrixNoJob,
	"kaflux_rebalance_rollback":        matrixNoJob,
}

const matrixNoJob = "needs a rebalance job id generated earlier in the same run (and, for lifecycle steps, a job in the matching state); the fixed matrix job id does not exist"

// matrixBodies are valid request bodies for tools whose REST route rejects
// an empty body before it authorizes.
var matrixBodies = map[string]map[string]any{
	"kaflux_topic_copy":                 {"name": "orders.copy", "partitions": 3, "replicationFactor": 3},
	"kaflux_schema_compatibility_check": {"confirmation": true, "payload": map[string]any{"schema": "{}"}},
	"kaflux_message_search":             {"topic": "orders.created", "match": map[string]any{"text": "ORD"}, "maxMatches": 5},
	"kaflux_topic_create":               {"name": "matrix.created", "partitions": 3, "replicationFactor": 3},
	"kaflux_topic_config_update":        {"config": map[string]any{"retention.ms": "86400000"}, "confirmation": true},
	"kaflux_topic_partitions_add":       {"count": 24, "confirmation": true},
	"kaflux_topic_delete":               {"confirmation": "shipping.dispatched"},
	"kaflux_topic_truncate":             {"confirmation": "notifications.email"},
	"kaflux_topic_recreate":             {"confirmation": "events.audit"},
	"kaflux_group_reset_offsets":        {"mode": "earliest", "topics": []any{"orders.created"}, "preview": true},
	"kaflux_acl_create":                 {"acl": map[string]any{"resourceType": "TOPIC", "resourceName": "orders.created", "patternType": "LITERAL", "principal": "User:alice", "host": "*", "operation": "READ", "permission": "ALLOW"}, "confirmation": true},
	"kaflux_schema_register":            {"confirmation": true, "payload": map[string]any{"schemaType": "AVRO", "schema": `"string"`}},
	"kaflux_schema_config_update":       {"confirmation": true, "payload": map[string]any{"compatibility": "BACKWARD"}},
	"kaflux_connector_upsert":           {"confirmation": true, "payload": map[string]any{"connector.class": "FileStreamSink", "topics": "orders.created"}},
	"kaflux_connector_config_update":    {"confirmation": true, "payload": map[string]any{"connector.class": "FileStreamSink", "topics": "orders.created"}},
	"kaflux_connector_pause":            {"confirmation": true},
	"kaflux_connector_resume":           {"confirmation": true},
	"kaflux_connector_restart":          {"confirmation": true},
	"kaflux_connector_task_restart":     {"confirmation": true},
	"kaflux_message_produce":            {"topic": "orders.created", "key": "42", "value": `{"id":42}`},
	"kaflux_rebalance_plan":             {"topics": []any{"orders.created"}, "brokers": []any{1, 2, 3, 4, 5, 6}, "rackAware": true, "throttleBytesPerSec": 10485760},
	"kaflux_schema_subject_delete":      {"confirmation": true},
	"kaflux_schema_version_delete":      {"confirmation": true},
	"kaflux_connector_delete":           {"confirmation": true},
	"kaflux_acl_delete":                 {"acl": map[string]any{"resourceType": "TOPIC", "resourceName": "orders.created", "patternType": "LITERAL", "principal": "User:alice", "host": "*", "operation": "READ", "permission": "ALLOW"}, "confirmation": true},
}

// matrixOverrides replace or add arguments, so destructive tools act on
// their own targets and need not run in a particular order.
var matrixOverrides = map[string]map[string]any{
	"kaflux_message_consume":     {"topic": "orders.created", "partition": 0, "offset": 0},
	"kaflux_metrics_query":       {"metric": "bytes-in"},
	"kaflux_topic_delete":        {"topic": "shipping.dispatched"},
	"kaflux_topic_truncate":      {"topic": "notifications.email"},
	"kaflux_topic_recreate":      {"topic": "events.audit"},
	"kaflux_group_reset_offsets": {"group": "demo-inactive"},
}

func matrixArgs(t *testing.T, tool Tool) map[string]any {
	args := map[string]any{}
	s := tool.InputSchema()
	req, _ := s["required"].([]string)
	if b, ok := matrixBodies[tool.Name]; ok {
		args["body"] = b
	}
	for _, name := range req {
		switch {
		case name == "body" && matrixBodies[tool.Name] != nil:
			args["body"] = matrixBodies[tool.Name]
			continue
		case name == "body":
			args["body"] = map[string]any{}
			continue
		case name == "integration" && tool.Domain == "schema":
			args[name] = "sr"
			continue
		case name == "integration":
			args[name] = "connect"
			continue
		}
		v, ok := matrixValues[name]
		if !ok {
			t.Fatalf("%s: no matrix value for required %q", tool.Name, name)
		}
		args[name] = v
	}
	for k, v := range matrixOverrides[tool.Name] {
		args[k] = v
	}
	return args
}

// restDenied calls the REST route for tool as a cookie session of the
// persona's owner, authorized by the persona's effective grants.
func restDenied(t *testing.T, e *testEnv, s auth.Session, tool Tool, args map[string]any) (bool, int) {
	t.Helper()
	d := NewDispatcher(e.api, Limits{})
	built, terr := d.buildRequest(context.Background(), Principal{TokenID: "matrix"}, tool, Call{Cluster: "demo", Args: args}, nil)
	if terr != nil {
		t.Fatalf("%s: build REST request: %+v", tool.Name, terr)
	}
	var body io.Reader
	if built.Body != nil {
		b, _ := io.ReadAll(built.Body)
		body = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(built.Method, e.srv.URL+built.URL.RequestURI(), body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CSRF-Token", s.CSRF)
	req.AddCookie(&http.Cookie{Name: "kaflux_session", Value: s.ID})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode == 403, resp.StatusCode
}

// mcpDenied calls tool over MCP; writes go through prepare then commit.
func mcpDenied(t *testing.T, cs *sdk.ClientSession, tool Tool, args map[string]any) (bool, string) {
	t.Helper()
	r := call(t, cs, tool.Name, args)
	if tool.IsWrite() && !r.IsError {
		conf, _ := r.StructuredContent.(map[string]any)["confirmation"].(string)
		commit := map[string]any{"confirmation": conf}
		for k, v := range args {
			commit[k] = v
		}
		r = call(t, cs, tool.Name, commit)
	}
	if !r.IsError {
		return false, "ok"
	}
	te := errorOf(t, r)
	return te.Status == 403, te.Code
}

func TestAuthMatrix(t *testing.T) {
	for _, p := range matrixPersonas() {
		t.Run(p.name, func(t *testing.T) {
			// Each side gets its own demo cluster; both see the same call
			// sequence, so their states evolve alike.
			viaMCP := newEnv(t, envConfig{writes: true, grants: p.grants, integrations: true})
			// The REST side's expected grants come from production Narrow,
			// which has its own tests (scopes_test.go); this matrix checks
			// that MCP applies them exactly as REST does.
			effective := Narrow(p.grants, p.scopes, p.clusters)
			if len(effective) == 0 {
				effective = []auth.Grant{{Role: "nobody", Cluster: "*", Action: "read", Pattern: "*"}}
			}
			viaREST := newEnv(t, envConfig{writes: true, grants: effective, integrations: true})
			cs := viaMCP.mustConnect(t, viaMCP.token(t, tokenSpec{roles: p.roles, scopes: p.scopes, clusters: p.clusters}), "")
			s := auth.NewSession(auth.User{ID: "alice", Username: "alice", Roles: p.roles, Provider: "local"})
			if err := viaREST.store.SaveSession(context.Background(), s); err != nil {
				t.Fatal(err)
			}
			for _, tool := range Catalog() {
				args := matrixArgs(t, tool)
				restNo, status := restDenied(t, viaREST, s, tool, args)
				// The token's scope is an MCP-side ceiling on top of RBAC: a
				// tool whose scope the token lacks is denied by design even
				// where the narrowed grants would let REST through (for
				// example acl_delete is destructive but REST needs only
				// manage-acls, which the write scope carries).
				if !slices.Contains(p.scopes, tool.Scope) {
					restNo = true
				}
				mcpNo, code := mcpDenied(t, cs, tool, args)
				if restNo != mcpNo {
					t.Errorf("tool %s persona %s: REST denied=%v (HTTP %d), MCP denied=%v (%s)", tool.Name, p.name, restNo, status, mcpNo, code)
				}
				// The admin must succeed, so agreement cannot rest on both
				// sides rejecting placeholder arguments alike.
				_, excused := adminCannotSucceed[tool.Name]
				if p.name == "admin" && (code == "ok") == excused {
					t.Errorf("tool %s persona admin: MCP %s, REST HTTP %d; want success unless listed in adminCannotSucceed", tool.Name, code, status)
				}
			}
		})
	}
}
