package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/api"
	"github.com/hawxxx/kaflux/backend/internal/config"
	"github.com/hawxxx/kaflux/backend/internal/httpgzip"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"github.com/hawxxx/kaflux/backend/internal/telemetry"
)

const mcpInitialize = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"wiring-test","version":"1"}}}`

func mcpConfig(enabled, writes, demo bool) config.Config {
	return config.Config{Demo: demo, MCP: config.MCP{Enabled: enabled, Writes: writes, MaxResponseBytes: 262144, CallTimeout: 30 * time.Second, RateLimitPerMinute: 120, ConcurrencyPerToken: 4}}
}

func newWiredServer(t *testing.T, cfg config.Config) (*httptest.Server, *store.Store) {
	t.Helper()
	s, err := store.New(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	demo := kafka.NewDemo()
	t.Cleanup(demo.Close)
	a := newAPI(cfg, api.Options{Demo: cfg.Demo, Store: s, Providers: map[string]kafka.Provider{"demo": demo}, Clusters: []model.Cluster{{ID: "demo", Mode: "demo"}}})
	srv := httptest.NewServer(a)
	t.Cleanup(srv.Close)
	return srv, s
}

func storeToken(t *testing.T, s *store.Store) string {
	t.Helper()
	secret, hash := store.NewMCPSecret()
	tok := store.MCPToken{ID: "tok1", Name: "agent", OwnerID: "alice", OwnerProvider: "local", OwnerRoles: []string{"administrator"}, Scopes: []string{"read"}, Expires: time.Now().Add(time.Hour), Created: time.Now()}
	if err := s.CreateMCPToken(context.Background(), tok, hash); err != nil {
		t.Fatal(err)
	}
	return secret
}

func postMCP(t *testing.T, url, secret string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url+"/mcp", strings.NewReader(mcpInitialize))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if secret != "" {
		req.Header.Set("Authorization", "Bearer "+secret)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestBuildMCPDisabledReturnsNil(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	defer s.Close()
	a := api.New(api.Options{Store: s})
	if h := buildMCP(mcpConfig(false, false, false), a, s); h != nil {
		t.Fatal("disabled MCP built a handler")
	}
}

func TestMCPDisabledNotMounted(t *testing.T) {
	for _, demo := range []bool{false, true} {
		srv, s := newWiredServer(t, mcpConfig(false, false, demo))
		if code, body := postMCP(t, srv.URL, storeToken(t, s)); code != http.StatusNotFound {
			t.Fatalf("demo=%v: disabled /mcp answered %d %s", demo, code, body)
		}
	}
}

func TestMCPEnabledServesInitialize(t *testing.T) {
	for _, demo := range []bool{false, true} {
		srv, s := newWiredServer(t, mcpConfig(true, false, demo))
		if code, body := postMCP(t, srv.URL, ""); code != http.StatusUnauthorized {
			t.Fatalf("demo=%v: no token answered %d %s", demo, code, body)
		}
		code, body := postMCP(t, srv.URL, storeToken(t, s))
		if code != http.StatusOK {
			t.Fatalf("demo=%v: initialize answered %d %s", demo, code, body)
		}
		var rpc struct {
			Result struct {
				ServerInfo struct{ Name string } `json:"serverInfo"`
			} `json:"result"`
		}
		if err := json.Unmarshal([]byte(body), &rpc); err != nil || rpc.Result.ServerInfo.Name == "" {
			t.Fatalf("demo=%v: initialize result %q (%v)", demo, body, err)
		}
	}
}

func TestMCPWiringReportsMetaAndMetrics(t *testing.T) {
	for _, tc := range []struct{ enabled, writes, wantWrites bool }{{false, true, false}, {true, false, false}, {true, true, true}} {
		srv, _ := newWiredServer(t, mcpConfig(tc.enabled, tc.writes, true))
		resp, err := http.Get(srv.URL + "/api/v1/admin/mcp-tokens")
		if err != nil {
			t.Fatal(err)
		}
		var list struct {
			Meta struct {
				Enabled       bool `json:"enabled"`
				WritesEnabled bool `json:"writesEnabled"`
			} `json:"meta"`
		}
		err = json.NewDecoder(resp.Body).Decode(&list)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("token list %d %v", resp.StatusCode, err)
		}
		if list.Meta.Enabled != tc.enabled || list.Meta.WritesEnabled != tc.wantWrites {
			t.Fatalf("%+v: meta %+v", tc, list.Meta)
		}
		resp, err = http.Get(srv.URL + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if got := strings.Contains(string(b), "kaflux_mcp_"); got != tc.enabled {
			t.Fatalf("%+v: MCP metrics exposed=%v", tc, got)
		}
	}
}

func TestMCPToolCount(t *testing.T) {
	read, all := mcpToolCount(false), mcpToolCount(true)
	if read == 0 || all <= read {
		t.Fatalf("tool counts read=%d all=%d", read, all)
	}
}

// The server-wide WriteTimeout must not cut off an MCP call that runs longer;
// the extension has to reach the connection through the telemetry and gzip
// wrappers main installs.
func TestMCPWriteDeadlineOutlastsServerTimeout(t *testing.T) {
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = io.WriteString(w, "done")
	})
	srv := httptest.NewUnstartedServer(telemetry.NewHTTP(httpgzip.Handler(extendWriteDeadline(slow, 5*time.Second)), nil))
	srv.Config.WriteTimeout = 100 * time.Millisecond
	srv.Start()
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/mcp")
	if err != nil {
		t.Fatalf("response cut off: %v", err)
	}
	defer resp.Body.Close()
	if b, _ := io.ReadAll(resp.Body); string(b) != "done" {
		t.Fatalf("body %q", b)
	}
}
