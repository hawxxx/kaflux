package mcp

import (
	"context"
	"slices"
	"strings"
	"sync"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// APIPrefix is the REST mount point; Tool.Path is relative to it, exactly as
// the paths appear in docs/openapi.yaml.
const APIPrefix = "/api/v1"

// Tool describes one MCP tool backed by exactly one REST operation.
type Tool struct {
	Name, Domain, Description string
	Method, Path              string // OpenAPI path template, e.g. /clusters/{cluster}/topics
	Scope                     string // read|messages|write|destructive: the MCP-side ceiling
	// Actions overrides the RBAC actions a prepare pre-checks, for routes the
	// REST API authorizes with an action outside Scope's (for example ACL
	// deletion needs manage-acls). Empty means ScopeActions[Scope].
	Actions               []string
	ReadOnly, Destructive bool
	Idempotent            bool
	Paginated             bool // dispatch adds limit/cursor handling
	Search                bool // dispatch adds a substring name filter; only where it maps onto the route
	// Preview optionally computes what a write would do before confirmation.
	Preview func(ctx context.Context, d *Dispatcher, c Call) (any, error)
}

// Call is one tool invocation: the target cluster and the decoded arguments.
type Call struct {
	Cluster string
	Args    map[string]any
}

// Excluded lists the REST operations deliberately not exposed as tools.
var Excluded = map[string]string{
	"GET /admin/sessions":                                  "session administration is an administrator-only console capability",
	"POST /admin/sessions/{handle}/revoke":                 "session administration is an administrator-only console capability",
	"GET /auth/providers":                                  "identity flow; MCP authenticates with bearer tokens only",
	"GET /auth/session":                                    "identity flow; MCP authenticates with bearer tokens only",
	"POST /auth/login":                                     "identity flow; a token must never create console sessions",
	"POST /auth/logout":                                    "identity flow; MCP authenticates with bearer tokens only",
	"GET /auth/oidc/{provider}/start":                      "browser OIDC redirect, not an API operation",
	"GET /auth/oidc/{provider}/callback":                   "browser OIDC redirect, not an API operation",
	"POST /clusters/{cluster}/test":                        "connection test is an administrator console action",
	"GET /clusters/{cluster}/rebalances/{job}/events.txt":  "file download; same data via kaflux_rebalance_events",
	"GET /clusters/{cluster}/rebalances/{job}/backup.json": "file download; same data via kaflux_rebalance_get",
	"GET /clusters/{cluster}/rebalances/{job}/report.csv":  "file download; same data via kaflux_rebalance_get",
}

// Catalog returns every tool, reads first, in a stable order.
func Catalog() []Tool {
	tools := append(slices.Clone(readTools), writeTools...)
	for i := range tools {
		t := &tools[i]
		t.Domain = domainOf(t.Name)
		switch t.Scope {
		case "read", "messages":
			t.ReadOnly, t.Destructive, t.Idempotent = true, false, true
		case "write":
			t.ReadOnly, t.Destructive = false, false
			t.Idempotent = t.Method == "PUT"
		case "destructive":
			t.ReadOnly, t.Destructive = false, true
			t.Idempotent = t.Method == "PUT" || t.Method == "DELETE"
		}
	}
	return tools
}

var byName = sync.OnceValue(func() map[string]Tool {
	m := map[string]Tool{}
	for _, t := range Catalog() {
		m[t.Name] = t
	}
	return m
})

// Lookup finds a catalog tool by name.
func Lookup(name string) (Tool, bool) {
	t, ok := byName()[name]
	return t, ok
}

// domainOf takes the segment after "kaflux_"; the verb is the rest.
func domainOf(name string) string {
	rest := strings.TrimPrefix(name, "kaflux_")
	domain, _, _ := strings.Cut(rest, "_")
	return domain
}

// IsWrite reports whether the tool mutates state and so needs confirmation.
func (t Tool) IsWrite() bool { return t.Scope == "write" || t.Scope == "destructive" }

// SDKTool renders the tool for the MCP SDK.
func (t Tool) SDKTool() *sdk.Tool {
	destructive := t.Destructive
	out := &sdk.Tool{
		Name:        t.Name,
		Description: t.Description,
		InputSchema: t.InputSchema(),
		Annotations: &sdk.ToolAnnotations{
			ReadOnlyHint:    t.ReadOnly,
			DestructiveHint: &destructive,
			IdempotentHint:  t.Idempotent,
		},
	}
	if s := t.OutputSchema(); s != nil {
		out.OutputSchema = s
	}
	return out
}
