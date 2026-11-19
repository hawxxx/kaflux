package mcp

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"gopkg.in/yaml.v3"
)

// specOperations lists every "METHOD /path" in the embedded OpenAPI document.
func specOperations(t *testing.T) []string {
	t.Helper()
	var doc struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(openapiSpec, &doc); err != nil {
		t.Fatal(err)
	}
	var ops []string
	for p, item := range doc.Paths {
		for m := range item {
			switch m {
			case "get", "put", "post", "delete", "patch", "head", "options", "trace":
				ops = append(ops, strings.ToUpper(m)+" "+p)
			}
		}
	}
	slices.Sort(ops)
	return ops
}

func TestCatalogParity(t *testing.T) {
	ops := specOperations(t)
	if len(ops) == 0 {
		t.Fatal("no operations in embedded OpenAPI")
	}
	mapped := map[string]string{}
	for _, tool := range Catalog() {
		key := tool.Method + " " + tool.Path
		if prev, ok := mapped[key]; ok {
			t.Errorf("%s mapped by both %s and %s", key, prev, tool.Name)
		}
		mapped[key] = tool.Name
		if !slices.Contains(ops, key) {
			t.Errorf("%s: %s is not an OpenAPI operation", tool.Name, key)
		}
		if _, ok := Excluded[key]; ok {
			t.Errorf("%s: %s is both mapped and excluded", tool.Name, key)
		}
	}
	for key, reason := range Excluded {
		if !slices.Contains(ops, key) {
			t.Errorf("excluded %s is not an OpenAPI operation", key)
		}
		if strings.TrimSpace(reason) == "" {
			t.Errorf("excluded %s has no reason", key)
		}
	}
	for _, op := range ops {
		_, isMapped := mapped[op]
		_, isExcluded := Excluded[op]
		if !isMapped && !isExcluded {
			t.Errorf("OpenAPI operation %s is neither in Catalog() nor Excluded", op)
		}
	}
}

func TestCatalogNeverExposesIdentityOrSessionRoutes(t *testing.T) {
	for _, tool := range Catalog() {
		for _, prefix := range []string{"/auth/", "/admin/"} {
			if strings.HasPrefix(tool.Path, prefix) {
				t.Errorf("%s exposes %s", tool.Name, tool.Path)
			}
		}
		if tool.Path == "/clusters/{cluster}/test" {
			t.Errorf("%s exposes the connection test", tool.Name)
		}
	}
	for _, op := range specOperations(t) {
		path := strings.SplitN(op, " ", 2)[1]
		if strings.HasPrefix(path, "/auth/") || strings.HasPrefix(path, "/admin/") {
			if _, ok := Excluded[op]; !ok {
				t.Errorf("%s must be excluded", op)
			}
		}
	}
}

var toolName = regexp.MustCompile(`^kaflux_[a-z0-9_]+$`)

func TestCatalogNamesUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, tool := range Catalog() {
		if !toolName.MatchString(tool.Name) || len(tool.Name) > 64 {
			t.Errorf("invalid tool name %q", tool.Name)
		}
		if seen[tool.Name] {
			t.Errorf("duplicate tool name %q", tool.Name)
		}
		seen[tool.Name] = true
		if tool.Domain == "" || !strings.HasPrefix(tool.Name, "kaflux_"+tool.Domain+"_") {
			t.Errorf("%s: domain %q does not prefix the name", tool.Name, tool.Domain)
		}
		if _, ok := ScopeActions[tool.Scope]; !ok {
			t.Errorf("%s: unknown scope %q", tool.Name, tool.Scope)
		}
	}
}

func isWrite(tool Tool) bool { return tool.Scope == "write" || tool.Scope == "destructive" }

func TestWritesHaveConfirmationParam(t *testing.T) {
	for _, tool := range Catalog() {
		props, _ := tool.InputSchema()["properties"].(map[string]any)
		_, has := props["confirmation"]
		if isWrite(tool) && !has {
			t.Errorf("%s: write tool lacks confirmation parameter", tool.Name)
		}
		if !isWrite(tool) && has {
			t.Errorf("%s: read tool has a confirmation parameter", tool.Name)
		}
		if has && slices.Contains(requiredOf(tool.InputSchema()), "confirmation") {
			t.Errorf("%s: confirmation must be optional (prepare call omits it)", tool.Name)
		}
		if tool.Preview != nil && !isWrite(tool) {
			t.Errorf("%s: preview on a read tool", tool.Name)
		}
	}
}

func requiredOf(s map[string]any) []string {
	r, _ := s["required"].([]string)
	return r
}

func TestReadToolsAreReadOnlyAnnotated(t *testing.T) {
	for _, tool := range Catalog() {
		switch tool.Scope {
		case "read", "messages":
			if !tool.ReadOnly || tool.Destructive {
				t.Errorf("%s: read tool must be ReadOnly and not Destructive", tool.Name)
			}
		case "write":
			if tool.ReadOnly || tool.Destructive {
				t.Errorf("%s: write tool must be neither ReadOnly nor Destructive", tool.Name)
			}
		case "destructive":
			if tool.ReadOnly || !tool.Destructive {
				t.Errorf("%s: destructive tool must be Destructive", tool.Name)
			}
		}
		if tool.Method == "GET" && tool.Scope != "read" && tool.Scope != "messages" {
			t.Errorf("%s: GET operation with write scope %q", tool.Name, tool.Scope)
		}
	}
}

// readOnlyPosts are the only non-GET operations exposed below write scope;
// each needs a read-level REST action.
var readOnlyPosts = map[string]string{
	"POST /clusters/{cluster}/schemas/{integration}/{subject}/compatibility": "read",
	"POST /clusters/{cluster}/messages/search":                               "messages",
}

// mustBeDestructive pins tools whose REST action (execute, rollback, delete,
// reset-offsets) lives only in the destructive scope.
var mustBeDestructive = []string{
	"kaflux_rebalance_pause", "kaflux_rebalance_resume", "kaflux_rebalance_cancel",
	"kaflux_rebalance_cancel_rollback", "kaflux_rebalance_throttle",
	"kaflux_rebalance_execute", "kaflux_rebalance_rollback",
	"kaflux_topic_truncate", "kaflux_topic_recreate", "kaflux_topic_bulk_delete",
	"kaflux_group_reset_offsets",
}

func TestScopesMatchOperations(t *testing.T) {
	for _, tool := range Catalog() {
		key := tool.Method + " " + tool.Path
		switch tool.Method {
		case "DELETE":
			if tool.Scope != "destructive" {
				t.Errorf("%s: DELETE operation has scope %q", tool.Name, tool.Scope)
			}
		case "POST", "PUT", "PATCH":
			if want, ok := readOnlyPosts[key]; ok {
				if tool.Scope != want {
					t.Errorf("%s: pinned read-only %s has scope %q, want %q", tool.Name, key, tool.Scope, want)
				}
			} else if !isWrite(tool) {
				t.Errorf("%s: %s has non-write scope %q", tool.Name, key, tool.Scope)
			}
		}
	}
	for key := range readOnlyPosts {
		found := false
		for _, tool := range Catalog() {
			found = found || tool.Method+" "+tool.Path == key
		}
		if !found {
			t.Errorf("pinned read-only operation %s is not in the catalog", key)
		}
	}
	for _, name := range mustBeDestructive {
		tool, ok := Lookup(name)
		if !ok || tool.Scope != "destructive" || !tool.Destructive {
			t.Errorf("%s must exist with scope destructive (got %q)", name, tool.Scope)
		}
	}
}

func TestSearchIsOptIn(t *testing.T) {
	for _, tool := range Catalog() {
		props, _ := tool.InputSchema()["properties"].(map[string]any)
		if _, has := props["search"]; has != tool.Search {
			t.Errorf("%s: search property %v, Search %v", tool.Name, has, tool.Search)
		}
	}
	for _, name := range []string{"kaflux_topic_list", "kaflux_group_list"} {
		if tool, _ := Lookup(name); !tool.Search {
			t.Errorf("%s: search expected", name)
		}
	}
}

func TestLookup(t *testing.T) {
	for _, tool := range Catalog() {
		got, ok := Lookup(tool.Name)
		if !ok || got.Method != tool.Method || got.Path != tool.Path || got.Scope != tool.Scope {
			t.Errorf("Lookup(%s) = %+v, %v", tool.Name, got, ok)
		}
	}
	if _, ok := Lookup("kaflux_auth_login"); ok {
		t.Error("Lookup found an unknown tool")
	}
}

// strictSchema decodes s with the SDK's JSON Schema implementation, resolves
// it, and reports any keyword the 2020-12 draft does not define.
func strictSchema(t *testing.T, name string, s map[string]any) {
	t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("%s: marshal: %v", name, err)
	}
	var js jsonschema.Schema
	if err := json.Unmarshal(raw, &js); err != nil {
		t.Fatalf("%s: decode: %v", name, err)
	}
	if _, err := js.Resolve(nil); err != nil {
		t.Errorf("%s: resolve: %v", name, err)
	}
	var unknown func(*jsonschema.Schema, string)
	unknown = func(s *jsonschema.Schema, at string) {
		if s == nil {
			return
		}
		for k := range s.Extra {
			t.Errorf("%s: unknown keyword %q at %s", name, k, at)
		}
		for k, p := range s.Properties {
			unknown(p, at+"/properties/"+k)
		}
		unknown(s.Items, at+"/items")
		unknown(s.AdditionalProperties, at+"/additionalProperties")
		for i, x := range s.AnyOf {
			unknown(x, at+"/anyOf/"+string(rune('0'+i)))
		}
		for i, x := range s.OneOf {
			unknown(x, at+"/oneOf/"+string(rune('0'+i)))
		}
		for i, x := range s.AllOf {
			unknown(x, at+"/allOf/"+string(rune('0'+i)))
		}
		for i, x := range s.PrefixItems {
			unknown(x, at+"/prefixItems/"+string(rune('0'+i)))
		}
	}
	unknown(&js, "#")
}

func TestSchemasValid(t *testing.T) {
	server := sdk.NewServer(&sdk.Implementation{Name: "kaflux-test", Version: "test"}, nil)
	for _, tool := range Catalog() {
		in := tool.InputSchema()
		if in["type"] != "object" {
			t.Errorf("%s: input schema type %v", tool.Name, in["type"])
		}
		strictSchema(t, tool.Name+" input", in)
		props, _ := in["properties"].(map[string]any)
		for _, req := range requiredOf(in) {
			if _, ok := props[req]; !ok {
				t.Errorf("%s: required %q is not a property", tool.Name, req)
			}
		}
		if strings.Contains(tool.Path, "{cluster}") && !slices.Contains(requiredOf(in), "cluster") {
			t.Errorf("%s: cluster argument not required", tool.Name)
		}
		if _, ok := props["X-CSRF-Token"]; ok {
			t.Errorf("%s: CSRF header leaked into input schema", tool.Name)
		}
		if tool.Paginated {
			for _, p := range []string{"limit", "cursor"} {
				if _, ok := props[p]; !ok {
					t.Errorf("%s: paginated tool lacks %q", tool.Name, p)
				}
			}
		}
		out := tool.OutputSchema()
		if out == nil {
			t.Errorf("%s: no output schema", tool.Name)
		} else {
			if out["type"] != "object" {
				t.Errorf("%s: output schema type %v", tool.Name, out["type"])
			}
			strictSchema(t, tool.Name+" output", out)
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: SDK rejected tool: %v", tool.Name, r)
				}
			}()
			server.AddTool(tool.SDKTool(), func(_ context.Context, _ *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
				return nil, nil
			})
		}()
	}
}

func TestToolDescriptionsShort(t *testing.T) {
	for _, tool := range Catalog() {
		d := tool.Description
		if len(d) == 0 || len(d) > 400 {
			t.Errorf("%s: description length %d", tool.Name, len(d))
		}
		if strings.ContainsAny(d, "\n\r\t") || strings.Contains(d, "  ") || strings.TrimSpace(d) != d {
			t.Errorf("%s: description has newlines or whitespace runs", tool.Name)
		}
	}
}

// The embedded copy must match docs/openapi.yaml apart from the one version
// line release-please bumps in the source only (regenerate with go generate).
func TestEmbeddedOpenAPIInSync(t *testing.T) {
	source, err := os.ReadFile("../../../docs/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string][]byte{"docs/openapi.yaml": source, "embedded copy": openapiSpec} {
		if n := len(releaseVersionLine.FindAllIndex(unixLines(b), -1)); n != 1 {
			t.Fatalf("%s: %d release-please version lines, want exactly 1", name, n)
		}
	}
	if normalizeSpec(source) != normalizeSpec(openapiSpec) {
		t.Fatal("backend/internal/mcp/openapi.yaml differs from docs/openapi.yaml; run go generate ./internal/mcp")
	}
}

var releaseVersionLine = regexp.MustCompile(`(?m)^  version: \S+ # x-release-please-version$`)

func unixLines(b []byte) []byte { return []byte(strings.ReplaceAll(string(b), "\r\n", "\n")) }

func normalizeSpec(b []byte) string {
	return string(releaseVersionLine.ReplaceAll(unixLines(b), nil))
}
