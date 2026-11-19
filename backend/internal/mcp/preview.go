package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type principalKey struct{}

// withPrincipal carries the caller into a Preview hook, so its reads run
// under the same identity and grants as the write being prepared.
func withPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

func principalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// PreviewRead runs the read-only GET tool name as the principal of a
// Preview hook's ctx, with args (cluster comes from c), and returns the
// decoded response. It refuses anything but a GET read tool.
func (d *Dispatcher) PreviewRead(ctx context.Context, name string, c Call, args map[string]any) (any, error) {
	p, ok := principalFrom(ctx)
	if !ok {
		return nil, &ToolError{Code: "internal", Message: "Preview has no caller", Hint: "This is a Kaflux defect; report it.", Status: 500}
	}
	t, ok := Lookup(name)
	if !ok || t.Method != "GET" || !t.ReadOnly {
		return nil, errNotDispatchable
	}
	cluster := c.Cluster
	if cluster == "" {
		cluster, _ = c.Args["cluster"].(string)
	}
	res, terr := d.Do(ctx, p, t, Call{Cluster: cluster, Args: args})
	if terr != nil {
		return nil, terr
	}
	var v any
	if err := json.Unmarshal(res.Body, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// readPreview previews a write by reading its target with readTool,
// passing the path arguments both tools share.
func readPreview(readTool string) func(context.Context, *Dispatcher, Call) (any, error) {
	return func(ctx context.Context, d *Dispatcher, c Call) (any, error) {
		rt, ok := Lookup(readTool)
		if !ok {
			return nil, errNotDispatchable
		}
		_, params := operation(rt.Method, rt.Path)
		args := map[string]any{}
		for _, prm := range params {
			name, _ := prm["name"].(string)
			if v, ok := c.Args[name]; ok && prm["in"] == "path" && name != "cluster" {
				args[name] = v
			}
		}
		v, err := d.PreviewRead(ctx, readTool, c, args)
		if err != nil {
			return nil, err
		}
		return map[string]any{"current": v, "readWith": readTool}, nil
	}
}

func distinctStrings(v any) []string {
	var in []string
	switch l := v.(type) {
	case []any:
		for _, x := range l {
			if s, ok := x.(string); ok {
				in = append(in, s)
			}
		}
	case []string:
		in = l
	}
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// bulkDeletePreview resolves the distinct target list; reading every
// target would spend up to 50 calls of the token's rate budget.
func bulkDeletePreview(_ context.Context, _ *Dispatcher, c Call) (any, error) {
	targets := distinctStrings(obj(c.Args["body"])["topics"])
	return map[string]any{"targets": targets, "count": len(targets)}, nil
}

// aclDeletePreview lists the ACLs the exact binding in body.acl matches.
func aclDeletePreview(ctx context.Context, d *Dispatcher, c Call) (any, error) {
	acl := obj(obj(c.Args["body"])["acl"])
	args := map[string]any{}
	for _, k := range []string{"resourceType", "resourceName", "principal"} {
		if s, ok := acl[k].(string); ok && s != "" {
			args[k] = s
		}
	}
	v, err := d.PreviewRead(ctx, "kaflux_acl_list", c, args)
	if err != nil {
		return nil, err
	}
	var rows []any
	if m := obj(v); m != nil {
		rows, _ = m["data"].([]any)
	}
	matching := []any{}
	for _, row := range rows {
		if matchesACL(obj(row), acl) {
			matching = append(matching, row)
		}
	}
	return map[string]any{"matching": matching, "count": len(matching)}, nil
}

func matchesACL(row, want map[string]any) bool {
	if row == nil {
		return false
	}
	src := row
	if inner := obj(row["acl"]); inner != nil {
		src = inner
	}
	for _, k := range []string{"resourceType", "resourceName", "patternType", "principal", "host", "operation", "permission"} {
		if w, ok := want[k]; ok && fmt.Sprint(src[k]) != fmt.Sprint(w) {
			return false
		}
	}
	return true
}

// previewHooks maps write tools to their read-only previews. Tools without
// one echo their arguments in the plan.
func previewHooks() map[string]func(context.Context, *Dispatcher, Call) (any, error) {
	rebalance := readPreview("kaflux_rebalance_get")
	return map[string]func(context.Context, *Dispatcher, Call) (any, error){
		"kaflux_topic_delete":              readPreview("kaflux_topic_get"),
		"kaflux_topic_truncate":            readPreview("kaflux_topic_get"),
		"kaflux_topic_recreate":            readPreview("kaflux_topic_get"),
		"kaflux_topic_bulk_delete":         bulkDeletePreview,
		"kaflux_group_reset_offsets":       readPreview("kaflux_group_get"),
		"kaflux_acl_delete":                aclDeletePreview,
		"kaflux_schema_subject_delete":     readPreview("kaflux_schema_versions"),
		"kaflux_schema_version_delete":     readPreview("kaflux_schema_version_get"),
		"kaflux_connector_delete":          readPreview("kaflux_connector_get"),
		"kaflux_rebalance_execute":         rebalance,
		"kaflux_rebalance_rollback":        rebalance,
		"kaflux_rebalance_pause":           rebalance,
		"kaflux_rebalance_resume":          rebalance,
		"kaflux_rebalance_cancel":          rebalance,
		"kaflux_rebalance_cancel_rollback": rebalance,
		"kaflux_rebalance_throttle":        rebalance,
	}
}

// Hooks are attached in init: referencing them from the writeTools literal
// would make an initialization cycle through Lookup.
func init() {
	hooks := previewHooks()
	for i := range writeTools {
		if h, ok := hooks[writeTools[i].Name]; ok {
			writeTools[i].Preview = h
		}
	}
}

// restConfirmation tells the agent which confirmation values the REST body
// itself must carry, derived from the request body schema. Dispatch never
// fills them in.
func restConfirmation(t Tool, c Call) string {
	body := obj(c.Args["body"])
	switch t.Name {
	case "kaflux_topic_bulk_delete":
		n := len(distinctStrings(body["topics"]))
		return fmt.Sprintf("body.confirmation must be exactly %q (the number of distinct topics in body.topics)", fmt.Sprintf("delete %d topics", n))
	case "kaflux_group_reset_offsets":
		group, _ := c.Args["group"].(string)
		if p, _ := body["preview"].(bool); p {
			return fmt.Sprintf("none while body.preview is true; to apply, set body.preview false, body.confirmation to the group name %q and body.previewHash from the preview response", group)
		}
		return fmt.Sprintf("body.confirmation must be the group name %q and body.previewHash must come from a fresh preview:true call", group)
	}
	op, _ := operation(t.Method, t.Path)
	schema := deref(jsonContentSchema(deref(obj(op["requestBody"]))))
	props := obj(schema["properties"])
	conf := obj(props["confirmation"])
	if conf == nil {
		return "none: the REST body has no confirmation field"
	}
	var parts []string
	desc, _ := conf["description"].(string)
	enum, _ := conf["enum"].([]any)
	switch {
	case len(enum) == 1 && enum[0] == true:
		parts = append(parts, "body.confirmation must be true")
	case strings.Contains(strings.ToLower(desc), "topic name"):
		topic, _ := c.Args["topic"].(string)
		parts = append(parts, fmt.Sprintf("body.confirmation must be the exact topic name %q", topic))
	default:
		parts = append(parts, "body.confirmation must be set as the tool description says")
	}
	if props["planHash"] != nil {
		parts = append(parts, "body.planHash must be the current planHash from kaflux_rebalance_get")
	}
	return strings.Join(parts, "; ")
}
