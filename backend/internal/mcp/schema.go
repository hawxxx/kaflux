package mcp

import (
	_ "embed"
	"maps"
	"strconv"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// openapiSpec is a generated copy of docs/openapi.yaml, the source of truth;
// a test fails when the two differ.
//
//go:generate cp ../../../docs/openapi.yaml openapi.yaml
//go:embed openapi.yaml
var openapiSpec []byte

var spec = sync.OnceValue(func() map[string]any {
	var doc map[string]any
	if err := yaml.Unmarshal(openapiSpec, &doc); err != nil {
		panic("mcp: embedded OpenAPI is invalid: " + err.Error())
	}
	return doc
})

// Pagination defaults shared with dispatch.
const (
	DefaultPageSize = 50
	MaxPageSize     = 200
)

func obj(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// resolveRef follows a local JSON pointer such as #/components/schemas/Plan.
func resolveRef(ref string) map[string]any {
	node := any(spec())
	for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		node = obj(node)[part]
	}
	return obj(node)
}

// deref resolves a non-schema object (parameter, request body, response).
func deref(m map[string]any) map[string]any {
	for i := 0; i < 8 && m != nil; i++ {
		ref, ok := m["$ref"].(string)
		if !ok {
			return m
		}
		m = resolveRef(ref)
	}
	return m
}

// operation returns the OpenAPI operation and its merged parameters
// (path-level first, operation-level overriding by name and location).
func operation(method, path string) (map[string]any, []map[string]any) {
	item := obj(obj(spec()["paths"])[path])
	op := obj(item[strings.ToLower(method)])
	if op == nil {
		return nil, nil
	}
	var params []map[string]any
	add := func(list any) {
		raw, _ := list.([]any)
		for _, p := range raw {
			pm := deref(obj(p))
			if pm == nil {
				continue
			}
			replaced := false
			for i, q := range params {
				if q["name"] == pm["name"] && q["in"] == pm["in"] {
					params[i], replaced = pm, true
				}
			}
			if !replaced {
				params = append(params, pm)
			}
		}
	}
	add(item["parameters"])
	add(op["parameters"])
	return op, params
}

func jsonContentSchema(m map[string]any) map[string]any {
	return obj(obj(obj(m["content"])["application/json"])["schema"])
}

// InputSchema derives the tool's JSON Schema (2020-12) arguments: path and
// query parameters at the top level, the JSON request body under "body",
// limit/cursor/search for paginated tools and an optional "confirmation"
// token for writes. Header parameters (CSRF) are supplied by dispatch.
func (t Tool) InputSchema() map[string]any {
	props := map[string]any{}
	required := []string{}
	op, params := operation(t.Method, t.Path)
	for _, p := range params {
		if p["in"] != "path" && p["in"] != "query" {
			continue
		}
		name, _ := p["name"].(string)
		s := convertSchema(p["schema"], nil)
		if d, ok := p["description"].(string); ok && s["description"] == nil {
			s["description"] = d
		}
		if name == "cluster" && s["description"] == nil {
			s["description"] = "Cluster id from kaflux_cluster_list, e.g. \"local\""
		}
		props[name] = s
		if req, _ := p["required"].(bool); req || p["in"] == "path" {
			required = append(required, name)
		}
	}
	if rb := deref(obj(op["requestBody"])); rb != nil {
		if s := jsonContentSchema(rb); s != nil {
			body := convertSchema(s, nil)
			if body["description"] == nil {
				body["description"] = "JSON request body, exactly as the REST API accepts it"
			}
			props["body"] = body
			if req, _ := rb["required"].(bool); req {
				required = append(required, "body")
			}
		}
	}
	if t.Paginated {
		if props["limit"] == nil {
			props["limit"] = map[string]any{
				"type": "integer", "minimum": 1, "maximum": MaxPageSize, "default": DefaultPageSize,
				"description": "Maximum items to return",
			}
		}
		if props["cursor"] == nil {
			props["cursor"] = map[string]any{"type": "string", "description": "nextCursor from the previous page"}
		}
		if props["search"] == nil {
			props["search"] = map[string]any{"type": "string", "description": "Case-insensitive substring filter on item names"}
		}
	}
	if t.IsWrite() {
		props["confirmation"] = map[string]any{
			"type":        "string",
			"description": "Omit to prepare: the result previews the change and returns a confirmation token. Repeat the identical call with that token to commit.",
		}
	}
	out := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	if len(required) > 0 {
		out["required"] = required
	}
	return out
}

// OutputSchema derives the JSON Schema of the operation's 200 JSON response;
// paginated tools also report truncation and the next cursor. Nil when the
// operation has no JSON success response.
func (t Tool) OutputSchema() map[string]any {
	op, _ := operation(t.Method, t.Path)
	responses := obj(op["responses"])
	var s map[string]any
	for _, code := range []string{"200", "201", "202"} {
		if r := deref(obj(responses[code])); r != nil {
			if s = jsonContentSchema(r); s != nil {
				break
			}
		}
	}
	if s == nil {
		return nil
	}
	out := convertSchema(s, nil)
	if t.Paginated && out["type"] == "object" {
		props := maps.Clone(obj(out["properties"]))
		if props == nil {
			props = map[string]any{}
		}
		props["truncated"] = map[string]any{"type": "boolean", "description": "True when more items exist than were returned"}
		props["nextCursor"] = map[string]any{"type": "string", "description": "Pass as cursor to fetch the next page"}
		out["properties"] = props
	}
	return out
}

// convertSchema turns an OpenAPI 3.0 schema object into a self-contained
// JSON Schema 2020-12 object: component $refs are inlined, nullable becomes a
// null type, and OpenAPI-only keywords are dropped.
func convertSchema(node any, seen map[string]bool) map[string]any {
	in := obj(node)
	if in == nil {
		return map[string]any{}
	}
	if ref, ok := in["$ref"].(string); ok {
		if seen[ref] {
			return map[string]any{} // recursive type: accept anything below
		}
		next := maps.Clone(seen)
		if next == nil {
			next = map[string]bool{}
		}
		next[ref] = true
		out := convertSchema(resolveRef(ref), next)
		if d, ok := in["description"].(string); ok {
			out["description"] = d
		}
		return out
	}
	out := map[string]any{}
	for k, v := range in {
		switch {
		case k == "nullable", k == "example", k == "discriminator", k == "xml", k == "externalDocs", strings.HasPrefix(k, "x-"):
		case k == "properties":
			props := map[string]any{}
			for name, ps := range obj(v) {
				props[name] = convertSchema(ps, seen)
			}
			out[k] = props
		case k == "items", k == "not":
			out[k] = convertSchema(v, seen)
		case k == "additionalProperties":
			if b, ok := v.(bool); ok {
				out[k] = b
			} else {
				out[k] = convertSchema(v, seen)
			}
		case k == "pattern":
			p, _ := v.(string)
			out[k] = portablePattern(p)
		case k == "allOf", k == "anyOf", k == "oneOf":
			list, _ := v.([]any)
			conv := make([]any, 0, len(list))
			for _, x := range list {
				conv = append(conv, convertSchema(x, seen))
			}
			out[k] = conv
		default:
			out[k] = v
		}
	}
	for _, pair := range [][2]string{{"exclusiveMinimum", "minimum"}, {"exclusiveMaximum", "maximum"}} {
		if b, ok := out[pair[0]].(bool); ok {
			delete(out, pair[0])
			if b && out[pair[1]] != nil {
				out[pair[0]] = out[pair[1]]
				delete(out, pair[1])
			}
		}
	}
	if nullable, _ := in["nullable"].(bool); nullable {
		switch typ := out["type"].(type) {
		case string:
			out["type"] = []any{typ, "null"}
			if enum, ok := out["enum"].([]any); ok {
				out["enum"] = append(append([]any{}, enum...), nil)
			}
		case nil:
			return map[string]any{"anyOf": []any{out, map[string]any{"type": "null"}}}
		}
	}
	return out
}

// portablePattern replaces ECMA-262 \uXXXX escapes, which Go's RE2 (used by
// the SDK validator) rejects, with the literal character both engines accept.
func portablePattern(p string) string {
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		if p[i] == '\\' && i+1 < len(p) {
			if p[i+1] == 'u' && i+6 <= len(p) {
				if r, err := strconv.ParseUint(p[i+2:i+6], 16, 32); err == nil {
					b.WriteRune(rune(r))
					i += 5
					continue
				}
			}
			b.WriteByte(p[i])
			b.WriteByte(p[i+1])
			i++
			continue
		}
		b.WriteByte(p[i])
	}
	return b.String()
}
