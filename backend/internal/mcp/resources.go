package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const resourceScheme = "kaflux://clusters"

// resourceDef maps a resource URI shape onto the read tool that backs it.
type resourceDef struct {
	tool string
	// segments after kaflux://clusters: each is a literal or a {variable}
	// that becomes the tool argument of the same name (id becomes cluster).
	shape []string
}

var resourceDefs = []resourceDef{
	{tool: "kaflux_cluster_list", shape: nil},
	{tool: "kaflux_cluster_overview", shape: []string{"{cluster}", "overview"}},
	{tool: "kaflux_topic_get", shape: []string{"{cluster}", "topics", "{topic}"}},
}

// addResources registers the read-only resources whose backing tool the
// caller may see. Reads take the same dispatcher path as the tool.
func (h *Handler) addResources(s *sdk.Server, c *caller) {
	for _, def := range resourceDefs {
		t, ok := Lookup(def.tool)
		if !ok || h.visibility(t, c) != "" {
			continue
		}
		handler := h.resourceHandler(def, t, c)
		switch def.tool {
		case "kaflux_cluster_list":
			s.AddResource(&sdk.Resource{Name: "clusters", Title: "Kaflux clusters", URI: resourceScheme, MIMEType: "application/json",
				Description: "The Kafka clusters the token may see (same data as kaflux_cluster_list)."}, handler)
		case "kaflux_cluster_overview":
			s.AddResourceTemplate(&sdk.ResourceTemplate{Name: "cluster-overview", Title: "Cluster overview", URITemplate: resourceScheme + "/{id}/overview", MIMEType: "application/json",
				Description: "Overview of one cluster by id (same data as kaflux_cluster_overview)."}, handler)
		case "kaflux_topic_get":
			s.AddResourceTemplate(&sdk.ResourceTemplate{Name: "topic", Title: "Topic details", URITemplate: resourceScheme + "/{id}/topics/{topic}", MIMEType: "application/json",
				Description: "Details of one topic (same data as kaflux_topic_get)."}, handler)
		}
	}
}

// parseResourceURI returns the decoded template variables for uri, or false
// when uri does not match def. Variables are rejected exactly like the
// dispatcher rejects path arguments.
func parseResourceURI(def resourceDef, uri string) (map[string]any, *ToolError) {
	rest, ok := strings.CutPrefix(uri, resourceScheme)
	if !ok || strings.ContainsAny(rest, "?#") {
		return nil, invalid("unsupported resource URI")
	}
	var parts []string
	if rest != "" {
		r, ok := strings.CutPrefix(rest, "/")
		if !ok {
			return nil, invalid("unsupported resource URI")
		}
		parts = strings.Split(r, "/")
	}
	if len(parts) != len(def.shape) {
		return nil, invalid("unsupported resource URI")
	}
	args := map[string]any{}
	for i, seg := range def.shape {
		if name, isVar := strings.CutSuffix(strings.TrimPrefix(seg, "{"), "}"); isVar && strings.HasPrefix(seg, "{") {
			v, err := url.PathUnescape(parts[i])
			if err != nil || v == "" || v == "." || v == ".." || strings.ContainsAny(v, "/{}\\?#") || strings.ContainsFunc(v, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
				return nil, invalid("resource variable %q is not a valid name", name)
			}
			args[name] = v
		} else if parts[i] != seg {
			return nil, invalid("unsupported resource URI")
		}
	}
	return args, nil
}

func (h *Handler) resourceHandler(def resourceDef, t Tool, c *caller) sdk.ResourceHandler {
	return func(ctx context.Context, req *sdk.ReadResourceRequest) (*sdk.ReadResourceResult, error) {
		uri := req.Params.URI
		if why := h.visibility(t, c); why != "" {
			h.metrics.deny(why)
			return nil, resourceError(hiddenError(t, why))
		}
		args, terr := parseResourceURI(def, uri)
		if terr != nil {
			return nil, resourceError(terr)
		}
		call := Call{Args: args}
		call.Cluster, _ = args["cluster"].(string)
		res, terr := h.d.Do(ctx, h.principal(c), t, call)
		if terr != nil {
			h.errorLabel(terr)
			return nil, resourceError(terr)
		}
		return &sdk.ReadResourceResult{Contents: []*sdk.ResourceContents{{URI: uri, MIMEType: "application/json", Text: string(res.Body)}}}, nil
	}
}

func resourceError(e *ToolError) error {
	b, _ := json.Marshal(map[string]any{"code": e.Code, "message": e.Message, "hint": e.Hint, "status": e.Status})
	return fmt.Errorf("%s", b)
}
