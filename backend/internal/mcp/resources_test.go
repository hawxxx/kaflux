package mcp

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func readResource(cs *sdk.ClientSession, uri string) (string, error) {
	res, err := cs.ReadResource(context.Background(), &sdk.ReadResourceParams{URI: uri})
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, c := range res.Contents {
		b.WriteString(c.Text)
	}
	return b.String(), nil
}

func listedResources(t *testing.T, cs *sdk.ClientSession) (uris, templates []string) {
	t.Helper()
	for r, err := range cs.Resources(context.Background(), nil) {
		if err != nil {
			t.Fatal(err)
		}
		uris = append(uris, r.URI)
	}
	for r, err := range cs.ResourceTemplates(context.Background(), nil) {
		if err != nil {
			t.Fatal(err)
		}
		templates = append(templates, r.URITemplate)
	}
	return
}

func TestResourcesListed(t *testing.T) {
	e := newEnv(t, envConfig{demo: true})
	cs := e.mustConnect(t, e.token(t, tokenSpec{scopes: []string{"read"}}), "")
	uris, templates := listedResources(t, cs)
	if !slices.Equal(uris, []string{"kaflux://clusters"}) {
		t.Errorf("resources = %v", uris)
	}
	for _, want := range []string{"kaflux://clusters/{id}/overview", "kaflux://clusters/{id}/topics/{topic}"} {
		if !slices.Contains(templates, want) {
			t.Errorf("template %s missing from %v", want, templates)
		}
	}
	body, err := readResource(cs, "kaflux://clusters")
	if err != nil || !strings.Contains(body, "demo") {
		t.Fatalf("clusters resource = %q, %v", body, err)
	}
	if body, err = readResource(cs, "kaflux://clusters/demo/overview"); err != nil || body == "" {
		t.Fatalf("overview = %q, %v", body, err)
	}
	topics := call(t, cs, "kaflux_topic_list", map[string]any{"cluster": "demo"})
	m := regexp.MustCompile(`"name":"([^"]+)"`).FindStringSubmatch(resultText(topics))
	if m == nil {
		t.Fatalf("no topic in %s", resultText(topics))
	}
	if body, err = readResource(cs, "kaflux://clusters/demo/topics/"+m[1]); err != nil || !strings.Contains(body, m[1]) {
		t.Fatalf("topic = %q, %v", body, err)
	}
}

func TestResourceAuthorized(t *testing.T) {
	e := newEnv(t, envConfig{demo: true})
	cs := e.mustConnect(t, e.token(t, tokenSpec{scopes: []string{"read"}, clusters: []string{"other"}}), "")
	_, errRes := readResource(cs, "kaflux://clusters/demo/overview")
	if errRes == nil {
		t.Fatal("denied cluster resource was readable")
	}
	tool := errorOf(t, call(t, cs, "kaflux_cluster_overview", map[string]any{"cluster": "demo"}))
	for _, part := range []string{tool.Code, tool.Message} {
		if !strings.Contains(errRes.Error(), part) {
			t.Errorf("resource error %q does not match tool error part %q", errRes, part)
		}
	}
	if _, err := readResource(cs, "kaflux://clusters/demo/topics/orders"); err == nil {
		t.Error("denied topic resource was readable")
	}
}

func TestResourceHiddenToolNotListedOrReadable(t *testing.T) {
	e := newEnv(t, envConfig{demo: true})
	cs := e.mustConnect(t, e.token(t, tokenSpec{scopes: []string{"messages"}}), "")
	uris, templates := listedResources(t, cs)
	if len(uris)+len(templates) != 0 {
		t.Errorf("token without read scope sees %v %v", uris, templates)
	}
	if _, err := readResource(cs, "kaflux://clusters"); err == nil {
		t.Error("resource readable without read scope")
	}
}

func TestResourceRejectsBadVariables(t *testing.T) {
	e := newEnv(t, envConfig{demo: true})
	cs := e.mustConnect(t, e.token(t, tokenSpec{scopes: []string{"read"}}), "")
	for _, uri := range []string{
		"kaflux://clusters/../overview",
		"kaflux://clusters/%2e%2e/overview",
		"kaflux://clusters/de%2Fmo/overview",
		"kaflux://clusters/%7Bcluster%7D/overview",
		"kaflux://clusters/demo/topics/a%2Fb",
		"kaflux://clusters/demo/topics/..",
		"kaflux://clusters/demo/topics/x?y=1",
	} {
		if body, err := readResource(cs, uri); err == nil {
			t.Errorf("%s was readable: %q", uri, body)
		}
	}
}

func TestParseResourceURI(t *testing.T) {
	def := resourceDefs[2]
	args, err := parseResourceURI(def, "kaflux://clusters/demo/topics/a%20b")
	if err != nil || args["cluster"] != "demo" || args["topic"] != "a b" {
		t.Fatalf("args=%v err=%v", args, err)
	}
	if _, err := parseResourceURI(def, "kaflux://clusters/demo/topics"); err == nil {
		t.Error("short URI accepted")
	}
}

func TestPromptsListed(t *testing.T) {
	e := newEnv(t, envConfig{demo: true})
	cs := e.mustConnect(t, e.token(t, tokenSpec{scopes: []string{"read"}}), "")
	var names []string
	for p, err := range cs.Prompts(context.Background(), nil) {
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, p.Name)
	}
	slices.Sort(names)
	want := []string{"audit-acl-changes", "diagnose-consumer-lag", "plan-safe-rebalance", "topic-health-review"}
	if !slices.Equal(names, want) {
		t.Fatalf("prompts = %v, want %v", names, want)
	}
}

func promptText(t *testing.T, cs *sdk.ClientSession, name string, args map[string]string) string {
	t.Helper()
	res, err := cs.GetPrompt(context.Background(), &sdk.GetPromptParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, m := range res.Messages {
		if tc, ok := m.Content.(*sdk.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

func TestPromptMentionsConfirmationProtocol(t *testing.T) {
	e := newEnv(t, envConfig{demo: true})
	cs := e.mustConnect(t, e.token(t, tokenSpec{scopes: []string{"read"}}), "")
	toolRef := regexp.MustCompile(`kaflux_[a-z_]+`)
	for _, name := range []string{"diagnose-consumer-lag", "topic-health-review", "plan-safe-rebalance", "audit-acl-changes"} {
		text := promptText(t, cs, name, map[string]string{"cluster": "demo", "group": "g1", "topic": "orders"})
		refs := toolRef.FindAllString(text, -1)
		if len(refs) == 0 {
			t.Errorf("%s names no tools", name)
		}
		for _, r := range refs {
			if _, ok := Lookup(r); !ok {
				t.Errorf("%s mentions unknown tool %s", name, r)
			}
		}
		if !strings.Contains(text, "demo") {
			t.Errorf("%s ignores its arguments", name)
		}
	}
	text := promptText(t, cs, "plan-safe-rebalance", map[string]string{"cluster": "demo"})
	for _, want := range []string{"WITHOUT \"confirmation\"", "repeat the identical call", "restConfirmation", "kaflux_rebalance_plan", "kaflux_rebalance_execute", "STOP", "destructive"} {
		if !strings.Contains(text, want) {
			t.Errorf("plan-safe-rebalance lacks %q", want)
		}
	}
	for _, step := range [][2]string{{"kaflux_balance_get", "kaflux_rebalance_plan"}, {"kaflux_rebalance_plan", "kaflux_rebalance_dry_run"}, {"kaflux_rebalance_dry_run", "kaflux_rebalance_execute"}} {
		if strings.Index(text, step[0]) > strings.Index(text, step[1]) {
			t.Errorf("%s should come before %s", step[0], step[1])
		}
	}
	if acl := promptText(t, cs, "audit-acl-changes", map[string]string{"cluster": "demo"}); !strings.Contains(acl, "confirmation") {
		t.Error("audit-acl-changes omits the protocol for its write follow-ups")
	}
}
