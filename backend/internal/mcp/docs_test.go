package mcp

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
)

// updateDocs rewrites the generated tool catalog in docs/mcp.md:
//
//	go test ./internal/mcp -run TestDocsCatalogTable -update-docs
var updateDocs = flag.Bool("update-docs", false, "rewrite the tool catalog table in docs/mcp.md")

const (
	docsPath     = "../../../docs/mcp.md"
	catalogBegin = "<!-- BEGIN GENERATED TOOL CATALOG (go test ./internal/mcp -run TestDocsCatalogTable -update-docs) -->"
	catalogEnd   = "<!-- END GENERATED TOOL CATALOG -->"
)

// catalogTable renders the registry as a Markdown table.
func catalogTable() string {
	var b strings.Builder
	b.WriteString("| Tool | REST operation | Scope | Access |\n|---|---|---|---|\n")
	for _, t := range Catalog() {
		access := "changes state"
		switch {
		case t.ReadOnly:
			access = "read-only"
		case t.Destructive:
			access = "destructive"
		}
		fmt.Fprintf(&b, "| `%s` | `%s %s` | %s | %s |\n", t.Name, t.Method, t.Path, t.Scope, access)
	}
	return b.String()
}

func readDocs(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(docsPath)
	if err != nil {
		t.Fatalf("docs/mcp.md must exist: %v", err)
	}
	return string(b)
}

func TestDocsListEveryTool(t *testing.T) {
	doc := readDocs(t)
	for _, tool := range Catalog() {
		if !strings.Contains(doc, "`"+tool.Name+"`") {
			t.Errorf("docs/mcp.md does not list tool %s", tool.Name)
		}
	}
}

func TestDocsCatalogTable(t *testing.T) {
	doc := readDocs(t)
	start := strings.Index(doc, catalogBegin)
	end := strings.Index(doc, catalogEnd)
	if start < 0 || end < start {
		t.Fatalf("docs/mcp.md needs the generated catalog markers %q ... %q", catalogBegin, catalogEnd)
	}
	want := catalogBegin + "\n\n" + catalogTable() + "\n"
	got := doc[start:end]
	if *updateDocs {
		if got != want {
			if err := os.WriteFile(docsPath, []byte(doc[:start]+want+doc[end:]), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	if got != want {
		t.Fatal("tool catalog in docs/mcp.md is stale; run: go test ./internal/mcp -run TestDocsCatalogTable -update-docs")
	}
}
