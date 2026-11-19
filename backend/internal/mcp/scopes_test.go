package mcp

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/auth"
)

func TestNarrowDropsOutOfScope(t *testing.T) {
	owner := []auth.Grant{{Role: "ops", Cluster: "a", Action: "*", Pattern: "*"}}
	got := Narrow(owner, []string{"read"}, []string{"a"})
	if len(got) == 0 {
		t.Fatal("wildcard owner grant was dropped instead of expanded")
	}
	for _, g := range got {
		if !slices.Contains(ScopeActions["read"], g.Action) {
			t.Fatalf("out-of-scope action %q kept", g.Action)
		}
		if g.Cluster != "a" || g.Role != "ops" || g.Pattern != "*" {
			t.Fatalf("grant fields altered: %+v", g)
		}
	}
	z := auth.Authorizer{Grants: got}
	u := auth.User{Roles: []string{"ops"}}
	if !z.Allowed(u, "a", "read", "orders") || !z.Allowed(u, "a", "schema-read", "s") {
		t.Fatal("read-class actions not allowed")
	}
	for _, denied := range []string{"consume", "produce", "delete", "manage-sessions", "*"} {
		if z.Allowed(u, "a", denied, "orders") {
			t.Fatalf("%s allowed under read scope", denied)
		}
	}
	explicit := []auth.Grant{{Role: "ops", Cluster: "a", Action: "delete", Pattern: "x"}, {Role: "ops", Cluster: "a", Action: "read", Pattern: "y"}}
	if got := Narrow(explicit, []string{"read"}, nil); len(got) != 1 || got[0].Action != "read" {
		t.Fatalf("explicit narrowing = %+v", got)
	}
}

func TestNarrowClusterAllowlist(t *testing.T) {
	owner := []auth.Grant{
		{Role: "ops", Cluster: "a", Action: "read", Pattern: "*"},
		{Role: "ops", Cluster: "b", Action: "read", Pattern: "*"},
		{Role: "ops", Cluster: "*", Action: "read", Pattern: "*"},
	}
	got := Narrow(owner, []string{"read"}, []string{"a"})
	z := auth.Authorizer{Grants: got}
	u := auth.User{Roles: []string{"ops"}}
	if !z.Allowed(u, "a", "read", "t") {
		t.Fatal("allowlisted cluster denied")
	}
	if z.Allowed(u, "b", "read", "t") || z.Allowed(u, "c", "read", "t") {
		t.Fatalf("non-allowlisted cluster allowed: %+v", got)
	}
	// An empty allowlist means every cluster the owner already holds.
	if all := Narrow(owner, []string{"read"}, nil); len(all) != 3 {
		t.Fatalf("empty allowlist = %+v", all)
	}
}

// subset reports whether every permission implied by got is also implied by owner.
func implied(gs []auth.Grant, g auth.Grant) bool {
	for _, o := range gs {
		if o.Role == g.Role && o.Pattern == g.Pattern && o.Regex == g.Regex &&
			(o.Cluster == "*" || o.Cluster == g.Cluster) && (o.Action == "*" || o.Action == g.Action) {
			return true
		}
	}
	return false
}

func TestNarrowNeverAddsGrant(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	roles := []string{"ops", "dev", "*"}
	clusters := []string{"a", "b", "c", "*"}
	actions := append(slices.Clone(auth.Actions), "*", "manage-mcp", "unknown")
	scopes := []string{"read", "messages", "write", "destructive", "bogus"}
	pick := func(xs []string) string { return xs[rng.Intn(len(xs))] }
	for i := 0; i < 2000; i++ {
		var owner []auth.Grant
		for n := rng.Intn(6); n > 0; n-- {
			owner = append(owner, auth.Grant{Role: pick(roles), Cluster: pick(clusters), Action: pick(actions), Pattern: pick([]string{"*", "orders.*", "x"}), Regex: rng.Intn(4) == 0})
		}
		var sc, cl []string
		for n := rng.Intn(4); n > 0; n-- {
			sc = append(sc, pick(scopes))
		}
		for n := rng.Intn(3); n > 0; n-- {
			cl = append(cl, pick(clusters))
		}
		got := Narrow(owner, sc, cl)
		for _, g := range got {
			if g.Action == "*" {
				t.Fatalf("wildcard action leaked: owner=%+v got=%+v", owner, g)
			}
			if !implied(owner, g) {
				t.Fatalf("grant %+v not implied by owner %+v (scopes %v clusters %v)", g, owner, sc, cl)
			}
			if g.Action == "manage-sessions" || g.Action == "manage-mcp" {
				t.Fatalf("admin action granted: %+v", g)
			}
		}
		if len(owner) == 0 && len(got) != 0 {
			t.Fatal("grants created from nothing")
		}
	}
}

func TestAdminActionsInNoScope(t *testing.T) {
	for scope, actions := range ScopeActions {
		for _, a := range actions {
			if a == "manage-sessions" || a == "manage-mcp" || a == "*" {
				t.Fatalf("scope %s contains admin action %s", scope, a)
			}
		}
	}
	owner := []auth.Grant{{Role: "admin", Cluster: "*", Action: "manage-sessions", Pattern: "*"}}
	if got := Narrow(owner, []string{"read", "messages", "write", "destructive"}, nil); len(got) != 0 {
		t.Fatalf("manage-sessions survived narrowing: %+v", got)
	}
}
