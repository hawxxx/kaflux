// Package mcp exposes Kaflux to Model Context Protocol clients.
package mcp

import (
	"slices"

	"github.com/hawxxx/kaflux/backend/internal/auth"
)

// ScopeActions maps each token scope to the API actions it permits. Session
// administration and any future administrative action belong to no scope, so
// a token can never exercise them.
var ScopeActions = map[string][]string{
	"read":        {"read", "schema-read", "connector-read", "audit"},
	"messages":    {"consume"},
	"write":       {"produce", "create", "alter-config", "rename", "plan", "manage-acls", "manage-quotas", "manage-users", "manage-tokens", "manage-cluster", "schema-update", "connector-update"},
	"destructive": {"delete", "reset-offsets", "execute", "rollback"},
}

// Narrow restricts owner's grants to the actions of scopes and to the cluster
// allowlist (empty means every cluster the owner holds). A "*" owner action or
// cluster is expanded to the concrete allowed values rather than kept, so the
// result is always a subset of what owner already permits.
func Narrow(owner []auth.Grant, scopes, clusters []string) []auth.Grant {
	var allowed []string
	for _, s := range scopes {
		for _, a := range ScopeActions[s] {
			if a != "*" && !slices.Contains(allowed, a) {
				allowed = append(allowed, a)
			}
		}
	}
	out := []auth.Grant{}
	for _, g := range owner {
		var actions []string
		switch {
		case g.Action == "*":
			actions = allowed
		case slices.Contains(allowed, g.Action):
			actions = []string{g.Action}
		}
		for _, c := range narrowClusters(g.Cluster, clusters) {
			for _, a := range actions {
				n := g
				n.Cluster, n.Action = c, a
				if !slices.Contains(out, n) {
					out = append(out, n)
				}
			}
		}
	}
	return out
}

// narrowClusters intersects one grant's cluster with the allowlist.
func narrowClusters(owner string, allow []string) []string {
	if len(allow) == 0 {
		return []string{owner}
	}
	var out []string
	for _, c := range allow {
		var keep string
		switch {
		case owner == "*":
			keep = c
		case c == "*" || c == owner:
			keep = owner
		default:
			continue
		}
		if !slices.Contains(out, keep) {
			out = append(out, keep)
		}
	}
	return out
}
