// Package mcp exposes Kaflux to Model Context Protocol clients.
package mcp

import (
	"slices"

	"github.com/hawxxx/kaflux/backend/internal/auth"
)

// ScopeActions maps each token scope to the API actions it permits. It is
// auth.MCPScopeActions, which lives in auth so the REST token endpoints can
// check scopes without importing this package.
var ScopeActions = auth.MCPScopeActions

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
