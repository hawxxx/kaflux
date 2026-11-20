package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

const (
	mcpTokensBase      = "/api/v1/admin/mcp-tokens"
	mcpMaxActiveTokens = 25
	mcpMaxExpiryDays   = 90
)

var mcpTokenID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// mcpTokenView is the API shape of a token. It never carries the hash; the
// secret is set only in the response that creates the token.
type mcpTokenView struct {
	ID            string     `json:"id"`
	Name          string     `json:"name"`
	OwnerID       string     `json:"ownerId"`
	OwnerProvider string     `json:"ownerProvider"`
	Scopes        []string   `json:"scopes"`
	Clusters      []string   `json:"clusters"`
	Created       time.Time  `json:"created"`
	Expires       time.Time  `json:"expires"`
	LastUsed      *time.Time `json:"lastUsed"`
	Revoked       *time.Time `json:"revoked"`
	Status        string     `json:"status"`
	Secret        string     `json:"secret,omitempty"`
}

func mcpTokenStatus(t store.MCPToken, now time.Time) string {
	switch {
	case t.Revoked != nil:
		return "revoked"
	case !t.Expires.After(now):
		return "expired"
	default:
		return "active"
	}
}

func viewMCPToken(t store.MCPToken, now time.Time) mcpTokenView {
	return mcpTokenView{ID: t.ID, Name: t.Name, OwnerID: t.OwnerID, OwnerProvider: t.OwnerProvider, Scopes: nonNilStrings(t.Scopes), Clusters: nonNilStrings(t.Clusters), Created: t.Created, Expires: t.Expires, LastUsed: t.LastUsed, Revoked: t.Revoked, Status: mcpTokenStatus(t, now)}
}

func nonNilStrings(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}

func ownsMCPToken(u auth.User, t store.MCPToken) bool {
	return t.OwnerID == u.ID && t.OwnerProvider == u.Provider
}

// mcpTokens serves token management. It accepts console (cookie + CSRF)
// sessions only: a token, or anything acting through /mcp, can never list,
// mint or revoke tokens.
func (a *API) mcpTokens(w http.ResponseWriter, r *http.Request, s auth.Session) bool {
	if r.URL.Path != mcpTokensBase && !strings.HasPrefix(r.URL.Path, mcpTokensBase+"/") {
		return false
	}
	if _, viaMCP := mcpFrom(r.Context()); viaMCP {
		fail(w, 403, "forbidden", "MCP tokens cannot manage MCP tokens; use the console")
		return true
	}
	// Token management judges what the console user holds; MCP sessions were refused above.
	az := auth.Authorizer{Grants: a.configuredGrants()}
	manager := az.AnyAllowed(s.User, "*", "manage-mcp")
	if r.URL.Path == mcpTokensBase {
		switch r.Method {
		case "GET":
			a.listMCPTokens(w, r, s, manager)
		case "POST":
			a.createMCPToken(w, r, s, az, manager)
		default:
			fail(w, 405, "method_not_allowed", "GET or POST required")
		}
		return true
	}
	id := strings.TrimPrefix(r.URL.Path, mcpTokensBase+"/")
	if !mcpTokenID.MatchString(id) {
		fail(w, 404, "not_found", "Token not found")
		return true
	}
	if r.Method != "DELETE" {
		fail(w, 405, "method_not_allowed", "DELETE required")
		return true
	}
	a.revokeMCPToken(w, r, s, manager, id)
	return true
}

// visibleMCPTokens lists the caller's tokens, or every token for a manager.
func (a *API) visibleMCPTokens(ctx context.Context, u auth.User, manager bool) ([]store.MCPToken, error) {
	all, e := a.o.Store.ListMCPTokens(ctx, u.ID, manager)
	if e != nil || manager {
		return all, e
	}
	out := []store.MCPToken{}
	for _, t := range all {
		if ownsMCPToken(u, t) {
			out = append(out, t)
		}
	}
	return out, nil
}

func (a *API) listMCPTokens(w http.ResponseWriter, r *http.Request, s auth.Session, manager bool) {
	tokens, e := a.visibleMCPTokens(r.Context(), s.User, manager)
	if e != nil {
		failCause(w, r, 503, "store_unavailable", "Token inventory unavailable; retry", e)
		return
	}
	now := time.Now()
	out := make([]mcpTokenView, 0, len(tokens))
	for _, t := range tokens {
		// A revoked token is finished: it stays stored so a replayed secret is still refused and the audit
		// trail keeps its history, but the inventory no longer lists it.
		if t.Revoked != nil {
			continue
		}
		out = append(out, viewMCPToken(t, now))
	}
	respond(w, out, map[string]any{"enabled": a.o.MCPEnabled, "writesEnabled": a.o.MCPWritesEnabled, "maxActive": mcpMaxActiveTokens, "maxExpiryDays": mcpMaxExpiryDays})
}

func validMCPTokenName(name string) bool {
	n := utf8.RuneCountInString(name)
	return utf8.ValidString(name) && n >= 1 && n <= 64 && !strings.Contains(name, "/") && strings.IndexFunc(name, unicode.IsControl) < 0
}

func (a *API) createMCPToken(w http.ResponseWriter, r *http.Request, s auth.Session, az auth.Authorizer, manager bool) {
	u := s.User
	if !manager {
		fail(w, 403, "forbidden", "Global manage-mcp permission required to create MCP tokens")
		return
	}
	var body struct {
		Name          string          `json:"name"`
		Scopes        []string        `json:"scopes"`
		Clusters      []string        `json:"clusters"`
		ExpiresInDays json.RawMessage `json:"expiresInDays"`
	}
	if !decode(w, r, &body) {
		return
	}
	name := strings.TrimSpace(body.Name)
	if !validMCPTokenName(name) {
		fail(w, 400, "invalid_name", "Token name must be 1 to 64 characters without '/' or control characters")
		return
	}
	if len(body.Scopes) == 0 {
		fail(w, 400, "invalid_scope", "At least one scope is required; allowed: "+strings.Join(auth.MCPScopes, ", "))
		return
	}
	for _, sc := range body.Scopes {
		if !slices.Contains(auth.MCPScopes, sc) {
			fail(w, 400, "invalid_scope", fmt.Sprintf("Unknown scope %q; allowed: %s", sc, strings.Join(auth.MCPScopes, ", ")))
			return
		}
	}
	scopes := []string{}
	for _, sc := range auth.MCPScopes {
		if slices.Contains(body.Scopes, sc) {
			scopes = append(scopes, sc)
		}
	}
	raw := strings.TrimSpace(string(body.ExpiresInDays))
	if raw == "" || raw == "null" {
		fail(w, 400, "expiry_required", fmt.Sprintf("expiresInDays is required (1 to %d)", mcpMaxExpiryDays))
		return
	}
	days, e := strconv.Atoi(raw)
	if e != nil || days < 1 || days > mcpMaxExpiryDays {
		fail(w, 400, "invalid_expiry", fmt.Sprintf("expiresInDays must be a whole number from 1 to %d", mcpMaxExpiryDays))
		return
	}
	clusters := []string{}
	for _, c := range body.Clusters {
		if !slices.ContainsFunc(a.o.Clusters, func(k model.Cluster) bool { return k.ID == c }) {
			fail(w, 400, "invalid_cluster", fmt.Sprintf("Unknown cluster %q", c))
			return
		}
		if !slices.Contains(clusters, c) {
			clusters = append(clusters, c)
		}
	}
	// A token only narrows its owner, so refuse scopes the creator could not use.
	targets := clusters
	if len(targets) == 0 {
		targets = []string{"*"}
		for _, c := range a.o.Clusters {
			targets = append(targets, c.ID)
		}
	}
	for _, sc := range scopes {
		held := false
		for _, action := range auth.MCPScopeActions[sc] {
			for _, c := range targets {
				held = held || az.AnyAllowed(u, c, action)
			}
		}
		if !held {
			fail(w, 403, "scope_not_permitted", fmt.Sprintf("You hold no grant for the %s scope on the selected clusters", sc))
			return
		}
	}

	// Serialize creation so the active-token limit cannot be raced past.
	a.mcpTokenMu.Lock()
	defer a.mcpTokenMu.Unlock()
	own, e := a.visibleMCPTokens(r.Context(), u, false)
	if e != nil {
		failCause(w, r, 503, "store_unavailable", "Token inventory unavailable; retry", e)
		return
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	active := 0
	for _, t := range own {
		if mcpTokenStatus(t, now) != "active" {
			continue
		}
		active++
		if t.Name == name {
			fail(w, 409, "name_conflict", "You already have an active token with this name")
			return
		}
	}
	if active >= mcpMaxActiveTokens {
		fail(w, 409, "token_limit", fmt.Sprintf("At most %d active tokens per user; revoke one first", mcpMaxActiveTokens))
		return
	}
	tok := store.MCPToken{ID: auth.Token()[:32], Name: name, OwnerID: u.ID, OwnerProvider: u.Provider, OwnerRoles: slices.Clone(u.Roles), Scopes: scopes, Clusters: clusters, Created: now, Expires: now.Add(time.Duration(days) * 24 * time.Hour)}
	details := map[string]any{"scopes": scopes, "clusters": clusters, "expires": tok.Expires}
	if e = a.mcpTokenAudit(r, u, "mcp.token.create", tok, "intent", details); e != nil {
		failCause(w, r, 503, "audit_unavailable", "Token creation blocked because audit storage is unavailable", e)
		return
	}
	secret, hash := store.NewMCPSecret()
	createErr := a.o.Store.CreateMCPToken(r.Context(), tok, hash)
	result := "success"
	if createErr != nil {
		result = "failed"
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
	defer cancel()
	auditErr := a.mcpTokenAudit(r.WithContext(ctx), u, "mcp.token.create", tok, result, details)
	if createErr != nil {
		failCause(w, r, 503, "store_unavailable", "Token could not be saved; retry", createErr)
		return
	}
	if auditErr != nil {
		// Never hand out a secret whose issue is not on record.
		_ = a.o.Store.RevokeMCPToken(ctx, tok.ID)
		failCause(w, r, 503, "audit_uncertain", "Token was not issued because its audit record failed; retry", auditErr)
		return
	}
	v := viewMCPToken(tok, now)
	v.Secret = secret
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusCreated)
	respond(w, v)
}

func (a *API) revokeMCPToken(w http.ResponseWriter, r *http.Request, s auth.Session, manager bool, id string) {
	u := s.User
	find := func() (store.MCPToken, bool, error) {
		tokens, e := a.visibleMCPTokens(r.Context(), u, manager)
		if e != nil {
			return store.MCPToken{}, false, e
		}
		for _, t := range tokens {
			if t.ID == id {
				return t, true, nil
			}
		}
		return store.MCPToken{}, false, nil
	}
	tok, ok, e := find()
	if e != nil {
		failCause(w, r, 503, "store_unavailable", "Token inventory unavailable; retry", e)
		return
	}
	// Other users' tokens look unknown to callers without manage-mcp.
	if !ok {
		fail(w, 404, "not_found", "Token not found")
		return
	}
	if tok.Revoked != nil {
		respond(w, viewMCPToken(tok, time.Now()))
		return
	}
	if e = a.mcpTokenAudit(r, u, "mcp.token.revoke", tok, "intent", nil); e != nil {
		failCause(w, r, 503, "audit_unavailable", "Token revocation blocked because audit storage is unavailable", e)
		return
	}
	revokeErr := a.o.Store.RevokeMCPToken(r.Context(), id)
	result := "success"
	if revokeErr != nil {
		result = "failed"
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
	defer cancel()
	auditErr := a.mcpTokenAudit(r.WithContext(ctx), u, "mcp.token.revoke", tok, result, nil)
	if revokeErr != nil {
		if errors.Is(revokeErr, store.ErrMCPTokenNotFound) {
			fail(w, 404, "not_found", "Token not found")
			return
		}
		failCause(w, r, 503, "store_unavailable", "Token could not be revoked; retry", revokeErr)
		return
	}
	if auditErr != nil {
		failCause(w, r, 503, "audit_uncertain", "Token revoked but result audit failed", auditErr)
		return
	}
	if tok, ok, e = find(); e != nil || !ok {
		failCause(w, r, 503, "store_unavailable", "Token revoked; reload the inventory", e)
		return
	}
	respond(w, viewMCPToken(tok, time.Now()))
}

// mcpTokenAudit records token management under the console user, at the
// global "*" cluster. The secret and hash are never part of the record.
func (a *API) mcpTokenAudit(r *http.Request, u auth.User, action string, t store.MCPToken, result string, details any) error {
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return a.o.Store.Audit(r.Context(), store.Audit{Actor: u.ID, Provider: u.Provider, ClusterID: "*", Action: action, Resource: t.ID + " (" + t.Name + ")", Result: result, RequestID: requestID(r), SourceIP: ip, AdminAfter: details})
}
