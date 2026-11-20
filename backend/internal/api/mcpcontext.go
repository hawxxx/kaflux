package api

import (
	"context"
	"net/http"

	"github.com/hawxxx/kaflux/backend/internal/auth"
)

// mcpContextKey is unexported so no other package can place a session in a
// request context; only WithMCPSession can.
type mcpContextKey struct{}

type mcpSession struct {
	session auth.Session
	grants  []auth.Grant
}

// WithMCPSession attaches an already-authenticated MCP session and its
// narrowed grants to r. The API then authorizes r with those grants only and
// skips the cookie CSRF check, which does not apply to bearer-token callers.
func WithMCPSession(r *http.Request, s auth.Session, grants []auth.Grant) *http.Request {
	g := append([]auth.Grant{}, grants...)
	return r.WithContext(context.WithValue(r.Context(), mcpContextKey{}, mcpSession{session: s, grants: g}))
}

func mcpFrom(ctx context.Context) (mcpSession, bool) {
	m, ok := ctx.Value(mcpContextKey{}).(mcpSession)
	return m, ok
}

// grants returns the grants that authorize r: the narrowed MCP grants when r
// carries an MCP session, otherwise the configured grants.
func (a *API) grants(r *http.Request) []auth.Grant { return a.grantsFrom(r.Context()) }

func (a *API) grantsFrom(ctx context.Context) []auth.Grant {
	if m, ok := mcpFrom(ctx); ok {
		return m.grants
	}
	return a.o.Grants
}

// configuredGrants returns the configured grants, ignoring any MCP session.
// Only routes that refuse MCP sessions outright (token management) may use it,
// to judge what a console user could delegate to a new token.
func (a *API) configuredGrants() []auth.Grant { return a.o.Grants }
