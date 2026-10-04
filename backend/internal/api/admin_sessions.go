package api

import (
	"errors"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"net"
	"net/http"
	"strconv"
	"strings"
)

func (a *API) adminSessions(w http.ResponseWriter, r *http.Request, s auth.Session) bool {
	const base = "/api/v1/admin/sessions"
	if r.URL.Path != base && !strings.HasPrefix(r.URL.Path, base+"/") {
		return false
	}
	az := auth.Authorizer{Grants: a.o.Grants}
	if r.URL.Path == base {
		if r.Method != "GET" {
			fail(w, 405, "method_not_allowed", "GET required")
			return true
		}
		if !az.Allowed(s.User, "*", "manage-sessions", "*") {
			fail(w, 403, "forbidden", "Global session administration permission required")
			return true
		}
		limit, offset := 25, 0
		for _, p := range []struct {
			name     string
			dest     *int
			min, max int
		}{{"limit", &limit, 1, 100}, {"offset", &offset, 0, 10000}} {
			if values, ok := r.URL.Query()[p.name]; ok {
				if len(values) != 1 {
					fail(w, 400, "invalid_pagination", "Invalid session pagination")
					return true
				}
				n, e := strconv.Atoi(values[0])
				if e != nil || n < p.min || n > p.max {
					fail(w, 400, "invalid_pagination", "Invalid session pagination")
					return true
				}
				*p.dest = n
			}
		}
		var items []store.SessionSummary
		var more bool
		var e error
		if a.o.Demo {
			items = []store.SessionSummary{}
			if offset == 0 {
				items = append(items, store.SummarizeSession(s, s.ID))
			}
		} else {
			items, more, e = a.o.Store.ListSessions(r.Context(), s.ID, limit, offset)
		}
		if e != nil {
			failCause(w, r, 503, "store_unavailable", "Session inventory unavailable; retry", e)
			return true
		}
		respond(w, items, map[string]any{"limit": limit, "offset": offset, "hasMore": more})
		return true
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, base+"/"), "/")
	if len(parts) != 2 || parts[1] != "revoke" {
		fail(w, 404, "not_found", "Endpoint not found")
		return true
	}
	if r.Method != "POST" {
		fail(w, 405, "method_not_allowed", "POST required")
		return true
	}
	handle := parts[0]
	if len(handle) != 64 || strings.Trim(handle, "0123456789abcdef") != "" {
		fail(w, 400, "invalid_handle", "Invalid session handle")
		return true
	}
	if !az.AnyAllowed(s.User, "*", "manage-sessions") {
		fail(w, 403, "forbidden", "Global session administration permission required")
		return true
	}
	var body struct {
		Confirm bool `json:"confirm"`
	}
	if !decode(w, r, &body) {
		return true
	}
	if !body.Confirm {
		fail(w, 400, "confirmation_required", "Confirm session revocation")
		return true
	}
	if a.o.Demo {
		fail(w, 422, "simulation_unsupported", "Simulator sessions cannot be revoked")
		return true
	}
	sourceIP, _, splitErr := net.SplitHostPort(r.RemoteAddr)
	if splitErr != nil {
		sourceIP = r.RemoteAddr
	}
	v, e := a.o.Store.RevokeSession(r.Context(), handle, s.ID, func(v store.SessionSummary) bool { return az.Allowed(s.User, "*", "manage-sessions", v.UserID) }, store.Audit{Actor: s.User.ID, Provider: s.User.Provider, ClusterID: "*", Action: "manage-sessions", RequestID: requestID(r), SourceIP: sourceIP})
	switch {
	case errors.Is(e, store.ErrSessionForbidden):
		fail(w, 403, "forbidden", "Permission denied")
	case errors.Is(e, store.ErrSessionNotFound):
		fail(w, 404, "session_not_found", "Session is no longer active")
	case e != nil:
		failCause(w, r, 503, "session_revocation_failed", "Session revocation unavailable; retry", e)
	default:
		if v.Current {
			http.SetCookie(w, &http.Cookie{Name: "kaflux_session", Value: "", Path: "/", MaxAge: -1, HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode})
		}
		respond(w, map[string]bool{"ok": true, "current": v.Current})
	}
	return true
}
