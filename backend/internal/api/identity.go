package api

import (
	"crypto/subtle"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"log/slog"
	"net/http"
	"sort"
	"strings"
)

func (a *API) identity(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path == "/api/v1/auth/providers" && r.Method == "GET" {
		providers := []map[string]string{}
		if a.o.AdminHash != "" {
			providers = append(providers, map[string]string{"id": "local", "name": "Local account", "type": "local"})
		}
		ids := make([]string, 0, len(a.o.OIDC))
		for id := range a.o.OIDC {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			providers = append(providers, map[string]string{"id": id, "name": a.o.OIDC[id].Name(), "type": "oidc"})
		}
		for _, p := range a.o.LDAP {
			providers = append(providers, map[string]string{"id": p.ID, "name": p.ID, "type": "ldap"})
		}
		respond(w, providers)
		return true
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 6 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "auth" || parts[3] != "oidc" {
		return false
	}
	provider := a.o.OIDC[parts[4]]
	if provider == nil {
		fail(w, 404, "provider_not_found", "Identity provider not configured")
		return true
	}
	if r.Method != "GET" {
		fail(w, 405, "method_not_allowed", "GET required")
		return true
	}
	switch parts[5] {
	case "start":
		url, state, e := provider.StartURL()
		if e != nil {
			failCause(w, r, 503, "provider_unavailable", "Identity provider unavailable", e)
			return true
		}
		http.SetCookie(w, &http.Cookie{Name: "kaflux_oidc_state", Value: state, Path: "/api/v1/auth/oidc/", HttpOnly: true, Secure: !a.o.Demo, SameSite: http.SameSiteLaxMode, MaxAge: 600})
		http.Redirect(w, r, url, http.StatusFound)
	case "callback":
		state := r.URL.Query().Get("state")
		cookie, e := r.Cookie("kaflux_oidc_state")
		if e != nil || state == "" || subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(state)) != 1 {
			loginFailed(w, r, "sso_state")
			return true
		}
		http.SetCookie(w, &http.Cookie{Name: "kaflux_oidc_state", Value: "", Path: "/api/v1/auth/oidc/", HttpOnly: true, Secure: !a.o.Demo, SameSite: http.SameSiteLaxMode, MaxAge: -1})
		if r.URL.Query().Get("error") != "" {
			slog.Warn("oidc provider returned an error", "provider", parts[4], "error", truncate(r.URL.Query().Get("error"), 64))
			loginFailed(w, r, "sso_denied")
			return true
		}
		u, e := provider.Complete(r.Context(), state, r.URL.Query().Get("code"))
		if e != nil {
			// Errors are sanitized by the provider; a missing role mapping is the common misconfiguration.
			slog.Warn("oidc login rejected", "provider", parts[4], "error", e)
			loginFailed(w, r, "sso_rejected")
			return true
		}
		if !a.establish(w, r, u) {
			return true
		}
		http.Redirect(w, r, "/", http.StatusFound)
	default:
		fail(w, 404, "not_found", "Identity endpoint not found")
	}
	return true
}

// loginFailed returns the browser to the sign-in page with a stable error code.
func loginFailed(w http.ResponseWriter, r *http.Request, code string) {
	http.Redirect(w, r, "/login?error="+code, http.StatusFound)
}

func (a *API) establish(w http.ResponseWriter, r *http.Request, u auth.User) bool {
	_, ok := a.establishSession(w, r, u)
	return ok
}

func (a *API) establishSession(w http.ResponseWriter, r *http.Request, u auth.User) (auth.Session, bool) {
	s := auth.NewSession(u, a.o.SessionLifetime)
	if e := a.o.Store.SaveSession(r.Context(), s); e != nil {
		failCause(w, r, 503, "store_unavailable", "Unable to create session", e)
		return auth.Session{}, false
	}
	http.SetCookie(w, &http.Cookie{Name: "kaflux_session", Value: s.ID, Path: "/", HttpOnly: true, Secure: !a.o.Demo, SameSite: http.SameSiteLaxMode, Expires: s.Expires})
	return s, true
}
