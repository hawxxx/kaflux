package api

import (
	"crypto/subtle"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"net/http"
	"strings"
)

func (a *API) identity(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path == "/api/v1/auth/providers" && r.Method == "GET" {
		providers := []map[string]string{{"id": "local", "name": "Local account", "type": "local"}}
		for id := range a.o.OIDC {
			providers = append(providers, map[string]string{"id": id, "name": id, "type": "oidc"})
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
			fail(w, 503, "provider_unavailable", "Identity provider unavailable")
			return true
		}
		http.SetCookie(w, &http.Cookie{Name: "kaflux_oidc_state", Value: state, Path: "/api/v1/auth/oidc/", HttpOnly: true, Secure: !a.o.Demo, SameSite: http.SameSiteLaxMode, MaxAge: 600})
		http.Redirect(w, r, url, http.StatusFound)
	case "callback":
		state := r.URL.Query().Get("state")
		cookie, e := r.Cookie("kaflux_oidc_state")
		if e != nil || state == "" || subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(state)) != 1 {
			fail(w, 403, "invalid_state", "Invalid login state")
			return true
		}
		http.SetCookie(w, &http.Cookie{Name: "kaflux_oidc_state", Value: "", Path: "/api/v1/auth/oidc/", HttpOnly: true, Secure: !a.o.Demo, SameSite: http.SameSiteLaxMode, MaxAge: -1})
		u, e := provider.Complete(r.Context(), state, r.URL.Query().Get("code"))
		if e != nil {
			fail(w, 401, "identity_rejected", "Identity verification failed")
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
func (a *API) establish(w http.ResponseWriter, r *http.Request, u auth.User) bool {
	_, ok := a.establishSession(w, r, u)
	return ok
}

func (a *API) establishSession(w http.ResponseWriter, r *http.Request, u auth.User) (auth.Session, bool) {
	s := auth.NewSession(u, a.o.SessionLifetime)
	if e := a.o.Store.SaveSession(r.Context(), s); e != nil {
		fail(w, 503, "store_unavailable", "Unable to create session")
		return auth.Session{}, false
	}
	http.SetCookie(w, &http.Cookie{Name: "kaflux_session", Value: s.ID, Path: "/", HttpOnly: true, Secure: !a.o.Demo, SameSite: http.SameSiteLaxMode, Expires: s.Expires})
	return s, true
}
