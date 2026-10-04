package api

import (
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func securityHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	// Browsers ignore HSTS over plain HTTP, so local and demo use is unaffected.
	w.Header().Set("Strict-Transport-Security", "max-age=31536000")
	w.Header().Set("Cross-Origin-Opener-Policy", "same-origin")
	w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
	// React and uPlot set inline styles; scripts and fonts remain restricted to our origin.
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; font-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
}

// Login has no authenticated session from which to obtain a CSRF token.
// JSON is not a simple browser form type, and browser origin metadata must
// agree with the host. Forwarded headers are deliberately not trusted.
func loginRequestAllowed(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		fail(w, http.StatusForbidden, "csrf_failed", "Same-origin login required")
		return false
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if err != nil || u == nil || (u.Scheme != "https" && u.Scheme != "http") || !strings.EqualFold(u.Host, r.Host) || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || (r.TLS != nil && u.Scheme != "https") {
			fail(w, http.StatusForbidden, "csrf_failed", "Same-origin login required")
			return false
		}
	}
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		fail(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Login requires application/json")
		return false
	}
	return true
}

const maxLoginClients = 10000
const maxLoginAttempts = 5

func (a *API) loginAllowed(remoteAddr string, now time.Time) bool {
	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		ip = remoteAddr
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	// Evict expired clients periodically without ever resetting active limits.
	if now.Sub(a.lastAttemptCleanup) >= time.Minute {
		for key, attempts := range a.attempts {
			if len(attempts) == 0 || now.Sub(attempts[len(attempts)-1]) >= time.Minute {
				delete(a.attempts, key)
			}
		}
		a.lastAttemptCleanup = now
	}
	recent := a.attempts[ip][:0]
	for _, attempted := range a.attempts[ip] {
		if now.Sub(attempted) < time.Minute {
			recent = append(recent, attempted)
		}
	}
	if len(recent) >= maxLoginAttempts {
		return false
	}
	if _, exists := a.attempts[ip]; !exists && len(a.attempts) >= maxLoginClients {
		return false
	}
	a.attempts[ip] = append(recent, now)
	return true
}

// truncate bounds user-supplied values written to logs.
func truncate(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}
