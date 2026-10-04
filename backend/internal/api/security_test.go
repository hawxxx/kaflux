package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/store"
	"golang.org/x/crypto/bcrypt"
)

func securityAPI(t *testing.T) *API {
	t.Helper()
	s, err := store.New(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	hash, err := bcrypt.GenerateFromPassword([]byte("fixture-password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return New(Options{Store: s, AdminUser: "fixture", AdminHash: string(hash)})
}

func TestLoginRejectsCrossSiteAndSimpleContentTypes(t *testing.T) {
	for _, tc := range []struct {
		name, origin, site, contentType string
		status                          int
	}{
		{"foreign origin", "https://attacker.example", "", "application/json", 403},
		{"null origin", "null", "", "application/json", 403},
		{"cross site", "", "cross-site", "application/json", 403},
		{"subdomain origin", "https://other.example.com", "same-site", "application/json", 403},
		{"plain JSON form", "", "", "text/plain", 415},
		{"form encoding", "", "", "application/x-www-form-urlencoded", 415},
		{"missing type", "", "", "", 415},
		{"same origin", "https://example.com", "same-origin", "application/json; charset=utf-8", 200},
		{"non browser", "", "", "application/json", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := securityAPI(t)
			r := httptest.NewRequest("POST", "https://example.com/api/v1/auth/login", strings.NewReader(`{"username":"fixture","password":"fixture-password"}`))
			r.Header.Set("Origin", tc.origin)
			r.Header.Set("Sec-Fetch-Site", tc.site)
			r.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			a.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("got %d: %s", w.Code, w.Body.String())
			}
			if tc.status != 200 && len(w.Result().Cookies()) != 0 {
				t.Fatal("rejected login established a cookie")
			}
		})
	}
}

func TestSecurityHeadersOnAPIAndStaticResponses(t *testing.T) {
	a := securityAPI(t)
	a.o.StaticDir = t.TempDir()
	if err := os.WriteFile(filepath.Join(a.o.StaticDir, "index.html"), []byte("<!doctype html><title>Kaflux</title>"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/health", "/", "/api/v1/auth/session"} {
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", "https://example.com"+path, nil))
		for key, expected := range map[string]string{"X-Frame-Options": "DENY", "Referrer-Policy": "no-referrer", "X-Content-Type-Options": "nosniff", "Strict-Transport-Security": "max-age=31536000", "Cross-Origin-Opener-Policy": "same-origin", "Cross-Origin-Resource-Policy": "same-origin"} {
			if w.Header().Get(key) != expected {
				t.Errorf("%s: missing %s", path, key)
			}
		}
		csp := w.Header().Get("Content-Security-Policy")
		if strings.Contains(csp, "fonts.g") || !strings.Contains(csp, "font-src 'self';") {
			t.Errorf("%s: CSP must not allow third-party fonts: %s", path, csp)
		}
		for _, directive := range []string{"script-src 'self'", "frame-ancestors 'none'", "object-src 'none'", "base-uri 'none'"} {
			if !strings.Contains(csp, directive) {
				t.Errorf("%s: missing %s", path, directive)
			}
		}
	}
}

func TestLoginThrottleBoundsBlockedTraffic(t *testing.T) {
	a := securityAPI(t)
	for i := 0; i < 100; i++ {
		r := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(`{"username":"wrong","password":"wrong"}`))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		if i >= 5 && w.Code != http.StatusTooManyRequests {
			t.Fatalf("attempt %d: %d", i, w.Code)
		}
	}
	for ip, attempts := range a.attempts {
		if len(attempts) > 5 {
			t.Errorf("%s retained %d blocked attempts", ip, len(attempts))
		}
	}
}

func TestLoginThrottleCapacityDoesNotResetExistingLimits(t *testing.T) {
	a := securityAPI(t)
	now := time.Now()
	for i := 0; i < 10000; i++ {
		a.attempts[fmt.Sprint(i)] = []time.Time{now}
	}
	a.attempts["192.0.2.1"] = []time.Time{now, now, now, now, now}
	r := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(`{"username":"wrong","password":"wrong"}`))
	r.Header.Set("Content-Type", "application/json")
	a.ServeHTTP(httptest.NewRecorder(), r)
	if len(a.attempts["192.0.2.1"]) != 5 {
		t.Fatal("full limiter discarded an active IP limit")
	}
}

func TestLoginThrottleRejectsNewClientsAtCapacityAndExpires(t *testing.T) {
	a := securityAPI(t)
	now := time.Now()
	a.lastAttemptCleanup = now
	for i := 0; i < maxLoginClients; i++ {
		a.attempts[fmt.Sprint(i)] = []time.Time{now}
	}
	if a.loginAllowed("203.0.113.1:1234", now) {
		t.Fatal("new client bypassed full limiter")
	}
	if len(a.attempts) != maxLoginClients {
		t.Fatal("limiter exceeded client bound")
	}
	if !a.loginAllowed("203.0.113.1:1234", now.Add(time.Minute)) {
		t.Fatal("expired limits did not release capacity")
	}
}

func TestMalformedLoginRequestsAreAlsoThrottled(t *testing.T) {
	a := securityAPI(t)
	for i := 0; i < 6; i++ {
		r := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader("{"))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		if i == 5 && w.Code != http.StatusTooManyRequests {
			t.Fatal("malformed bodies bypassed login throttle")
		}
	}
}
