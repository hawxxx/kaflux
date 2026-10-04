package api

import (
	"context"
	"golang.org/x/crypto/bcrypt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

func TestProviderSessionLifetimeAndCookie(t *testing.T) {
	for _, provider := range []string{"local", "oidc", "ldap"} {
		t.Run(provider, func(t *testing.T) {
			st, err := store.New(context.Background(), "")
			if err != nil {
				t.Fatal(err)
			}
			a := New(Options{Store: st, SessionLifetime: 15 * time.Minute})
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/", nil)
			before := time.Now()
			if !a.establish(w, r, auth.User{ID: "fixture", Provider: provider}) {
				t.Fatal("session establishment failed")
			}
			cookies := w.Result().Cookies()
			if len(cookies) != 1 {
				t.Fatal("missing session cookie")
			}
			session, err := st.Session(context.Background(), cookies[0].Value)
			if err != nil {
				t.Fatal(err)
			}
			if session.Expires.Before(before.Add(15*time.Minute)) || session.Expires.After(time.Now().Add(15*time.Minute)) {
				t.Fatalf("wrong %s lifetime", provider)
			}
			if !cookies[0].Expires.Equal(session.Expires.Truncate(time.Second)) {
				t.Fatal("cookie differs from stored expiry")
			}
		})
	}
}

func TestLocalLoginUsesConfiguredLifetime(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("fixture-password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.New(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	a := New(Options{Store: st, AdminUser: "fixture", AdminHash: string(hash), SessionLifetime: 5 * time.Minute})
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(`{"username":"fixture","password":"fixture-password"}`))
	before := time.Now()
	a.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("login status %d", w.Code)
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatal("missing cookie")
	}
	session, err := st.Session(context.Background(), cookies[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if session.Expires.Before(before.Add(5*time.Minute)) || session.Expires.After(time.Now().Add(5*time.Minute)) {
		t.Fatal("local login ignored configured lifetime")
	}
	if !cookies[0].Expires.Equal(session.Expires.Truncate(time.Second)) {
		t.Fatal("cookie differs from stored expiry")
	}
}
