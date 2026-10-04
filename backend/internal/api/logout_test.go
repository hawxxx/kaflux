package api

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http/httptest"
	"testing"
)

func TestLogoutRevocationFailureKeepsCookie(t *testing.T) {
	// A closed pool deterministically rejects revocation without a live database.
	pool, err := pgxpool.New(context.Background(), "postgres://test:test@127.0.0.1:1/test")
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	a := New(Options{Demo: true, Store: &store.Store{DB: pool}})
	request := httptest.NewRequest("POST", "/api/v1/auth/logout", nil)
	request.Header.Set("X-CSRF-Token", a.demo.CSRF)
	response := httptest.NewRecorder()
	a.ServeHTTP(response, request)
	if response.Code != 503 {
		t.Fatalf("logout reported %d: %s", response.Code, response.Body.String())
	}
	if len(response.Result().Cookies()) != 0 {
		t.Fatal("failed revocation cleared the retryable session cookie")
	}
}

func TestLogoutSuccessClearsCookie(t *testing.T) {
	s, err := store.New(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a := New(Options{Demo: true, Store: s})
	request := httptest.NewRequest("POST", "/api/v1/auth/logout", nil)
	request.Header.Set("X-CSRF-Token", a.demo.CSRF)
	response := httptest.NewRecorder()
	a.ServeHTTP(response, request)
	cookies := response.Result().Cookies()
	if response.Code != 200 || len(cookies) != 1 || cookies[0].Name != "kaflux_session" || cookies[0].MaxAge != -1 {
		t.Fatalf("%d cookies=%v", response.Code, cookies)
	}
}
