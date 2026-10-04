package integrations

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestBoundedCachedClientAndRedaction(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		if r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("missing server-side credential")
		}
		w.Write([]byte(`{"name":"sample","password":"secret","nested":{"api.key":"secret","topic":"orders"}}`))
	}))
	defer server.Close()
	t.Setenv("FIXTURE_TOKEN", "fixture")
	client, e := New(Config{ID: "test", ClusterID: "cluster", Kind: "connect", URL: server.URL, TokenEnv: "FIXTURE_TOKEN"})
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	for i := 0; i < 2; i++ {
		raw, e := client.Do(context.Background(), "GET", "/connectors", nil)
		if e != nil {
			t.Fatal(e)
		}
		safe, e := Redact(raw)
		if e != nil || strings.Contains(string(safe), "secret") {
			t.Fatal("secret exposed", e)
		}
	}
	if count.Load() != 1 {
		t.Fatal("cache failed")
	}
}
func TestResponseLimitAndRedirects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "http://example.invalid", 302)
			return
		}
		w.Write(make([]byte, 2<<20))
	}))
	defer server.Close()
	client, e := New(Config{ID: "test", Kind: "schemas", URL: server.URL})
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	if _, e = client.Do(context.Background(), "GET", "/large", nil); e == nil {
		t.Fatal("oversize accepted")
	}
	if _, e = client.Do(context.Background(), "GET", "/redirect", nil); e == nil {
		t.Fatal("redirect accepted")
	}
}
