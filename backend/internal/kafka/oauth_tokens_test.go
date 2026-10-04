package kafka

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOAuthClientCredentialsCoalescesRefresh(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		u, p, ok := r.BasicAuth()
		r.ParseForm()
		if !ok || u != "client" || p != "test-secret" || r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("scope") != "kafka.read" {
			t.Error("invalid client credentials request")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"access_token":"test-token","token_type":"Bearer","expires_in":3600}`))
	}))
	defer server.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600)
	t.Setenv("KAFLUX_TEST_OAUTH_SECRET", "test-secret")
	source, err := newOAuthTokens(Config{OAuthTokenEndpoint: server.URL, OAuthClientID: "client", OAuthClientSecretEnv: "KAFLUX_TEST_OAUTH_SECRET", OAuthScopes: []string{"kafka.read"}, OAuthCAFile: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := source.Token(context.Background())
			if err != nil || token != "test-token" {
				t.Errorf("token failed: %v", err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("%d token requests", calls.Load())
	}
	source.expires = time.Now().Add(-time.Second)
	if _, err := source.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("expired token was not refreshed")
	}
}

func TestOAuthTokenErrorsAreBoundedAndRedacted(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(401)
		w.Write([]byte(`private-response-test-secret`))
	}))
	defer server.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600)
	t.Setenv("KAFLUX_TEST_OAUTH_SECRET", "test-secret")
	cfg := Config{OAuthTokenEndpoint: server.URL, OAuthClientID: "client", OAuthClientSecretEnv: "KAFLUX_TEST_OAUTH_SECRET", OAuthCAFile: ca}
	source, err := newOAuthTokens(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	for i := 0; i < 2; i++ {
		_, err := source.Token(context.Background())
		if err == nil || strings.Contains(err.Error(), "test-secret") || strings.Contains(err.Error(), "private-response") {
			t.Fatal("unsafe token error")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("provider failure was not backed off")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.Token(cancelled); err == nil {
		t.Fatal("cancelled acquisition accepted")
	}
	cfg.OAuthTokenEndpoint = "http://localhost/token"
	if _, err := newOAuthTokens(cfg); err == nil {
		t.Fatal("plaintext token endpoint accepted")
	}
}

func TestOAuthRejectsInvalidResponsesAndRedirects(t *testing.T) {
	t.Setenv("KAFLUX_TEST_OAUTH_SECRET", "test-secret")
	for _, sample := range []struct {
		body     string
		redirect bool
	}{
		{`{"access_token":"token","token_type":"Bearer"}`, false},
		{`{"access_token":"token","token_type":"Bearer","expires_in":-1}`, false},
		{`{"access_token":"token","token_type":"Basic","expires_in":3600}`, false},
		{`{"access_token":"bad\nvalue","token_type":"Bearer","expires_in":3600}`, false},
		{strings.Repeat("x", 65537), false}, {"", true},
	} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if sample.redirect {
				w.Header().Set("Location", "https://example.invalid/private-secret")
				w.WriteHeader(302)
				return
			}
			w.Write([]byte(sample.body))
		}))
		ca := filepath.Join(t.TempDir(), "ca.pem")
		os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600)
		source, err := newOAuthTokens(Config{OAuthTokenEndpoint: server.URL, OAuthClientID: "client", OAuthClientSecretEnv: "KAFLUX_TEST_OAUTH_SECRET", OAuthCAFile: ca})
		if err != nil {
			t.Fatal(err)
		}
		_, err = source.Token(context.Background())
		source.Close()
		server.Close()
		if err == nil || strings.Contains(err.Error(), "private-secret") {
			t.Fatal("unsafe response accepted")
		}
	}
}

func TestNativeOAuthClientCredentialsConfiguration(t *testing.T) {
	t.Setenv("KAFLUX_TEST_OAUTH_SECRET", "test-secret")
	cfg := Config{Seeds: []string{"localhost:9093"}, TLS: true, SASL: "oauthbearer", OAuthTokenEndpoint: "https://identity.example/token", OAuthClientID: "client", OAuthClientSecretEnv: "KAFLUX_TEST_OAUTH_SECRET"}
	native, err := NewNative(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if native.oauthTokens == nil {
		t.Fatal("client credentials not wired to native provider")
	}
	native.Close()
	cfg.TLS = false
	cfg.AllowPlaintext = true
	if n, err := NewNative(cfg); err == nil {
		n.Close()
		t.Fatal("Kafka TLS not enforced")
	}
	cfg.TLS = true
	cfg.OAuthTokenEnv = "KAFLUX_TEST_STATIC_TOKEN"
	t.Setenv(cfg.OAuthTokenEnv, "test-token")
	if n, err := NewNative(cfg); err == nil {
		n.Close()
		t.Fatal("ambiguous static/client mode accepted")
	}
	cfg.OAuthTokenEndpoint = ""
	cfg.OAuthClientID = ""
	cfg.OAuthClientSecretEnv = ""
	native, err = NewNative(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	if native.oauthTokens != nil {
		t.Fatal("static mode unexpectedly changed")
	}
}

func TestOAuthProviderFailureNeverReturnsExpiredToken(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KAFLUX_TEST_OAUTH_SECRET", "test-secret")
	source, err := newOAuthTokens(Config{OAuthTokenEndpoint: server.URL, OAuthClientID: "client", OAuthClientSecretEnv: "KAFLUX_TEST_OAUTH_SECRET", OAuthCAFile: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	source.token = "still-valid"
	source.expires = time.Now().Add(time.Minute)
	source.refreshAt = time.Now().Add(-time.Second)
	if token, err := source.Token(context.Background()); err != nil || token != "still-valid" {
		t.Fatalf("unexpired fallback unavailable: %v", err)
	}
	source.expires = time.Now().Add(-time.Second)
	if token, err := source.Token(context.Background()); err == nil || token != "" {
		t.Fatal("expired token returned during provider backoff")
	}
	if calls.Load() != 1 {
		t.Fatal("backoff did not prevent a second provider request")
	}
}
