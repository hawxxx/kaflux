package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

func TestOIDCVerifiedLoginStateNonceAndPKCE(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var issuer, nonce, challenge string
	badAudience := false
	encode := func(v any) string { b, _ := json.Marshal(v); return base64.RawURLEncoding.EncodeToString(b) }
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "authorization_endpoint": issuer + "/authorize", "token_endpoint": issuer + "/token", "jwks_uri": issuer + "/keys", "id_token_signing_alg_values_supported": []string{"RS256"}})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{"kty": "RSA", "kid": "fixture", "use": "sig", "alg": "RS256", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		h := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		if base64.RawURLEncoding.EncodeToString(h[:]) != challenge {
			t.Error("PKCE verifier did not match authorization challenge")
			http.Error(w, "PKCE", 400)
			return
		}
		audience := "fixture-client"
		if badAudience {
			audience = "different-client"
		}
		signed := encode(map[string]any{"alg": "RS256", "kid": "fixture"}) + "." + encode(map[string]any{"iss": issuer, "aud": audience, "sub": "operator", "preferred_username": "alice", "exp": time.Now().Add(time.Hour).Unix(), "iat": time.Now().Unix(), "nonce": nonce, "groups": []string{"operations"}})
		digest := sha256.Sum256([]byte(signed))
		sig, _ := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "fixture", "token_type": "Bearer", "id_token": signed + "." + base64.RawURLEncoding.EncodeToString(sig)})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	issuer = server.URL
	provider, err := NewOIDC(context.Background(), OIDCConfig{ID: "authentik", Issuer: issuer, ClientID: "fixture-client", RedirectURL: "http://localhost/callback", AllowHTTP: true, GroupRoles: map[string][]string{"operations": {"operator"}}})
	if err != nil {
		t.Fatal(err)
	}
	flows := &fixtureFlows{values: map[string]OIDCPending{}}
	provider.SetFlowStore(flows)
	callbackProvider, err := NewOIDC(context.Background(), provider.cfg)
	if err != nil {
		t.Fatal(err)
	}
	callbackProvider.SetFlowStore(flows)
	start := func() string {
		raw, state, err := provider.StartURL()
		if err != nil {
			t.Fatal(err)
		}
		u, _ := url.Parse(raw)
		nonce = u.Query().Get("nonce")
		challenge = u.Query().Get("code_challenge")
		if nonce == "" || challenge == "" || u.Query().Get("code_challenge_method") != "S256" {
			t.Fatal("authorization protections missing")
		}
		return state
	}
	state := start()
	otherConfig := provider.cfg
	otherConfig.ID = "different-provider"
	otherProvider, err := NewOIDC(context.Background(), otherConfig)
	if err != nil {
		t.Fatal(err)
	}
	otherProvider.SetFlowStore(flows)
	if _, err := otherProvider.Complete(context.Background(), state, "code"); err == nil {
		t.Fatal("login state accepted by a different provider")
	}
	if _, err := provider.Complete(context.Background(), "unknown-state", "code"); err == nil {
		t.Fatal("unknown state accepted")
	}
	user, err := callbackProvider.Complete(context.Background(), state, "code")
	if err != nil {
		t.Fatal(err)
	}
	if user.Username != "alice" || user.Provider != "authentik" || strings.Join(user.Roles, ",") != "operator" {
		t.Fatalf("incorrect verified identity: %+v", user)
	}
	if _, err := provider.Complete(context.Background(), state, "code"); err == nil {
		t.Fatal("state replay accepted")
	}
	state = start()
	badAudience = true
	if _, err := provider.Complete(context.Background(), state, "code"); err == nil {
		t.Fatal("wrong audience accepted")
	}
	state = start()
	badAudience = false
	nonce = "wrong-nonce"
	if _, err := provider.Complete(context.Background(), state, "code"); err == nil {
		t.Fatal("wrong nonce accepted")
	}
}

type fixtureFlows struct {
	mu     sync.Mutex
	values map[string]OIDCPending
}

func (f *fixtureFlows) SaveOIDCState(ctx context.Context, id string, p OIDCPending) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.values[id] = p
	return nil
}
func (f *fixtureFlows) ConsumeOIDCState(ctx context.Context, id string) (OIDCPending, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.values[id]
	delete(f.values, id)
	if !ok {
		return p, fmt.Errorf("state absent")
	}
	return p, nil
}

func TestOIDCRejectsHTTPByDefault(t *testing.T) {
	if _, err := NewOIDC(context.Background(), OIDCConfig{Issuer: "http://example.invalid", ClientID: "id", RedirectURL: "https://console.example.invalid/callback"}); err == nil {
		t.Fatal("insecure issuer accepted")
	}
}

func TestOIDCRejectsInsecureDiscoveryKeyEndpoint(t *testing.T) {
	var issuer string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "authorization_endpoint": issuer + "/authorize", "token_endpoint": issuer + "/token", "jwks_uri": "http://keys.example.invalid", "id_token_signing_alg_values_supported": []string{"RS256"}})
	}))
	defer server.Close()
	issuer = server.URL
	ctx := oidc.ClientContext(context.Background(), server.Client())
	if _, err := NewOIDC(ctx, OIDCConfig{Issuer: issuer, ClientID: "client", RedirectURL: "https://console.example.invalid/callback"}); err == nil {
		t.Fatal("insecure key endpoint accepted")
	}
}
