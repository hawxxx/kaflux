package kafka

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestOAuthClientCredentialsConfiguration(t *testing.T) {
	t.Setenv("KAFLUX_FIXTURE_CLIENT_SECRET", "private-client-secret")
	var c Config
	if err := json.Unmarshal([]byte(`{"seeds":["broker.invalid:9093"],"tls":true,"sasl":"oauthbearer","oauthTokenEndpoint":"https://identity.example.invalid/token","oauthClientId":"kaflux","oauthClientSecretEnv":"KAFLUX_FIXTURE_CLIENT_SECRET","oauthScopes":["kafka"]}`), &c); err != nil {
		t.Fatal(err)
	}
	n, err := NewNative(c)
	if err != nil {
		t.Fatalf("client credentials configuration rejected: %v", err)
	}
	n.Close()
}

func TestOAuthBearerRequiresTLSAndSecretReference(t *testing.T) {
	c := Config{Seeds: []string{"broker.invalid:9093"}, SASL: "oauthbearer", OAuthTokenEnv: "KAFLUX_FIXTURE_OAUTH", AllowPlaintext: true}
	t.Setenv(c.OAuthTokenEnv, "private-fixture-token")
	if _, e := NewNative(c); e == nil || !strings.Contains(e.Error(), "TLS") {
		t.Fatal("OAuth bearer allowed without TLS")
	}
	c.TLS = true
	t.Setenv(c.OAuthTokenEnv, "")
	if _, e := NewNative(c); e == nil {
		t.Fatal("missing bearer token accepted")
	}
	t.Setenv(c.OAuthTokenEnv, "private-fixture-token")
	n, e := NewNative(c)
	if e != nil {
		t.Fatal(e)
	}
	n.Close()
}
