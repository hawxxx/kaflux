package kafka

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/auth"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
)

func TestGoogleKafkaBearer(t *testing.T) {
	now := time.Unix(1700000000, 0)
	expiry := now.Add(time.Hour)
	principal := "kaflux@example.iam.gserviceaccount.com"
	token := &auth.Token{Value: "fixture-access-token", Expiry: expiry}
	bearer, err := googleKafkaBearer(token, principal, now)
	if err != nil {
		t.Fatal(err)
	}
	segments := strings.Split(bearer.Token, ".")
	if len(segments) != 3 {
		t.Fatal("expected three Google bearer token segments")
	}
	for i, want := range []string{
		`{"typ":"JWT","alg":"GOOG_OAUTH2_TOKEN"}`,
		fmt.Sprintf(`{"iss":"Google","sub":%q,"iat":%d,"exp":%d}`, principal, now.Unix(), expiry.Unix()),
		token.Value,
	} {
		got, err := base64.RawURLEncoding.DecodeString(segments[i])
		if err != nil || string(got) != want {
			t.Fatalf("segment %d: got %q, want %q, error %v", i, got, want, err)
		}
	}
	if _, err := googleKafkaBearer(token, principal, expiry); err == nil {
		t.Fatal("accepted expired token")
	}
}

func TestCloudIAMBrokerBoundary(t *testing.T) {
	for _, tc := range []struct{ mode, seed, broker, outside string }{
		{"gcp-iam", "bootstrap.test.us-central1.managedkafka.example.cloud.goog:9092", "broker-0.test.us-central1.managedkafka.example.cloud.goog:9092", "broker-0.other.us-central1.managedkafka.example.cloud.goog:9092"},
		{"azure-iam", "example.servicebus.windows.net:9093", "EXAMPLE.servicebus.windows.net:9093", "different.servicebus.windows.net:9093"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			c := Config{SASL: tc.mode, Seeds: []string{tc.seed}, AllowPlaintext: true}
			if _, err := NewNative(c); err == nil || !strings.Contains(err.Error(), "requires TLS") {
				t.Fatalf("expected TLS requirement, got %v", err)
			}
			c.TLS = true
			_, allows, err := cloudBrokerBoundary(c)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			mechanism := cloudIAMMechanism{allows: allows, Mechanism: oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
				calls++
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 5*time.Second {
					t.Error("token request is unbounded")
				}
				return oauth.Auth{Token: "fixture"}, nil
			})}
			if _, _, err := mechanism.Authenticate(t.Context(), tc.outside); err == nil || calls != 0 {
				t.Fatal("authentication requested credentials for a broker outside the configured endpoint")
			}
			if _, _, err := mechanism.Authenticate(t.Context(), tc.broker); err != nil || calls != 1 {
				t.Fatalf("valid broker authentication failed: %v", err)
			}
			c.Seeds = append(c.Seeds, tc.outside)
			if _, _, err := cloudBrokerBoundary(c); err == nil {
				t.Fatal("accepted seeds from different endpoints")
			}
		})
	}
}

func TestAzureEventHubsManagedIdentity(t *testing.T) {
	const namespace = "example.servicebus.windows.net"
	const fixture = "fixture-azure-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("resource"); got != "https://"+namespace {
			t.Errorf("managed identity resource = %q", got)
		}
		if r.Header.Get("X-IDENTITY-HEADER") != "fixture-header" {
			t.Error("expected managed identity header")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"access_token": fixture,
			"expires_on":   fmt.Sprint(time.Now().Add(time.Hour).Unix()),
			"token_type":   "Bearer",
			"resource":     "https://" + namespace,
		})
	}))
	defer server.Close()
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "ManagedIdentityCredential")
	t.Setenv("AZURE_CLIENT_ID", "11111111-1111-1111-1111-111111111111")
	t.Setenv("IDENTITY_ENDPOINT", server.URL)
	t.Setenv("IDENTITY_HEADER", "fixture-header")
	t.Setenv("IDENTITY_SERVER_THUMBPRINT", "")
	mechanism, err := newCloudIAM(Config{SASL: "azure-iam", TLS: true, Seeds: []string{namespace + ":9093"}})
	if err != nil {
		t.Fatal(err)
	}
	_, payload, err := mechanism.Authenticate(t.Context(), namespace+":9093")
	if err != nil || string(payload) != "n,,\x01auth=Bearer "+fixture+"\x01\x01" {
		t.Fatalf("unexpected OAUTHBEARER exchange: %q, %v", payload, err)
	}
}

func TestGoogleManagedKafkaADC(t *testing.T) {
	const principal = "kaflux@example.iam.gserviceaccount.com"
	const broker = "bootstrap.test.us-central1.managedkafka.example.cloud.goog:9092"
	const fixture = "fixture-google-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
			t.Error("expected service account assertion exchange")
		}
		parts := strings.Split(r.Form.Get("assertion"), ".")
		if len(parts) != 3 {
			t.Error("expected a signed assertion")
			return
		}
		claims, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil || !strings.Contains(string(claims), "https://www.googleapis.com/auth/cloud-platform") {
			t.Error("expected the Google Cloud scope")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"access_token":%q,"expires_in":3600,"token_type":"Bearer"}`, fixture)
	}))
	defer server.Close()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]string{
		"type":         "service_account",
		"client_email": principal,
		"private_key": string(pem.EncodeToMemory(&pem.Block{
			Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key),
		})),
		"token_uri": server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)
	t.Setenv("GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL", principal)

	mechanism, err := newCloudIAM(Config{SASL: "gcp-iam", TLS: true, Seeds: []string{broker}})
	if err != nil {
		t.Fatal(err)
	}
	_, payload, err := mechanism.Authenticate(t.Context(), broker)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(payload), "."+base64.RawURLEncoding.EncodeToString([]byte(fixture))+"\x01\x01") {
		t.Fatal("SASL exchange did not include the wrapped ADC access token")
	}
}
