package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
)

var (
	googleKafkaHost = regexp.MustCompile(`^[a-z0-9-]+\.[a-z0-9-]+\.[a-z0-9-]+\.managedkafka\.[a-z0-9-]+\.cloud\.goog$`)
	eventHubsHost   = regexp.MustCompile(`^[a-z0-9-]+\.servicebus\.windows\.net$`)
)

// Scope bearer credentials to the configured cloud endpoint, including when
// connecting to brokers learned from metadata.
type cloudIAMMechanism struct {
	sasl.Mechanism
	allows func(string) bool
}

func (m cloudIAMMechanism) Authenticate(ctx context.Context, host string) (sasl.Session, []byte, error) {
	if !m.allows(brokerHostname(host)) {
		return nil, nil, errors.New("broker hostname is outside the configured cloud IAM endpoint")
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return m.Mechanism.Authenticate(bounded, host)
}

func brokerHostname(address string) string {
	if host, _, err := net.SplitHostPort(address); err == nil {
		address = host
	}
	return strings.ToLower(strings.TrimSuffix(address, "."))
}

func cloudBrokerBoundary(c Config) (string, func(string) bool, error) {
	if !c.TLS {
		return "", nil, fmt.Errorf("%s requires TLS", c.SASL)
	}
	host := brokerHostname(c.Seeds[0])
	var allows func(string) bool
	switch c.SASL {
	case "gcp-iam":
		if !googleKafkaHost.MatchString(host) {
			return "", nil, errors.New("gcp-iam requires a Google Managed Kafka bootstrap hostname")
		}
		clusterDomain := strings.SplitN(host, ".", 2)[1]
		allows = func(candidate string) bool {
			return googleKafkaHost.MatchString(candidate) && strings.HasSuffix(candidate, "."+clusterDomain)
		}
	case "azure-iam":
		if !eventHubsHost.MatchString(host) {
			return "", nil, errors.New("azure-iam requires an Azure Event Hubs namespace hostname")
		}
		allows = func(candidate string) bool { return candidate == host }
	default:
		return "", nil, errors.New("unsupported cloud IAM mechanism")
	}
	for _, seed := range c.Seeds {
		if !allows(brokerHostname(seed)) {
			return "", nil, errors.New("all seeds must belong to the same cloud IAM endpoint")
		}
	}
	return host, allows, nil
}

func newCloudIAM(c Config) (sasl.Mechanism, error) {
	host, allows, err := cloudBrokerBoundary(c)
	if err != nil {
		return nil, err
	}
	var source func(context.Context) (oauth.Auth, error)
	if c.SASL == "gcp-iam" {
		principal := strings.TrimSpace(c.GCPPrincipal)
		if principal == "" {
			principal = strings.TrimSpace(os.Getenv("GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL"))
		}
		if principal == "" {
			return nil, errors.New("gcp-iam requires gcpPrincipal or GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL")
		}
		creds, err := credentials.DetectDefault(&credentials.DetectOptions{
			Scopes:              []string{"https://www.googleapis.com/auth/cloud-platform"},
			Client:              &http.Client{Timeout: 5 * time.Second},
			DisableAsyncRefresh: true,
		})
		if err != nil {
			return nil, errors.New("could not initialize Google application default credentials")
		}
		source = func(ctx context.Context) (oauth.Auth, error) {
			token, err := creds.Token(ctx)
			if err != nil {
				return oauth.Auth{}, errors.New("could not acquire Google application default access token")
			}
			return googleKafkaBearer(token, principal, time.Now())
		}
	} else {
		creds, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, errors.New("could not initialize Azure default credentials")
		}
		scopes := []string{"https://" + host + "/.default"}
		source = func(ctx context.Context) (oauth.Auth, error) {
			token, err := creds.GetToken(ctx, policy.TokenRequestOptions{Scopes: scopes})
			if err != nil || token.Token == "" || !token.ExpiresOn.After(time.Now()) {
				return oauth.Auth{}, errors.New("could not acquire a valid Azure Event Hubs access token")
			}
			return oauth.Auth{Token: token.Token}, nil
		}
	}
	return cloudIAMMechanism{Mechanism: oauth.Oauth(source), allows: allows}, nil
}

// Google Managed Kafka wraps the opaque ADC access token in this
// service-specific three-segment format for SASL/OAUTHBEARER.
func googleKafkaBearer(token *auth.Token, principal string, now time.Time) (oauth.Auth, error) {
	if token == nil || token.Value == "" || !token.Expiry.After(now) {
		return oauth.Auth{}, errors.New("Google application default credentials returned an invalid access token")
	}
	claims, err := json.Marshal(struct {
		Issuer   string `json:"iss"`
		Subject  string `json:"sub"`
		IssuedAt int64  `json:"iat"`
		Expiry   int64  `json:"exp"`
	}{"Google", principal, now.Unix(), token.Expiry.Unix()})
	if err != nil {
		return oauth.Auth{}, err
	}
	encode := base64.RawURLEncoding.EncodeToString
	header := encode([]byte(`{"typ":"JWT","alg":"GOOG_OAUTH2_TOKEN"}`))
	return oauth.Auth{Token: header + "." + encode(claims) + "." + encode([]byte(token.Value))}, nil
}
