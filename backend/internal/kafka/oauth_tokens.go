package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type oauthTokens struct {
	config                      Config
	client                      *http.Client
	gate                        chan struct{}
	token                       string
	expires, refreshAt, retryAt time.Time
	failures                    int
}

func newOAuthTokens(c Config) (*oauthTokens, error) {
	u, err := url.Parse(c.OAuthTokenEndpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("OAuth token endpoint must be a fixed HTTPS URL without credentials, query or fragment")
	}
	if c.OAuthClientID == "" || c.OAuthClientSecretEnv == "" || os.Getenv(c.OAuthClientSecretEnv) == "" || c.OAuthTokenEnv != "" {
		return nil, errors.New("OAuth client credentials require a client ID and secret environment reference, without static token mode")
	}
	if len(c.OAuthClientID) > 1024 || len(c.OAuthScopes) > 64 {
		return nil, errors.New("OAuth configuration exceeds limits")
	}
	for _, scope := range c.OAuthScopes {
		if len(scope) > 256 || strings.ContainsAny(scope, " \t\n\r") {
			return nil, errors.New("invalid OAuth scope")
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost = 1
	transport.MaxIdleConnsPerHost = 1
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if c.OAuthCAFile != "" {
		pem, err := os.ReadFile(c.OAuthCAFile)
		if err != nil {
			return nil, errors.New("OAuth CA file could not be read")
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("OAuth CA file contains no certificates")
		}
		transport.TLSClientConfig.RootCAs = roots
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("OAuth redirects disabled") }}
	return &oauthTokens{config: c, client: client, gate: make(chan struct{}, 1)}, nil
}

func (s *oauthTokens) Close() { s.client.CloseIdleConnections() }

func (s *oauthTokens) Token(ctx context.Context) (string, error) {
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if bounded.Err() != nil {
		return "", bounded.Err()
	}
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-bounded.Done():
		return "", bounded.Err()
	}
	now := time.Now()
	if s.token != "" && now.Before(s.refreshAt) && now.Before(s.expires) {
		return s.token, nil
	}
	if now.Before(s.retryAt) {
		if s.token != "" && now.Before(s.expires) {
			return s.token, nil
		}
		return "", errors.New("OAuth token endpoint temporarily unavailable")
	}
	secret := os.Getenv(s.config.OAuthClientSecretEnv)
	if secret == "" {
		return "", errors.New("OAuth client credential is unavailable")
	}
	form := url.Values{"grant_type": {"client_credentials"}}
	if len(s.config.OAuthScopes) > 0 {
		form.Set("scope", strings.Join(s.config.OAuthScopes, " "))
	}
	request, err := http.NewRequestWithContext(bounded, "POST", s.config.OAuthTokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", errors.New("invalid OAuth token request")
	}
	request.SetBasicAuth(url.QueryEscape(s.config.OAuthClientID), url.QueryEscape(secret))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "application/json")
	response, err := s.client.Do(request)
	var body []byte
	if err == nil {
		defer response.Body.Close()
		if response.StatusCode == 200 {
			body, err = io.ReadAll(io.LimitReader(response.Body, 65537))
			if len(body) > 65536 {
				err = errors.New("oversized response")
			}
		} else {
			err = errors.New("token request rejected")
		}
	}
	var token struct {
		AccessToken string      `json:"access_token"`
		Type        string      `json:"token_type"`
		ExpiresIn   json.Number `json:"expires_in"`
	}
	if err == nil {
		err = json.Unmarshal(body, &token)
	}
	seconds, e := token.ExpiresIn.Int64()
	if err != nil || e != nil || seconds < 1 || seconds > 86400 || token.AccessToken == "" || len(token.AccessToken) > 16384 || strings.IndexFunc(token.AccessToken, func(r rune) bool { return r < 33 || r > 126 }) >= 0 || !strings.EqualFold(token.Type, "Bearer") {
		s.failures++
		delay := time.Second * time.Duration(1<<min(s.failures-1, 5))
		s.retryAt = time.Now().Add(delay + time.Duration(rand.Int63n(int64(delay/5)+1)))
		if bounded.Err() != nil {
			return "", bounded.Err()
		}
		if s.token != "" && time.Now().Before(s.expires) {
			return s.token, nil
		}
		return "", errors.New("OAuth token endpoint failed or returned an invalid bounded response")
	}
	lifetime := time.Duration(seconds) * time.Second
	s.token = token.AccessToken
	s.expires = time.Now().Add(lifetime)
	margin := min(30*time.Second, lifetime/10)
	s.refreshAt = s.expires.Add(-margin + time.Duration(rand.Int63n(int64(margin/5)+1)))
	s.failures = 0
	s.retryAt = time.Time{}
	return s.token, nil
}
