// Package integrations isolates optional REST services from Kafka administration.
package integrations

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

type Config struct {
	ID          string `yaml:"id"`
	ClusterID   string `yaml:"clusterID"`
	Kind        string `yaml:"kind"`
	URL         string `yaml:"url"`
	Username    string `yaml:"username"`
	PasswordEnv string `yaml:"passwordEnv"`
	TokenEnv    string `yaml:"tokenEnv"`
}
type cached struct {
	body    []byte
	expires time.Time
}
type Client struct {
	config     Config
	http       *http.Client
	slots      chan struct{}
	mu         sync.Mutex
	cache      map[string]cached
	failures   int
	retryAfter time.Time
}
type UpstreamError struct{ Status int }

func (e *UpstreamError) Error() string { return fmt.Sprintf("Integration returned HTTP %d", e.Status) }
func New(c Config) (*Client, error) {
	u, e := url.Parse(c.URL)
	if e != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("integration URL must be an HTTP(S) base URL without credentials, query or fragment")
	}
	if c.ID == "" || (c.Kind != "schemas" && c.Kind != "connect") {
		return nil, errors.New("integration requires an ID and schemas/connect kind")
	}
	for _, ref := range []string{c.PasswordEnv, c.TokenEnv} {
		if ref != "" && os.Getenv(ref) == "" {
			return nil, errors.New("integration credential environment reference is unset")
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost = 4
	transport.MaxIdleConnsPerHost = 4
	return &Client{config: c, http: &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("integration redirects disabled") }}, slots: make(chan struct{}, 4), cache: map[string]cached{}}, nil
}
func (c *Client) Kind() string      { return c.config.Kind }
func (c *Client) ClusterID() string { return c.config.ClusterID }
func (c *Client) Close()            { c.http.CloseIdleConnections() }
func (c *Client) Do(ctx context.Context, method, path string, payload []byte) ([]byte, error) {
	if !strings.HasPrefix(path, "/") || strings.Contains(path, "..") || len(payload) > 1<<20 {
		return nil, errors.New("invalid integration request")
	}
	c.mu.Lock()
	if method == "GET" {
		if entry, ok := c.cache[path]; ok && time.Now().Before(entry.expires) {
			body := append([]byte(nil), entry.body...)
			c.mu.Unlock()
			return body, nil
		}
	}
	if time.Now().Before(c.retryAfter) {
		c.mu.Unlock()
		return nil, errors.New("integration temporarily unavailable; retry later")
	}
	c.mu.Unlock()
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	case <-bounded.Done():
		return nil, bounded.Err()
	}
	request, e := http.NewRequestWithContext(bounded, method, strings.TrimRight(c.config.URL, "/")+path, bytes.NewReader(payload))
	if e != nil {
		return nil, errors.New("invalid integration request")
	}
	request.Header.Set("Accept", "application/json")
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.config.PasswordEnv != "" {
		request.SetBasicAuth(c.config.Username, os.Getenv(c.config.PasswordEnv))
	}
	if c.config.TokenEnv != "" {
		request.Header.Set("Authorization", "Bearer "+os.Getenv(c.config.TokenEnv))
	}
	response, e := c.http.Do(request)
	if e != nil {
		c.failure()
		return nil, errors.New("integration request failed or timed out")
	}
	defer response.Body.Close()
	body, e := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if e != nil || len(body) > 1<<20 {
		c.failure()
		return nil, errors.New("integration response exceeded limit or could not be read")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if response.StatusCode >= 500 || response.StatusCode == 429 {
			c.failure()
		}
		return nil, &UpstreamError{Status: response.StatusCode}
	}
	if len(body) == 0 {
		body = []byte(`{}`)
	}
	if !json.Valid(body) {
		return nil, errors.New("integration returned invalid JSON")
	}
	c.mu.Lock()
	c.failures = 0
	c.retryAfter = time.Time{}
	if method == "GET" {
		if len(c.cache) >= 16 {
			c.cache = map[string]cached{}
		}
		c.cache[path] = cached{body: append([]byte(nil), body...), expires: time.Now().Add(5 * time.Second)}
	} else {
		c.cache = map[string]cached{}
	}
	c.mu.Unlock()
	return body, nil
}
func (c *Client) failure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures++
	if c.failures >= 3 {
		c.retryAfter = time.Now().Add(time.Second * time.Duration(1<<min(c.failures-3, 5)))
	}
}

var embeddedSecret = regexp.MustCompile(`(?i)(://[^\s/@]+:[^\s/@]+@|[?;&](password|passwd|token|secret|api[_-]?key|credential|access[_-]?key)=|authorization\s*[:=]\s*(basic|bearer)|-----BEGIN (RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----)`)

func Redact(raw []byte) ([]byte, error) {
	var value any
	if e := json.Unmarshal(raw, &value); e != nil {
		return nil, e
	}
	var visit func(any)
	visit = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			for key, child := range x {
				lower := strings.ToLower(key)
				text, _ := child.(string)
				if strings.Contains(lower, "password") || strings.Contains(lower, "secret") || strings.Contains(lower, "token") || strings.Contains(lower, "credential") || strings.Contains(lower, "api.key") || strings.Contains(lower, "jaas") || strings.Contains(lower, "private.key") || strings.Contains(lower, "basic.auth.user.info") || strings.Contains(lower, "authorization") || strings.Contains(lower, "ssl.keystore.key") || lower == "ssl.key" || embeddedSecret.MatchString(text) {
					x[key] = "[REDACTED]"
				} else {
					visit(child)
				}
			}
		case []any:
			for _, child := range x {
				visit(child)
			}
		}
	}
	visit(value)
	return json.Marshal(value)
}
