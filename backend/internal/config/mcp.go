package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// MCPRuntime is the runtime.mcp YAML block. Integer limits are pointers so an
// explicit 0 is rejected instead of silently taking the default.
type MCPRuntime struct {
	Enabled             bool     `yaml:"enabled"`
	Writes              string   `yaml:"writes"`
	AllowedOrigins      []string `yaml:"allowedOrigins"`
	MaxResponseBytes    *int     `yaml:"maxResponseBytes"`
	CallTimeoutSeconds  *int     `yaml:"callTimeoutSeconds"`
	RateLimitPerMinute  *int     `yaml:"rateLimitPerMinute"`
	ConcurrencyPerToken *int     `yaml:"concurrencyPerToken"`
}

// MCP is the resolved, validated MCP server configuration.
type MCP struct {
	Enabled bool
	// Writes is true for mcp.writes: enabled.
	Writes bool
	// AllowedOrigins holds normalized scheme://host[:port] browser origins.
	AllowedOrigins      []string
	MaxResponseBytes    int
	CallTimeout         time.Duration
	RateLimitPerMinute  int
	ConcurrencyPerToken int
}

const (
	defaultMCPMaxResponseBytes    = 256 << 10
	defaultMCPCallTimeoutSeconds  = 30
	defaultMCPRateLimitPerMinute  = 120
	defaultMCPConcurrencyPerToken = 4
)

// resolveMCP applies KAFLUX_MCP_* overrides and validates runtime.mcp. It runs
// whether or not MCP is enabled, so a typo fails at startup, not on enabling.
func resolveMCP(r MCPRuntime) (MCP, error) {
	m := MCP{Enabled: r.Enabled}
	if v, ok := os.LookupEnv("KAFLUX_MCP_ENABLED"); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return m, fmt.Errorf("KAFLUX_MCP_ENABLED must be a boolean")
		}
		m.Enabled = b
	}
	switch writes := override("KAFLUX_MCP_WRITES", r.Writes); writes {
	case "", "disabled":
	case "enabled":
		m.Writes = true
	default:
		return m, fmt.Errorf("mcp.writes (KAFLUX_MCP_WRITES) must be \"disabled\" or \"enabled\", got %q", writes)
	}
	origins := r.AllowedOrigins
	if v, ok := os.LookupEnv("KAFLUX_MCP_ALLOWED_ORIGINS"); ok {
		origins = nil
		for _, o := range strings.Split(v, ",") {
			if o = strings.TrimSpace(o); o != "" {
				origins = append(origins, o)
			}
		}
	}
	for _, o := range origins {
		n, err := normalizeOrigin(o)
		if err != nil {
			return m, fmt.Errorf("mcp.allowedOrigins (KAFLUX_MCP_ALLOWED_ORIGINS): %q %v", o, err)
		}
		m.AllowedOrigins = append(m.AllowedOrigins, n)
	}
	var timeout int
	for _, l := range []struct {
		yaml, env     string
		value         *int
		def, min, max int
		dest          *int
	}{
		{"maxResponseBytes", "KAFLUX_MCP_MAX_RESPONSE_BYTES", r.MaxResponseBytes, defaultMCPMaxResponseBytes, 4096, 16 << 20, &m.MaxResponseBytes},
		{"callTimeoutSeconds", "KAFLUX_MCP_CALL_TIMEOUT_SECONDS", r.CallTimeoutSeconds, defaultMCPCallTimeoutSeconds, 1, 300, &timeout},
		{"rateLimitPerMinute", "KAFLUX_MCP_RATE_LIMIT_PER_MINUTE", r.RateLimitPerMinute, defaultMCPRateLimitPerMinute, 1, 6000, &m.RateLimitPerMinute},
		{"concurrencyPerToken", "KAFLUX_MCP_CONCURRENCY_PER_TOKEN", r.ConcurrencyPerToken, defaultMCPConcurrencyPerToken, 1, 64, &m.ConcurrencyPerToken},
	} {
		n := l.def
		if l.value != nil {
			n = *l.value
		}
		if v, ok := os.LookupEnv(l.env); ok {
			parsed, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil {
				return m, fmt.Errorf("%s must be an integer", l.env)
			}
			n = parsed
		}
		if n < l.min || n > l.max {
			return m, fmt.Errorf("mcp.%s (%s) must be between %d and %d, got %d", l.yaml, l.env, l.min, l.max, n)
		}
		*l.dest = n
	}
	m.CallTimeout = time.Duration(timeout) * time.Second
	return m, nil
}

// normalizeOrigin accepts an http(s) origin, scheme://host[:port] with at most
// a trailing slash, and returns it lowercased without the slash.
func normalizeOrigin(o string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(o))
	if err != nil {
		return "", fmt.Errorf("is not a URL")
	}
	scheme := strings.ToLower(u.Scheme)
	switch {
	case scheme != "http" && scheme != "https":
		return "", fmt.Errorf("must use http or https")
	case u.Hostname() == "" || u.Opaque != "":
		return "", fmt.Errorf("must include a host")
	case u.User != nil:
		return "", fmt.Errorf("must not include credentials")
	case (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "":
		return "", fmt.Errorf("must be an origin without a path, query or fragment")
	}
	return scheme + "://" + strings.ToLower(u.Host), nil
}
