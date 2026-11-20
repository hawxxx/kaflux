package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func loadMCPYAML(t *testing.T, body string) (Config, error) {
	t.Helper()
	t.Setenv("KAFLUX_DEMO", "true")
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return Load(p)
}

func TestMCPDefaults(t *testing.T) {
	c, err := loadMCPYAML(t, "runtime:\n  demo: true\n")
	if err != nil {
		t.Fatal(err)
	}
	m := c.MCP
	if m.Enabled || m.Writes || len(m.AllowedOrigins) != 0 {
		t.Fatalf("MCP must be off by default: %+v", m)
	}
	if m.MaxResponseBytes != 262144 || m.CallTimeout != 30*time.Second || m.RateLimitPerMinute != 120 || m.ConcurrencyPerToken != 4 {
		t.Fatalf("defaults: %+v", m)
	}
}

func TestMCPYAML(t *testing.T) {
	c, err := loadMCPYAML(t, `runtime:
  mcp:
    enabled: true
    writes: enabled
    allowedOrigins: ["https://Kaflux.Example.com/", "http://localhost:5173"]
    maxResponseBytes: 4096
    callTimeoutSeconds: 300
    rateLimitPerMinute: 6000
    concurrencyPerToken: 64
`)
	if err != nil {
		t.Fatal(err)
	}
	m := c.MCP
	if !m.Enabled || !m.Writes || m.MaxResponseBytes != 4096 || m.CallTimeout != 300*time.Second || m.RateLimitPerMinute != 6000 || m.ConcurrencyPerToken != 64 {
		t.Fatalf("yaml: %+v", m)
	}
	if want := []string{"https://kaflux.example.com", "http://localhost:5173"}; !slices.Equal(m.AllowedOrigins, want) {
		t.Fatalf("origins %q, want %q", m.AllowedOrigins, want)
	}
}

func TestMCPEnvironmentOverride(t *testing.T) {
	t.Setenv("KAFLUX_MCP_ENABLED", "true")
	t.Setenv("KAFLUX_MCP_WRITES", "enabled")
	t.Setenv("KAFLUX_MCP_ALLOWED_ORIGINS", " https://a.example.com , ,http://b.example.com:8080")
	t.Setenv("KAFLUX_MCP_MAX_RESPONSE_BYTES", "65536")
	t.Setenv("KAFLUX_MCP_CALL_TIMEOUT_SECONDS", "10")
	t.Setenv("KAFLUX_MCP_RATE_LIMIT_PER_MINUTE", "60")
	t.Setenv("KAFLUX_MCP_CONCURRENCY_PER_TOKEN", "2")
	c, err := loadMCPYAML(t, "runtime:\n  mcp:\n    enabled: false\n    writes: disabled\n    allowedOrigins: [https://yaml.example.com]\n    rateLimitPerMinute: 5\n")
	if err != nil {
		t.Fatal(err)
	}
	m := c.MCP
	if !m.Enabled || !m.Writes || m.MaxResponseBytes != 65536 || m.CallTimeout != 10*time.Second || m.RateLimitPerMinute != 60 || m.ConcurrencyPerToken != 2 {
		t.Fatalf("env: %+v", m)
	}
	if want := []string{"https://a.example.com", "http://b.example.com:8080"}; !slices.Equal(m.AllowedOrigins, want) {
		t.Fatalf("origins %q, want %q", m.AllowedOrigins, want)
	}
	// An empty environment list clears the YAML origins.
	t.Setenv("KAFLUX_MCP_ALLOWED_ORIGINS", "")
	c, err = loadMCPYAML(t, "runtime:\n  mcp:\n    allowedOrigins: [https://yaml.example.com]\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.MCP.AllowedOrigins) != 0 {
		t.Fatalf("empty env kept origins %q", c.MCP.AllowedOrigins)
	}
}

func TestMCPValidationFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		yaml, env, value, want string
	}{
		"writes yaml":             {yaml: "writes: yes", want: "mcp.writes"},
		"writes env":              {env: "KAFLUX_MCP_WRITES", value: "true", want: "mcp.writes"},
		"enabled env":             {env: "KAFLUX_MCP_ENABLED", value: "maybe", want: "KAFLUX_MCP_ENABLED"},
		"origin path":             {yaml: "allowedOrigins: [https://a.example.com/app]", want: "mcp.allowedOrigins"},
		"origin scheme":           {yaml: "allowedOrigins: [ftp://a.example.com]", want: "mcp.allowedOrigins"},
		"origin wildcard":         {yaml: "allowedOrigins: ['*']", want: "mcp.allowedOrigins"},
		"origin null":             {yaml: "allowedOrigins: ['null']", want: "mcp.allowedOrigins"},
		"origin query":            {yaml: "allowedOrigins: ['https://a.example.com?x=1']", want: "mcp.allowedOrigins"},
		"origin credentials":      {yaml: "allowedOrigins: ['https://u:p@a.example.com']", want: "mcp.allowedOrigins"},
		"origin env":              {env: "KAFLUX_MCP_ALLOWED_ORIGINS", value: "https://ok.example.com,a.example.com", want: "mcp.allowedOrigins"},
		"max bytes low":           {yaml: "maxResponseBytes: 4095", want: "mcp.maxResponseBytes"},
		"max bytes zero":          {yaml: "maxResponseBytes: 0", want: "mcp.maxResponseBytes"},
		"max bytes high":          {yaml: "maxResponseBytes: 16777217", want: "mcp.maxResponseBytes"},
		"max bytes env":           {env: "KAFLUX_MCP_MAX_RESPONSE_BYTES", value: "big", want: "KAFLUX_MCP_MAX_RESPONSE_BYTES"},
		"timeout zero":            {yaml: "callTimeoutSeconds: 0", want: "mcp.callTimeoutSeconds"},
		"timeout high":            {yaml: "callTimeoutSeconds: 301", want: "mcp.callTimeoutSeconds"},
		"timeout env":             {env: "KAFLUX_MCP_CALL_TIMEOUT_SECONDS", value: "30s", want: "KAFLUX_MCP_CALL_TIMEOUT_SECONDS"},
		"rate zero":               {yaml: "rateLimitPerMinute: 0", want: "mcp.rateLimitPerMinute"},
		"rate high":               {yaml: "rateLimitPerMinute: 6001", want: "mcp.rateLimitPerMinute"},
		"rate env":                {env: "KAFLUX_MCP_RATE_LIMIT_PER_MINUTE", value: "", want: "KAFLUX_MCP_RATE_LIMIT_PER_MINUTE"},
		"concurrency zero":        {yaml: "concurrencyPerToken: 0", want: "mcp.concurrencyPerToken"},
		"concurrency high":        {yaml: "concurrencyPerToken: 65", want: "mcp.concurrencyPerToken"},
		"concurrency env":         {env: "KAFLUX_MCP_CONCURRENCY_PER_TOKEN", value: "-1", want: "mcp.concurrencyPerToken"},
		"unknown key":             {yaml: "confirmationSecretEnv: X", want: "confirmationSecretEnv"},
		"validated when disabled": {yaml: "enabled: false\n    writes: on", want: "mcp.writes"},
	} {
		t.Run(name, func(t *testing.T) {
			if tc.env != "" {
				t.Setenv(tc.env, tc.value)
			}
			body := "runtime:\n  demo: true\n"
			if tc.yaml != "" {
				body += "  mcp:\n    " + tc.yaml + "\n"
			}
			_, err := loadMCPYAML(t, body)
			if err == nil {
				t.Fatal("invalid MCP configuration accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name %q", err, tc.want)
			}
		})
	}
}
