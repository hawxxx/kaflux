// Package config loads strict YAML configuration and resolves environment overrides.
package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/capacity"
	"github.com/hawxxx/kaflux/backend/internal/integrations"
	"gopkg.in/yaml.v3"
)

type Cluster struct {
	ID                   string           `yaml:"id"`
	Name                 string           `yaml:"name"`
	Environment          string           `yaml:"environment"`
	Seeds                []string         `yaml:"seeds"`
	TLS                  bool             `yaml:"tls"`
	CAFile               string           `yaml:"caFile"`
	CertFile             string           `yaml:"certFile"`
	KeyFile              string           `yaml:"keyFile"`
	SASL                 string           `yaml:"sasl"`
	User                 string           `yaml:"user"`
	PasswordEnv          string           `yaml:"passwordEnv"`
	AllowPlaintext       bool             `yaml:"allowPlaintext"`
	Region               string           `yaml:"region"`
	RoleARN              string           `yaml:"roleArn"`
	MSKClusterARN        string           `yaml:"mskClusterArn"`
	OAuthTokenEnv        string           `yaml:"oauthTokenEnv"`
	OAuthTokenEndpoint   string           `yaml:"oauthTokenEndpoint"`
	OAuthClientID        string           `yaml:"oauthClientId"`
	OAuthClientSecretEnv string           `yaml:"oauthClientSecretEnv"`
	OAuthScopes          []string         `yaml:"oauthScopes"`
	OAuthCAFile          string           `yaml:"oauthCAFile"`
	Capacity             *capacity.Config `yaml:"capacity"`
}

type Runtime struct {
	StorageBackend        string `yaml:"storageBackend"`
	SQLitePath            string `yaml:"sqlitePath"`
	SessionLifetime       string `yaml:"sessionLifetime"`
	Listen                string `yaml:"listen"`
	StaticDirectory       string `yaml:"staticDirectory"`
	Demo                  bool   `yaml:"demo"`
	DatabaseURLEnv        string `yaml:"databaseURLEnv"`
	AdminUserEnv          string `yaml:"adminUserEnv"`
	AdminPasswordHashEnv  string `yaml:"adminPasswordHashEnv"`
	PrometheusURLEnv      string `yaml:"prometheusURLEnv"`
	ClustersFile          string `yaml:"clustersFile"`
	AmpURL                string `yaml:"ampURL"`
	CloudWatchRegion      string `yaml:"cloudWatchRegion"`
	AWSRoleARN            string `yaml:"awsRoleARN"`
	CloudWatchClusterName string `yaml:"cloudWatchClusterName"`
}

type Config struct {
	StorageBackend        string                `yaml:"-"`
	SQLitePath            string                `yaml:"-"`
	SessionLifetime       time.Duration         `yaml:"-"`
	Runtime               Runtime               `yaml:"runtime"`
	Clusters              []Cluster             `yaml:"clusters"`
	OIDC                  []auth.OIDCConfig     `yaml:"oidc"`
	LDAP                  []auth.LDAPConfig     `yaml:"ldap"`
	Grants                []Grant               `yaml:"grants"`
	Integrations          []integrations.Config `yaml:"integrations"`
	Listen                string                `yaml:"-"`
	StaticDirectory       string                `yaml:"-"`
	Demo                  bool                  `yaml:"-"`
	DatabaseURL           string                `yaml:"-"`
	AdminUser             string                `yaml:"-"`
	AdminPasswordHash     string                `yaml:"-"`
	PrometheusURL         string                `yaml:"-"`
	ClustersFile          string                `yaml:"-"`
	AmpURL                string                `yaml:"-"`
	CloudWatchRegion      string                `yaml:"-"`
	AWSRoleARN            string                `yaml:"-"`
	CloudWatchClusterName string                `yaml:"-"`
}

type Grant struct {
	Role    string `yaml:"role"`
	Cluster string `yaml:"cluster"`
	Action  string `yaml:"action"`
	Pattern string `yaml:"pattern"`
	Regex   bool   `yaml:"regex"`
}

// Load accepts a requested path or discovers config.yaml. A missing discovered
// file is allowed; a missing explicit path fails. Secrets never appear in errors.
func Load(path string) (Config, error) {
	var c Config
	explicit := path != ""
	if !explicit {
		path = "config.yaml"
	}
	raw, err := os.ReadFile(path)
	if err != nil && !(os.IsNotExist(err) && !explicit) {
		return c, fmt.Errorf("read configuration: %w", err)
	}
	if err == nil {
		d := yaml.NewDecoder(bytes.NewReader(raw))
		d.KnownFields(true)
		if err := d.Decode(&c); err != nil && err != io.EOF {
			return c, fmt.Errorf("invalid YAML configuration: %w", err)
		}
		var extra any
		if err := d.Decode(&extra); err != io.EOF {
			return c, fmt.Errorf("configuration requires a single YAML document")
		}
	}
	for _, cluster := range c.Clusters {
		if err := cluster.Capacity.Validate(); err != nil {
			return c, fmt.Errorf("cluster %q: %w", cluster.ID, err)
		}
	}
	c.SessionLifetime = auth.DefaultSessionLifetime
	lifetime := override("KAFLUX_SESSION_LIFETIME", c.Runtime.SessionLifetime)
	_, lifetimeEnv := os.LookupEnv("KAFLUX_SESSION_LIFETIME")
	if lifetime != "" || lifetimeEnv {
		c.SessionLifetime, err = time.ParseDuration(lifetime)
		if err != nil || c.SessionLifetime < 5*time.Minute || c.SessionLifetime > 24*time.Hour {
			return c, fmt.Errorf("session lifetime must be a duration between 5m and 24h")
		}
	}
	c.Listen = override("KAFLUX_LISTEN", c.Runtime.Listen)
	if v, ok := os.LookupEnv("KAFLUX_ADDR"); ok {
		c.Listen = v
	}
	if c.Listen == "" {
		c.Listen = "127.0.0.1:8080"
	}
	c.StaticDirectory = override("KAFLUX_STATIC_DIR", c.Runtime.StaticDirectory)
	c.ClustersFile = override("KAFLUX_CLUSTERS_FILE", c.Runtime.ClustersFile)
	c.AmpURL = override("KAFLUX_AMP_URL", c.Runtime.AmpURL)
	c.CloudWatchRegion = override("KAFLUX_CLOUDWATCH_REGION", c.Runtime.CloudWatchRegion)
	c.AWSRoleARN = override("KAFLUX_AWS_ROLE_ARN", c.Runtime.AWSRoleARN)
	c.CloudWatchClusterName = override("KAFLUX_CLOUDWATCH_CLUSTER_NAME", c.Runtime.CloudWatchClusterName)
	c.Demo = c.Runtime.Demo
	if value, ok := os.LookupEnv("KAFLUX_DEMO"); ok {
		c.Demo, err = strconv.ParseBool(value)
		if err != nil {
			return c, fmt.Errorf("KAFLUX_DEMO must be a boolean")
		}
	}
	for _, entry := range []struct {
		direct, ref string
		dest        *string
	}{
		{"KAFLUX_DATABASE_URL", c.Runtime.DatabaseURLEnv, &c.DatabaseURL},
		{"KAFLUX_ADMIN_USER", c.Runtime.AdminUserEnv, &c.AdminUser},
		{"KAFLUX_ADMIN_PASSWORD_HASH", c.Runtime.AdminPasswordHashEnv, &c.AdminPasswordHash},
		{"KAFLUX_PROMETHEUS_URL", c.Runtime.PrometheusURLEnv, &c.PrometheusURL},
	} {
		if value, ok := os.LookupEnv(entry.direct); ok {
			*entry.dest = value
			continue
		}
		if entry.ref != "" {
			value, ok := os.LookupEnv(entry.ref)
			if !ok || value == "" {
				return c, fmt.Errorf("required environment reference %s is unset", entry.ref)
			}
			*entry.dest = value
		}
	}
	c.StorageBackend = override("KAFLUX_STORAGE_BACKEND", c.Runtime.StorageBackend)
	c.SQLitePath = override("KAFLUX_SQLITE_PATH", c.Runtime.SQLitePath)
	if c.StorageBackend == "" {
		switch {
		case c.DatabaseURL != "":
			c.StorageBackend = "postgres"
		case c.Demo:
			c.StorageBackend = "memory"
		default:
			c.StorageBackend = "sqlite"
		}
	}
	switch c.StorageBackend {
	case "postgres":
		if c.DatabaseURL == "" {
			return c, fmt.Errorf("postgres storage requires KAFLUX_DATABASE_URL")
		}
	case "sqlite":
		if c.DatabaseURL != "" {
			return c, fmt.Errorf("sqlite storage conflicts with a PostgreSQL database URL")
		}
		if c.SQLitePath == "" {
			c.SQLitePath = "/var/lib/kaflux/kaflux.db"
		}
		if c.SQLitePath == ":memory:" || strings.HasPrefix(strings.ToLower(c.SQLitePath), "file:") || strings.TrimSpace(c.SQLitePath) == "" {
			return c, fmt.Errorf("sqlite storage requires a filesystem path, not an in-memory database or URI")
		}
	case "memory":
		if !c.Demo || c.DatabaseURL != "" {
			return c, fmt.Errorf("memory storage requires demo mode and no database URL")
		}
	default:
		return c, fmt.Errorf("storage backend must be sqlite, postgres, or memory")
	}
	return c, nil
}

func override(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}
