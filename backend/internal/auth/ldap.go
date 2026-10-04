package auth

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"
)

type LDAPConfig struct {
	ID              string              `yaml:"id"`
	URL             string              `yaml:"url"`
	StartTLS        bool                `yaml:"startTLS"`
	BindDN          string              `yaml:"bindDN"`
	BindPasswordEnv string              `yaml:"bindPasswordEnv"`
	BaseDN          string              `yaml:"baseDN"`
	UserFilter      string              `yaml:"userFilter"`
	GroupAttribute  string              `yaml:"groupAttribute"`
	GroupRoles      map[string][]string `yaml:"groupRoles"`
	DefaultRoles    []string            `yaml:"defaultRoles"`
}

func ldapUserFilter(template, username string) (string, error) {
	if strings.Count(template, "{username}") != 1 || username == "" || len(username) > 256 {
		return "", fmt.Errorf("invalid LDAP user filter or username")
	}
	return strings.Replace(template, "{username}", ldap.EscapeFilter(username), 1), nil
}

func AuthenticateLDAP(ctx context.Context, cfg LDAPConfig, username, password string) (User, error) {
	if password == "" || len(password) > 4096 {
		return User{}, fmt.Errorf("LDAP requires non-empty credentials")
	}
	endpoint, err := url.Parse(cfg.URL)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "ldaps" && !(endpoint.Scheme == "ldap" && cfg.StartTLS)) {
		return User{}, fmt.Errorf("LDAP requires LDAPS or StartTLS")
	}
	if cfg.BaseDN == "" {
		return User{}, fmt.Errorf("LDAP base DN is required")
	}
	filter, err := ldapUserFilter(cfg.UserFilter, username)
	if err != nil {
		return User{}, err
	}
	if err := ctx.Err(); err != nil {
		return User{}, err
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: endpoint.Hostname()}
	conn, err := ldap.DialURL(cfg.URL, ldap.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}), ldap.DialWithTLSConfig(tlsConfig))
	if err != nil {
		return User{}, fmt.Errorf("LDAP connection failed")
	}
	defer conn.Close()
	stopCancellation := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopCancellation()
	conn.SetTimeout(5 * time.Second)
	if cfg.StartTLS && endpoint.Scheme == "ldap" {
		if err := conn.StartTLS(tlsConfig); err != nil {
			return User{}, fmt.Errorf("LDAP TLS negotiation failed")
		}
	}
	if cfg.BindDN != "" {
		secret := os.Getenv(cfg.BindPasswordEnv)
		if secret == "" {
			return User{}, fmt.Errorf("LDAP bind password reference is unset")
		}
		if err := conn.Bind(cfg.BindDN, secret); err != nil {
			return User{}, fmt.Errorf("LDAP service bind failed")
		}
	}
	groupAttribute := cfg.GroupAttribute
	if groupAttribute == "" {
		groupAttribute = "memberOf"
	}
	result, err := conn.Search(ldap.NewSearchRequest(cfg.BaseDN, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 2, 5, false, filter, []string{groupAttribute, "uid", "sAMAccountName"}, nil))
	if err != nil || len(result.Entries) != 1 {
		return User{}, fmt.Errorf("LDAP identity lookup failed")
	}
	entry := result.Entries[0]
	if err := conn.Bind(entry.DN, password); err != nil {
		return User{}, fmt.Errorf("LDAP credentials rejected")
	}
	if err := ctx.Err(); err != nil {
		return User{}, err
	}
	roles := mappedRoles(entry.GetAttributeValues(groupAttribute), cfg.GroupRoles, cfg.DefaultRoles)
	if len(roles) == 0 {
		return User{}, fmt.Errorf("LDAP identity has no assigned roles")
	}
	provider := cfg.ID
	if provider == "" {
		provider = "ldap"
	}
	return User{ID: provider + ":" + entry.DN, Username: username, Provider: provider, Roles: roles}, nil
}
