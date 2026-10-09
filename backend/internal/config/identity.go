package config

import (
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/hawxxx/kaflux/backend/internal/auth"
)

var providerID = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// validateIdentity rejects grant and identity provider settings that would
// otherwise fail silently at login or deny every request. Omitted grant
// cluster and pattern mean "*". Roles that providers assign but no grant
// mentions are logged, because those users can sign in yet see nothing.
func validateIdentity(c *Config) error {
	granted := map[string]bool{}
	for i := range c.Grants {
		g := &c.Grants[i]
		if g.Role == "" || g.Action == "" {
			return fmt.Errorf("grant %d: role and action are required", i+1)
		}
		if g.Action != "*" && !slices.Contains(auth.Actions, g.Action) {
			return fmt.Errorf("grant %d: unknown action %q (use * or one of %s)", i+1, g.Action, strings.Join(auth.Actions, ", "))
		}
		if g.Cluster == "" {
			g.Cluster = "*"
		}
		if g.Pattern == "" {
			g.Pattern = "*"
		}
		if g.Regex {
			if len(g.Pattern) > 512 {
				return fmt.Errorf("grant %d: regex pattern exceeds 512 characters", i+1)
			}
			if _, err := regexp.Compile(g.Pattern); err != nil {
				return fmt.Errorf("grant %d: invalid regex pattern", i+1)
			}
		}
		granted[g.Role] = true
	}
	if len(c.Grants) == 0 {
		// The API grants administrators everything when no grants are configured.
		granted["administrator"] = true
	}
	ids := map[string]bool{}
	assigned := map[string]string{}
	assign := func(provider string, mapping map[string][]string, defaults []string) {
		for _, roles := range append(slices.Collect(maps.Values(mapping)), defaults) {
			for _, role := range roles {
				assigned[role] = provider
			}
		}
	}
	for i := range c.OIDC {
		p := &c.OIDC[i]
		if p.ID == "" {
			p.ID = "oidc"
		}
		if !providerID.MatchString(p.ID) || ids[p.ID] {
			return fmt.Errorf("oidc %q: id must be unique, lowercase letters, digits, - or _", p.ID)
		}
		ids[p.ID] = true
		if p.Issuer == "" || p.ClientID == "" || p.RedirectURL == "" {
			return fmt.Errorf("oidc %q: issuer, clientId and redirectURL are required", p.ID)
		}
		redirect, err := url.Parse(p.RedirectURL)
		if callback := "/api/v1/auth/oidc/" + p.ID + "/callback"; err != nil || redirect.Path != callback {
			return fmt.Errorf("oidc %q: redirectURL path must be %s", p.ID, callback)
		}
		assign("oidc "+p.ID, p.GroupRoles, p.DefaultRoles)
	}
	for i := range c.LDAP {
		p := &c.LDAP[i]
		if p.ID == "" {
			p.ID = "ldap"
		}
		if !providerID.MatchString(p.ID) || ids[p.ID] {
			return fmt.Errorf("ldap %q: id must be unique, lowercase letters, digits, - or _", p.ID)
		}
		ids[p.ID] = true
		endpoint, err := url.Parse(p.URL)
		if err != nil || endpoint.Host == "" || (endpoint.Scheme != "ldaps" && !(endpoint.Scheme == "ldap" && p.StartTLS)) {
			return fmt.Errorf("ldap %q: url must use ldaps:// or ldap:// with startTLS", p.ID)
		}
		if p.BaseDN == "" || strings.Count(p.UserFilter, "{username}") != 1 {
			return fmt.Errorf("ldap %q: baseDN and a userFilter containing {username} once are required", p.ID)
		}
		if p.BindDN != "" && (p.BindPasswordEnv == "" || os.Getenv(p.BindPasswordEnv) == "") {
			return fmt.Errorf("ldap %q: bindDN requires bindPasswordEnv naming a set environment variable", p.ID)
		}
		assign("ldap "+p.ID, p.GroupRoles, p.DefaultRoles)
	}
	if c.AdminPasswordHash != "" {
		assigned["administrator"] = "local account"
	}
	for role, provider := range assigned {
		if !granted[role] {
			slog.Warn("identity provider assigns a role without grants; its users will be denied", "provider", provider, "role", role)
		}
	}
	return nil
}
