package config

import (
	"os"
	"strings"
	"testing"
)

const identityHead = "runtime:\n  demo: true\n"

func TestGrantDefaultsAndValidation(t *testing.T) {
	c, err := loadYAML(t, identityHead+"grants:\n  - role: viewer\n    action: read\n")
	if err != nil {
		t.Fatal(err)
	}
	if g := c.Grants[0]; g.Cluster != "*" || g.Pattern != "*" {
		t.Fatalf("omitted cluster/pattern must mean *: %+v", g)
	}
	for input, want := range map[string]string{
		"grants:\n  - role: viewer\n    action: reads\n":                                   "unknown action",
		"grants:\n  - action: read\n":                                                      "role and action",
		"grants:\n  - role: viewer\n    action: read\n    regex: true\n    pattern: '('\n": "invalid regex",
	} {
		if _, err := loadYAML(t, identityHead+input); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%q: got %v, want %q", input, err, want)
		}
	}
}

func TestIdentityProviderValidation(t *testing.T) {
	t.Setenv("TEST_LDAP_BIND", "secret")
	valid := "oidc:\n  - id: authentik\n    issuer: https://id.example.invalid/application/o/kaflux/\n    clientId: kaflux\n    redirectURL: https://kaflux.example.invalid/api/v1/auth/oidc/authentik/callback\n" +
		"ldap:\n  - id: directory\n    url: ldaps://dir.example.invalid\n    bindDN: cn=svc\n    bindPasswordEnv: TEST_LDAP_BIND\n    baseDN: dc=example,dc=invalid\n    userFilter: '(cn={username})'\n"
	if _, err := loadYAML(t, identityHead+valid); err != nil {
		t.Fatal(err)
	}
	for name, input := range map[string]string{
		"redirect path mismatch": strings.Replace(valid, "oidc/authentik/callback", "oidc/kaflux/callback", 1),
		"duplicate id":           strings.Replace(valid, "id: directory", "id: authentik", 1),
		"insecure ldap":          strings.Replace(valid, "ldaps://", "ldap://", 1),
		"missing placeholder":    strings.Replace(valid, "{username}", "alice", 1),
		"unset bind password":    strings.Replace(valid, "TEST_LDAP_BIND", "NEVER_SET_KAFLUX_LDAP", 1),
		"missing client":         strings.Replace(valid, "clientId: kaflux", "clientId: ''", 1),
	} {
		if _, err := loadYAML(t, identityHead+input); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

// The shipped example, with its identity providers enabled, must pass validation.
func TestExampleIdentityConfigurationIsValid(t *testing.T) {
	raw, err := os.ReadFile("../../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	example := string(raw)
	start := strings.Index(example, "# oidc:")
	if start < 0 {
		t.Fatal("example has no OIDC block")
	}
	// The identity examples close the file; uncomment them.
	lines := strings.Split(example[start:], "\n")
	for i, line := range lines {
		lines[i] = strings.TrimPrefix(line, "# ")
	}
	example = example[:start] + strings.Join(lines, "\n")
	for _, name := range []string{"KAFLUX_ADMIN_USER", "KAFLUX_ADMIN_PASSWORD_HASH", "KAFLUX_PROMETHEUS_URL", "KAFLUX_LDAP_BIND_PASSWORD"} {
		t.Setenv(name, "configured")
	}
	c, err := loadYAML(t, example)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.OIDC) != 1 || len(c.LDAP) != 1 || len(c.Grants) == 0 {
		t.Fatalf("example identity not loaded: %d oidc, %d ldap", len(c.OIDC), len(c.LDAP))
	}
}
