package auth

import (
	"context"
	"strings"
	"testing"
)

func TestLDAPRejectsInsecureTransportAndEmptyPassword(t *testing.T) {
	for _, cfg := range []LDAPConfig{{URL: "ldap://example.invalid", BaseDN: "dc=example,dc=invalid", UserFilter: "(uid={username})"}, {URL: "ldaps://example.invalid", BaseDN: "dc=example,dc=invalid", UserFilter: "(uid={username})"}} {
		password := "secret"
		if strings.HasPrefix(cfg.URL, "ldaps:") {
			password = ""
		}
		if _, err := AuthenticateLDAP(context.Background(), cfg, "alice", password); err == nil {
			t.Fatal("insecure/anonymous LDAP accepted")
		}
	}
}

func TestLDAPFilterEscapesUntrustedUsername(t *testing.T) {
	filter, err := ldapUserFilter("(uid={username})", "alice*)(uid=*)")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(filter, "alice*)") || !strings.Contains(filter, `\2a`) {
		t.Fatalf("unsafe LDAP filter: %s", filter)
	}
	if _, err := ldapUserFilter("(uid=alice)", "alice"); err == nil {
		t.Fatal("filter without username placeholder accepted")
	}
}
