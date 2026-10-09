package auth

import (
	"slices"
	"testing"
)

func TestLDAPGroupMappingIgnoresDNCaseAndSpacing(t *testing.T) {
	mapping := map[string][]string{"cn=kafka-viewers,ou=groups,dc=ldap,dc=goauthentik,dc=io": {"viewer"}}
	roles := ldapGroupRoles([]string{"CN=Kafka-Viewers, OU=groups, DC=ldap, DC=goauthentik, DC=io"}, mapping, nil)
	if !slices.Equal(roles, []string{"viewer"}) {
		t.Fatalf("roles %v", roles)
	}
	if roles := ldapGroupRoles([]string{"cn=other,dc=io"}, mapping, nil); len(roles) != 0 {
		t.Fatalf("unrelated group mapped: %v", roles)
	}
}

func TestPermissionsReflectGrantsPerCluster(t *testing.T) {
	a := Authorizer{Grants: []Grant{
		{Role: "viewer", Cluster: "*", Action: "read", Pattern: "*"},
		{Role: "viewer", Cluster: "dev", Action: "produce", Pattern: "test.*"},
	}}
	u := User{Roles: []string{"viewer"}}
	if got := a.Permissions(u, "prod"); !slices.Equal(got, []string{"read"}) {
		t.Fatalf("prod permissions %v", got)
	}
	if got := a.Permissions(u, "dev"); !slices.Equal(got, []string{"read", "produce"}) {
		t.Fatalf("dev permissions %v", got)
	}
	admin := Authorizer{Grants: []Grant{{Role: "administrator", Cluster: "*", Action: "*", Pattern: "*"}}}
	if got := admin.Permissions(User{Roles: []string{"administrator"}}, "prod"); len(got) != len(Actions) {
		t.Fatalf("administrator permissions %v", got)
	}
}
