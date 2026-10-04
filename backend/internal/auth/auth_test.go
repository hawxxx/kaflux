package auth

import "testing"

func TestDefaultDenyAndScopes(t *testing.T) {
	a := Authorizer{Grants: []Grant{{Role: "viewer", Cluster: "prod", Action: "read", Pattern: "orders-*"}}}
	if !a.Allowed(User{Roles: []string{"viewer"}}, "prod", "read", "orders-v1") {
		t.Fatal("grant denied")
	}
	for _, action := range []string{"execute", "consume", "rollback"} {
		if a.Allowed(User{Roles: []string{"viewer"}}, "prod", action, "orders-v1") {
			t.Fatal("unauthorized action")
		}
	}
	if a.Allowed(User{Roles: []string{"viewer"}}, "other", "read", "orders-v1") {
		t.Fatal("cross cluster grant")
	}
}
func TestScopedListPermission(t *testing.T) {
	a := Authorizer{Grants: []Grant{{Role: "viewer", Cluster: "prod", Action: "read", Pattern: "orders-*"}}}
	u := User{Roles: []string{"viewer"}}
	if !a.AnyAllowed(u, "prod", "read") {
		t.Fatal("scoped grant cannot list permitted resources")
	}
	if a.AnyAllowed(u, "prod", "execute") {
		t.Fatal("scope incorrectly broadened action")
	}
}
