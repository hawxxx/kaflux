package kafka

import (
	"context"
	"errors"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"os"
	"testing"
)

func TestACLValidationPreservesExactScopeAndRaw(t *testing.T) {
	fixture := model.ACL{ResourceType: "TOPIC", ResourceName: "orders.", PatternType: "PREFIXED", Principal: "User:operator", Host: "*", Operation: "READ", Permission: "ALLOW"}
	normalized, e := NormalizeACL(fixture)
	if e != nil || normalized.Raw == nil || *normalized.Raw != (model.ACLRaw{ResourceType: 2, PatternType: 4, Operation: 3, Permission: 3}) {
		t.Fatalf("raw encoding %+v %v", normalized, e)
	}
	if _, e = aclBuilder(fixture); e != nil {
		t.Fatal(e)
	}
	for _, alter := range []func(*model.ACL){func(a *model.ACL) { a.Principal = "operator" }, func(a *model.ACL) { a.Host = "example.invalid" }, func(a *model.ACL) { a.Operation = "ANY" }, func(a *model.ACL) { a.PatternType = "MATCH" }, func(a *model.ACL) { a.ResourceType = "ANY" }, func(a *model.ACL) { a.PatternType = "PREFIXED"; a.ResourceName = "*" }, func(a *model.ACL) { a.Raw = &model.ACLRaw{ResourceType: 4} }, func(a *model.ACL) { a.ResourceType = "CLUSTER"; a.ResourceName = "arbitrary" }} {
		copy := fixture
		alter(&copy)
		if _, e := NormalizeACL(copy); e == nil {
			t.Fatalf("invalid ACL accepted %+v", copy)
		}
	}
}
func TestNativeACLDisabledAuthorizerIntegration(t *testing.T) {
	seed := os.Getenv("KAFLUX_TEST_KAFKA_SEED")
	if seed == "" {
		t.Skip("set KAFLUX_TEST_KAFKA_SEED for Kafka fixture without authorizer")
	}
	native, e := NewNative(Config{Seeds: []string{seed}, AllowPlaintext: true})
	if e != nil {
		t.Fatal(e)
	}
	defer native.Close()
	ctx := context.Background()
	if _, e = native.ACLs(ctx); !errors.Is(e, ErrACLUnsupported) {
		t.Fatalf("expected authorizer unsupported: %v", e)
	}
	acl := model.ACL{ResourceType: "TOPIC", ResourceName: "kaflux-acl-unsupported", PatternType: "LITERAL", Principal: "User:test", Host: "*", Operation: "READ", Permission: "ALLOW"}
	if e = native.CreateACL(ctx, acl); !errors.Is(e, ErrACLUnsupported) {
		t.Fatalf("create authorizer unsupported: %v", e)
	}
	if e = native.DeleteACL(ctx, acl); !errors.Is(e, ErrACLUnsupported) {
		t.Fatalf("delete authorizer unsupported: %v", e)
	}
}
