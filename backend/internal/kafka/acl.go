package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"net"
	"sort"
	"strings"
)

var ErrACLUnsupported = errors.New("Kafka authorizer is not enabled")

type ACLProvider interface {
	ACLs(context.Context) ([]model.ACL, error)
	CreateACL(context.Context, model.ACL) error
	DeleteACL(context.Context, model.ACL) error
}

func NormalizeACL(in model.ACL) (model.ACL, error) {
	resources := map[string]int8{"TOPIC": 2, "GROUP": 3, "CLUSTER": 4, "TRANSACTIONAL_ID": 5, "DELEGATION_TOKEN": 6}
	patterns := map[string]int8{"LITERAL": 3, "PREFIXED": 4}
	operations := map[string]int8{"ALL": 2, "READ": 3, "WRITE": 4, "CREATE": 5, "DELETE": 6, "ALTER": 7, "DESCRIBE": 8, "CLUSTER_ACTION": 9, "DESCRIBE_CONFIGS": 10, "ALTER_CONFIGS": 11, "IDEMPOTENT_WRITE": 12}
	permissions := map[string]int8{"DENY": 2, "ALLOW": 3}
	resource, rok := resources[in.ResourceType]
	pattern, pok := patterns[in.PatternType]
	operation, ook := operations[in.Operation]
	permission, aok := permissions[in.Permission]
	if !rok || !pok || !ook || !aok || len(in.ResourceName) == 0 || len(in.ResourceName) > 249 || strings.ContainsAny(in.ResourceName, "\x00\r\n") || len(in.Principal) > 256 || !strings.HasPrefix(in.Principal, "User:") || len(in.Principal) <= 5 || strings.ContainsAny(in.Principal, "\x00\r\n") || (in.Host != "*" && net.ParseIP(in.Host) == nil) {
		return in, fmt.Errorf("invalid ACL")
	}
	if in.ResourceType == "CLUSTER" && (in.ResourceName != "kafka-cluster" || in.PatternType != "LITERAL") {
		return in, fmt.Errorf("cluster ACL must use literal kafka-cluster")
	}
	if in.PatternType == "PREFIXED" && in.ResourceName == "*" {
		return in, fmt.Errorf("wildcard ACL must be literal")
	}
	raw := model.ACLRaw{ResourceType: resource, PatternType: pattern, Operation: operation, Permission: permission}
	if in.Raw != nil && *in.Raw != raw {
		return in, fmt.Errorf("raw ACL disagrees with readable fields")
	}
	in.Raw = &raw
	return in, nil
}
func aclBuilder(in model.ACL) (*kadm.ACLBuilder, error) {
	acl, e := NormalizeACL(in)
	if e != nil {
		return nil, e
	}
	b := kadm.NewACLs().ResourcePatternType(kadm.ACLPattern(acl.Raw.PatternType)).Operations(kadm.ACLOperation(acl.Raw.Operation))
	switch acl.ResourceType {
	case "TOPIC":
		b.Topics(acl.ResourceName)
	case "GROUP":
		b.Groups(acl.ResourceName)
	case "CLUSTER":
		b.Clusters()
	case "TRANSACTIONAL_ID":
		b.TransactionalIDs(acl.ResourceName)
	case "DELEGATION_TOKEN":
		b.DelegationTokens(acl.ResourceName)
	}
	if acl.Permission == "ALLOW" {
		b.Allow(acl.Principal).AllowHosts(acl.Host)
	} else {
		b.Deny(acl.Principal).DenyHosts(acl.Host)
	}
	return b, b.ValidateCreate()
}
func aclProviderError(e error) error {
	if errors.Is(e, kerr.SecurityDisabled) || errors.Is(e, kerr.UnsupportedVersion) {
		return ErrACLUnsupported
	}
	return e
}
func (n *Native) ACLs(ctx context.Context) ([]model.ACL, error) {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	results, e := n.admin.DescribeACLs(c, kadm.NewACLs().AnyResource().ResourcePatternType(kadm.ACLPatternAny).Operations(kadm.OpAny).Allow().AllowHosts().Deny().DenyHosts())
	if e != nil {
		return nil, aclProviderError(e)
	}
	out := []model.ACL{}
	for _, result := range results {
		if result.Err != nil {
			return nil, aclProviderError(result.Err)
		}
		for _, a := range result.Described {
			out = append(out, model.ACL{ResourceType: a.Type.String(), ResourceName: a.Name, PatternType: a.Pattern.String(), Principal: a.Principal, Host: a.Host, Operation: a.Operation.String(), Permission: a.Permission.String(), Raw: &model.ACLRaw{ResourceType: int8(a.Type), PatternType: int8(a.Pattern), Operation: int8(a.Operation), Permission: int8(a.Permission)}})
		}
	}
	sort.Slice(out, func(i, j int) bool { return aclKey(out[i]) < aclKey(out[j]) })
	return out, nil
}
func (n *Native) CreateACL(ctx context.Context, in model.ACL) error {
	b, e := aclBuilder(in)
	if e != nil {
		return e
	}
	c, done, e := n.bounded(ctx)
	if e != nil {
		return e
	}
	defer done()
	results, e := n.admin.CreateACLs(c, b)
	if e != nil {
		return aclProviderError(e)
	}
	for _, r := range results {
		if r.Err != nil {
			return aclProviderError(r.Err)
		}
	}
	return nil
}
func (n *Native) DeleteACL(ctx context.Context, in model.ACL) error {
	b, e := aclBuilder(in)
	if e != nil {
		return e
	}
	c, done, e := n.bounded(ctx)
	if e != nil {
		return e
	}
	defer done()
	results, e := n.admin.DeleteACLs(c, b)
	if e != nil {
		return aclProviderError(e)
	}
	for _, r := range results {
		if r.Err != nil {
			return aclProviderError(r.Err)
		}
		for _, deleted := range r.Deleted {
			if deleted.Err != nil {
				return aclProviderError(deleted.Err)
			}
		}
	}
	return nil
}
func aclKey(a model.ACL) string {
	return strings.Join([]string{a.ResourceType, a.ResourceName, a.PatternType, a.Principal, a.Host, a.Operation, a.Permission}, "\x00")
}
func (d *Demo) ACLs(context.Context) ([]model.ACL, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := []model.ACL{}
	for _, acl := range d.acls {
		out = append(out, acl)
	}
	sort.Slice(out, func(i, j int) bool { return aclKey(out[i]) < aclKey(out[j]) })
	return out, nil
}
func (d *Demo) CreateACL(ctx context.Context, in model.ACL) error {
	a, e := NormalizeACL(in)
	if e != nil {
		return e
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.acls == nil {
		d.acls = map[string]model.ACL{}
	}
	d.acls[aclKey(a)] = a
	return nil
}
func (d *Demo) DeleteACL(ctx context.Context, in model.ACL) error {
	a, e := NormalizeACL(in)
	if e != nil {
		return e
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.acls, aclKey(a))
	return nil
}

// Compile-time checks keep raw enum conversions aligned with Kafka's types.
var _ = kmsg.ACLResourceTypeTopic
