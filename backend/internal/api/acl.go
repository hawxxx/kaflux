package api

import (
	"errors"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"net/http"
	"strings"
)

func aclScope(acl model.ACL) string {
	if acl.PatternType == "PREFIXED" {
		return acl.ResourceName + "*"
	}
	return acl.ResourceName
}
func aclError(w http.ResponseWriter, e error) {
	if errors.Is(e, kafka.ErrACLUnsupported) {
		fail(w, 422, "acl_unsupported", "Kafka authorizer is not enabled for this configured cluster")
		return
	}
	adminError(w, e)
}
func (a *API) aclAdministration(w http.ResponseWriter, r *http.Request, u auth.User, id string, p kafka.Provider, parts []string) bool {
	if parts[4] != "acls" {
		return false
	}
	if len(parts) != 5 {
		fail(w, 404, "not_found", "ACL endpoint not found")
		return true
	}
	action := "read"
	if r.Method != "GET" {
		action = "manage-acls"
	}
	authorize := auth.Authorizer{Grants: a.o.Grants}
	if !authorize.AnyAllowed(u, id, action) {
		fail(w, 403, "forbidden", "Permission denied")
		return true
	}
	provider, ok := p.(kafka.ACLProvider)
	if !ok {
		aclError(w, kafka.ErrACLUnsupported)
		return true
	}
	if r.Method == "GET" {
		entries, e := provider.ACLs(r.Context())
		if e != nil {
			aclError(w, e)
			return true
		}
		out := []model.ACL{}
		q := r.URL.Query()
		for _, acl := range entries {
			if !authorize.Allowed(u, id, "read", aclScope(acl)) {
				continue
			}
			if q.Get("resourceType") != "" && q.Get("resourceType") != acl.ResourceType || q.Get("resourceName") != "" && !strings.Contains(acl.ResourceName, q.Get("resourceName")) || q.Get("principal") != "" && !strings.Contains(acl.Principal, q.Get("principal")) || q.Get("operation") != "" && q.Get("operation") != acl.Operation || q.Get("permission") != "" && q.Get("permission") != acl.Permission {
				continue
			}
			out = append(out, acl)
		}
		respond(w, out)
		return true
	}
	if r.Method != "POST" && r.Method != "DELETE" {
		fail(w, 405, "method_not_allowed", "GET, POST or DELETE required")
		return true
	}
	var body struct {
		ACL          model.ACL `json:"acl"`
		Confirmation bool      `json:"confirmation"`
	}
	if !decode(w, r, &body) {
		return true
	}
	if !body.Confirmation {
		fail(w, 400, "confirmation_required", "Review the exact ACL and confirm the change")
		return true
	}
	acl, e := kafka.NormalizeACL(body.ACL)
	if e != nil {
		fail(w, 400, "invalid_request", "Invalid ACL resource, pattern, principal, host, operation or permission")
		return true
	}
	if !a.allowed(u, id, "manage-acls", aclScope(acl), w) {
		return true
	}
	// Describe first so disabled authorization cannot masquerade as ACL support.
	existing, e := provider.ACLs(r.Context())
	if e != nil {
		aclError(w, e)
		return true
	}
	before := []model.ACL{}
	for _, v := range existing {
		if v.ResourceType == acl.ResourceType && v.ResourceName == acl.ResourceName && v.PatternType == acl.PatternType && v.Principal == acl.Principal && v.Host == acl.Host && v.Operation == acl.Operation && v.Permission == acl.Permission {
			before = append(before, v)
		}
	}
	action = "create-acl"
	after := []model.ACL{acl}
	mutation := func() error { return provider.CreateACL(r.Context(), acl) }
	if r.Method == "DELETE" {
		action = "delete-acl"
		after = []model.ACL{}
		mutation = func() error { return provider.DeleteACL(r.Context(), acl) }
	}
	if a.adminMutation(w, r, u, id, action, acl.ResourceType+":"+acl.ResourceName, before, after, mutation) {
		respond(w, map[string]bool{"ok": true})
	}
	return true
}
