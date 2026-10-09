package api

import (
	"context"
	"errors"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"github.com/twmb/franz-go/pkg/kerr"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

var topicName = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

// errRecreateIncomplete means the topic was deleted but not created again.
var errRecreateIncomplete = errors.New("topic deleted but not recreated")

// validNewTopic checks the name and sizing of a topic about to be created.
// Names starting with "__" are reserved for Kafka itself.
func validNewTopic(in model.TopicCreate) bool {
	return topicName.MatchString(in.Name) && in.Name != "." && in.Name != ".." && !strings.HasPrefix(in.Name, "__") && in.Partitions >= 1 && in.Partitions <= 10000 && in.ReplicationFactor >= 1 && in.ReplicationFactor <= 100
}

// validCopiedConfig accepts readable keys the broker describes for the source
// topic, so a copy can carry any override the source has.
func validCopiedConfig(described []model.ConfigEntry, cfg map[string]string) bool {
	if len(cfg) > 128 {
		return false
	}
	known := map[string]bool{}
	for _, e := range described {
		known[e.Name] = !e.Sensitive
	}
	for k, v := range cfg {
		if !known[k] || len(v) > 4096 {
			return false
		}
	}
	return true
}

// recreateTopic deletes the topic and creates it again from spec. Brokers
// delete asynchronously and answer TOPIC_ALREADY_EXISTS until the old topic
// is gone, so creation is retried for a bounded time.
func recreateTopic(ctx context.Context, admin kafka.AdminProvider, spec model.TopicCreate) error {
	if e := admin.DeleteTopic(ctx, spec.Name); e != nil {
		return e
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		e := admin.CreateTopic(ctx, spec)
		if e == nil {
			return nil
		}
		if !errors.Is(e, kerr.TopicAlreadyExists) || time.Now().After(deadline) {
			return errors.Join(errRecreateIncomplete, e)
		}
		select {
		case <-ctx.Done():
			return errors.Join(errRecreateIncomplete, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func validTopicConfig(cfg map[string]string) bool {
	if len(cfg) > 32 {
		return false
	}
	for k, v := range cfg {
		if len(v) > 256 {
			return false
		}
		switch k {
		case "retention.ms", "retention.bytes":
			n, e := strconv.ParseInt(v, 10, 64)
			if e != nil || n < -1 {
				return false
			}
		case "cleanup.policy":
			if v != "delete" && v != "compact" && v != "compact,delete" && v != "delete,compact" {
				return false
			}
		case "max.message.bytes", "segment.bytes", "segment.ms", "min.insync.replicas":
			n, e := strconv.ParseInt(v, 10, 64)
			if e != nil || n <= 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// validConfigChange accepts only keys the broker describes for this topic,
// excluding sensitive ones, each changed at most once.
func validConfigChange(described []model.ConfigEntry, set map[string]string, reset []string) bool {
	if len(set)+len(reset) == 0 || len(set)+len(reset) > 128 {
		return false
	}
	known := map[string]bool{}
	for _, e := range described {
		known[e.Name] = !e.Sensitive
	}
	for k, v := range set {
		if !known[k] || len(v) > 4096 {
			return false
		}
	}
	seen := map[string]bool{}
	for _, k := range reset {
		if _, both := set[k]; !known[k] || both || seen[k] {
			return false
		}
		seen[k] = true
	}
	return true
}
func adminError(w http.ResponseWriter, e error) {
	switch {
	case errors.Is(e, errRecreateIncomplete):
		fail(w, 502, "recreate_incomplete", "Topic was deleted but could not be created again; create it with the configuration recorded in the audit log")
	case errors.Is(e, kerr.PolicyViolation):
		fail(w, 422, "policy_violation", "Kafka rejected the operation because of the topic's cleanup policy")
	case errors.Is(e, store.ErrThrottleConflict):
		fail(w, 409, "throttle_conflict", e.Error())
	case errors.Is(e, store.ErrJobState):
		fail(w, 409, "invalid_state", strings.TrimPrefix(e.Error(), store.ErrJobState.Error()+": "))
	case errors.Is(e, store.ErrJobActive):
		fail(w, 409, "job_active", e.Error())
	case errors.Is(e, kafka.ErrACLUnsupported):
		fail(w, 422, "acl_unsupported", "Kafka authorizer is not enabled for this configured cluster")
	case errors.Is(e, kafka.ErrActiveGroup):
		fail(w, 409, "group_active", "Stop all consumers before resetting offsets")
	case errors.Is(e, kafka.ErrStaleOffsets):
		fail(w, 409, "stale_preview", "Offsets changed; preview again")
	case errors.Is(e, kerr.TopicAuthorizationFailed), errors.Is(e, kerr.GroupAuthorizationFailed), errors.Is(e, kerr.ClusterAuthorizationFailed):
		fail(w, 403, "broker_forbidden", "Kafka denied this operation")
	case errors.Is(e, kerr.UnknownTopicOrPartition), errors.Is(e, kerr.GroupIDNotFound):
		fail(w, 404, "not_found", "Kafka resource not found")
	default:
		fail(w, 422, "administration_failed", "Operation failed; verify resource state and broker availability before retrying")
	}
}

func (a *API) adminAudit(r *http.Request, u auth.User, id, action, resource, result string, before, after any) error {
	provider := "native-kafka"
	for _, c := range a.o.Clusters {
		if c.ID == id && c.Kind != "" {
			provider = c.Kind
		}
	}
	if a.o.Demo {
		provider = "development-simulator"
	}
	return a.o.Store.Audit(r.Context(), store.Audit{Actor: u.ID, ClusterID: id, Provider: provider, Action: action, Resource: resource, Result: result, RequestID: requestID(r), AdminBefore: before, AdminAfter: after})
}

// renameCluster sets the cluster's display-name override; an empty name or the
// configured name clears it.
func (a *API) renameCluster(w http.ResponseWriter, r *http.Request, u auth.User, id string) {
	if r.Method != "PUT" {
		fail(w, 405, "method_not_allowed", "PUT required")
		return
	}
	if !a.allowed(u, id, "rename", "*", w) {
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if !decode(w, r, &body) {
		return
	}
	name := strings.TrimSpace(body.Name)
	if utf8.RuneCountInString(name) > 64 || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		fail(w, 400, "invalid_request", "Cluster name must be at most 64 characters without control characters")
		return
	}
	names, e := a.o.Store.ClusterNames(r.Context())
	if e != nil {
		failCause(w, r, 503, "store_unavailable", "Cluster names unavailable", e)
		return
	}
	configured := id
	for _, c := range a.o.Clusters {
		if c.ID == id {
			configured = c.Name
		}
	}
	if name == configured {
		name = ""
	}
	after := name
	if after == "" {
		after = configured
	}
	// Display names identify clusters in the switcher, so they must stay unique.
	for _, c := range a.o.Clusters {
		other := c.Name
		if names[c.ID] != "" {
			other = names[c.ID]
		}
		if c.ID != id && strings.EqualFold(other, after) {
			fail(w, 409, "name_conflict", "Another cluster already uses this name")
			return
		}
	}
	before := configured
	if names[id] != "" {
		before = names[id]
	}
	if name == names[id] {
		respond(w, map[string]string{"id": id, "name": after, "configuredName": configured})
		return
	}
	if e = a.adminAudit(r, u, id, "rename", id, "intent", before, after); e != nil {
		failCause(w, r, 503, "audit_unavailable", "Rename blocked because audit storage is unavailable", e)
		return
	}
	e = a.o.Store.SetClusterName(r.Context(), id, name)
	result := "success"
	if e != nil {
		result = "failed"
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
	defer cancel()
	auditErr := a.adminAudit(r.WithContext(ctx), u, id, "rename", id, result, before, after)
	if e != nil {
		failCause(w, r, 503, "store_unavailable", "Cluster name could not be saved", e)
		return
	}
	if auditErr != nil {
		failCause(w, r, 503, "audit_uncertain", "Cluster renamed but result audit failed", auditErr)
		return
	}
	respond(w, map[string]string{"id": id, "name": after, "configuredName": configured})
}
func (a *API) adminMutation(w http.ResponseWriter, r *http.Request, u auth.User, id, action, resource string, before, after any, run func() error) bool {
	if e := a.adminAudit(r, u, id, action, resource, "intent", before, after); e != nil {
		failCause(w, r, 503, "audit_unavailable", "Mutation blocked because audit storage is unavailable", e)
		return false
	}
	e := run()
	result := "success"
	if e != nil {
		result = "failed-or-uncertain"
	}
	// Record the broker outcome even when the caller disconnects.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
	defer cancel()
	auditErr := a.adminAudit(r.WithContext(ctx), u, id, action, resource, result, before, after)
	if auditErr != nil {
		failCause(w, r, 503, "audit_uncertain", "Broker operation attempted but result audit failed; inspect the resource before retrying", auditErr)
		return false
	}
	if e != nil {
		adminError(w, e)
		return false
	}
	return true
}
func (a *API) administration(w http.ResponseWriter, r *http.Request, u auth.User, id string, p kafka.Provider, parts []string) bool {
	endpoint := parts[4]
	resource := "*"
	if len(parts) > 5 {
		resource = parts[5]
	}
	if endpoint == "test" {
		if !a.allowed(u, id, "read", "*", w) {
			return true
		}
		if r.Method != "POST" || len(parts) != 5 {
			fail(w, 405, "method_not_allowed", "POST required")
			return true
		}
		var body struct {
			Confirmation bool `json:"confirmation"`
		}
		if !decode(w, r, &body) {
			return true
		}
		start := time.Now()
		snap, e := p.Snapshot(r.Context())
		if e != nil {
			failCause(w, r, 503, "connection_failed", "Configured Kafka connection failed; verify TLS, authentication and broker availability", e)
			return true
		}
		respond(w, map[string]any{"status": "connected", "configured": true, "brokerCount": len(snap.Brokers), "latencyMs": time.Since(start).Milliseconds(), "checkedAt": time.Now().UTC()})
		return true
	}
	handled := endpoint == "topics" && (r.Method != "GET" || (len(parts) == 7 && (parts[6] == "config" || parts[6] == "consumers"))) || endpoint == "consumer-groups" && len(parts) >= 6
	if !handled {
		return false
	}
	action := "read"
	if endpoint == "topics" {
		if r.Method == "DELETE" {
			action = "delete"
		} else if r.Method != "GET" {
			switch {
			case len(parts) == 5:
				action = "create"
			case len(parts) == 7 && (parts[6] == "truncate" || parts[6] == "recreate"):
				action = "delete"
			case len(parts) == 7 && parts[6] == "copy":
				// Reading the source is checked here; creating the copy is checked by name below.
				action = "read"
			default:
				action = "alter-config"
			}
		}
	} else if len(parts) == 7 && parts[6] == "reset-offsets" {
		action = "reset-offsets"
	}
	if action == "create" && len(parts) == 5 {
		if !(auth.Authorizer{Grants: a.o.Grants}).AnyAllowed(u, id, action) {
			fail(w, 403, "forbidden", "Permission denied")
			return true
		}
	} else if !a.allowed(u, id, action, resource, w) {
		return true
	}
	admin, ok := p.(kafka.AdminProvider)
	if !ok {
		fail(w, 422, "unsupported", "Provider does not support administration")
		return true
	}
	if endpoint == "consumer-groups" {
		a.groupAdministration(w, r, u, id, resource, admin, parts)
		return true
	}
	if len(parts) == 7 && parts[6] == "consumers" {
		if r.Method != "GET" {
			fail(w, 405, "method_not_allowed", "GET required")
			return true
		}
		groups, e := admin.TopicConsumers(r.Context(), resource)
		if e != nil {
			adminError(w, e)
			return true
		}
		authorizer := auth.Authorizer{Grants: a.o.Grants}
		out := []model.GroupDetail{}
		for _, g := range groups {
			if authorizer.Allowed(u, id, "read", g.ID) {
				out = append(out, g)
			}
		}
		respond(w, out)
		return true
	}
	if r.Method == "POST" && len(parts) == 5 {
		var in model.TopicCreate
		if !decode(w, r, &in) {
			return true
		}
		if !validNewTopic(in) || !validTopicConfig(in.Config) {
			fail(w, 400, "invalid_request", "Invalid topic name, partition count, replication factor or configuration")
			return true
		}
		if !a.allowed(u, id, "create", in.Name, w) {
			return true
		}
		if a.adminMutation(w, r, u, id, "create", in.Name, nil, in, func() error { return admin.CreateTopic(r.Context(), in) }) {
			respond(w, map[string]bool{"ok": true})
		}
		return true
	}
	if len(parts) == 6 && r.Method == "DELETE" {
		var in struct {
			Confirmation string `json:"confirmation"`
		}
		if !decode(w, r, &in) {
			return true
		}
		if in.Confirmation != resource {
			fail(w, 400, "confirmation_required", "Type the topic name to confirm deletion")
			return true
		}
		before, e := admin.TopicConfig(r.Context(), resource)
		if e != nil {
			adminError(w, e)
			return true
		}
		if a.adminMutation(w, r, u, id, "delete", resource, model.ConfigValues(before), nil, func() error { return admin.DeleteTopic(r.Context(), resource) }) {
			respond(w, map[string]bool{"ok": true})
		}
		return true
	}
	if len(parts) == 7 && r.Method == "POST" && (parts[6] == "truncate" || parts[6] == "recreate" || parts[6] == "copy") {
		a.topicLifecycle(w, r, u, id, resource, parts[6], p, admin)
		return true
	}
	if len(parts) == 7 && parts[6] == "config" {
		before, e := admin.TopicConfig(r.Context(), resource)
		if e != nil {
			adminError(w, e)
			return true
		}
		if r.Method == "GET" {
			respond(w, before)
			return true
		}
		if r.Method == "POST" {
			var in struct {
				Config       map[string]string `json:"config"`
				Reset        []string          `json:"reset"`
				Confirmation bool              `json:"confirmation"`
			}
			if !decode(w, r, &in) {
				return true
			}
			if !in.Confirmation || !validConfigChange(before, in.Config, in.Reset) {
				fail(w, 400, "invalid_request", "Supported configuration and confirmation required")
				return true
			}
			current := model.ConfigValues(before)
			selected := map[string]string{}
			for k := range in.Config {
				selected[k] = current[k]
			}
			for _, k := range in.Reset {
				selected[k] = current[k]
			}
			after := map[string]any{"set": in.Config, "reset": in.Reset}
			if a.adminMutation(w, r, u, id, "alter-config", resource, selected, after, func() error {
				return admin.AlterTopicConfig(r.Context(), resource, in.Config, in.Reset)
			}) {
				respond(w, map[string]bool{"ok": true})
			}
			return true
		}
	}
	if len(parts) == 7 && parts[6] == "partitions" && r.Method == "POST" {
		var in struct {
			Count        int32 `json:"count"`
			Confirmation bool  `json:"confirmation"`
		}
		if !decode(w, r, &in) {
			return true
		}
		if !in.Confirmation || in.Count < 1 || in.Count > 10000 {
			fail(w, 400, "invalid_request", "Partition count and confirmation required")
			return true
		}
		snap, e := p.Snapshot(r.Context())
		if e != nil {
			adminError(w, e)
			return true
		}
		count := 0
		for _, t := range snap.Topics {
			if t.Name == resource {
				count = len(t.Partitions)
			}
		}
		if int(in.Count) <= count {
			fail(w, 400, "invalid_request", "Partition count must increase")
			return true
		}
		if a.adminMutation(w, r, u, id, "alter-config", resource, map[string]int{"partitions": count}, map[string]int32{"partitions": in.Count}, func() error { return admin.IncreasePartitions(r.Context(), resource, in.Count) }) {
			respond(w, map[string]bool{"ok": true})
		}
		return true
	}
	fail(w, 405, "method_not_allowed", "Unsupported administration method")
	return true
}

// topicLifecycle clears, recreates or copies an existing topic. The caller has
// already authorized "delete" (truncate, recreate) or "read" (copy) on it.
func (a *API) topicLifecycle(w http.ResponseWriter, r *http.Request, u auth.User, id, topic, op string, p kafka.Provider, admin kafka.AdminProvider) {
	if op == "copy" {
		var in model.TopicCreate
		if !decode(w, r, &in) {
			return
		}
		if !validNewTopic(in) {
			fail(w, 400, "invalid_request", "Invalid topic name, partition count or replication factor")
			return
		}
		described, e := admin.TopicConfig(r.Context(), topic)
		if e != nil {
			adminError(w, e)
			return
		}
		if !validCopiedConfig(described, in.Config) {
			fail(w, 400, "invalid_request", "Configuration keys must be readable settings of the source topic")
			return
		}
		if !a.allowed(u, id, "create", in.Name, w) {
			return
		}
		if a.adminMutation(w, r, u, id, "create", in.Name, map[string]string{"copiedFrom": topic}, in, func() error { return admin.CreateTopic(r.Context(), in) }) {
			respond(w, map[string]bool{"ok": true})
		}
		return
	}
	var in struct {
		Confirmation string `json:"confirmation"`
	}
	if !decode(w, r, &in) {
		return
	}
	if in.Confirmation != topic {
		fail(w, 400, "confirmation_required", "Type the topic name to confirm")
		return
	}
	described, e := admin.TopicConfig(r.Context(), topic)
	if e != nil {
		adminError(w, e)
		return
	}
	if op == "truncate" {
		if !strings.Contains(model.ConfigValues(described)["cleanup.policy"], "delete") {
			fail(w, 422, "policy_violation", "Only topics whose cleanup policy includes delete can be cleared")
			return
		}
		if a.adminMutation(w, r, u, id, "truncate", topic, nil, nil, func() error { return admin.TruncateTopic(r.Context(), topic) }) {
			respond(w, map[string]bool{"ok": true})
		}
		return
	}
	if !a.allowed(u, id, "create", topic, w) {
		return
	}
	snap, e := p.Snapshot(r.Context())
	if e != nil {
		adminError(w, e)
		return
	}
	var source *model.Topic
	for i := range snap.Topics {
		if snap.Topics[i].Name == topic {
			source = &snap.Topics[i]
		}
	}
	if source == nil {
		fail(w, 404, "not_found", "Kafka resource not found")
		return
	}
	if source.IsInternal() {
		fail(w, 422, "internal_topic", "Internal topics cannot be recreated")
		return
	}
	spec := model.TopicCreate{Name: topic, Partitions: int32(len(source.Partitions)), ReplicationFactor: int16(source.ReplicationFactor), Config: map[string]string{}}
	for _, c := range described {
		if !c.Override {
			continue
		}
		if c.Sensitive || c.Value == nil {
			fail(w, 422, "sensitive_config", "Topic has sensitive configuration overrides that cannot be carried over")
			return
		}
		spec.Config[c.Name] = *c.Value
	}
	// Once deletion starts, finish even if the browser disconnects.
	ctx := context.WithoutCancel(r.Context())
	if a.adminMutation(w, r, u, id, "recreate", topic, spec, spec, func() error { return recreateTopic(ctx, admin, spec) }) {
		respond(w, map[string]bool{"ok": true})
	}
}
func (a *API) groupAdministration(w http.ResponseWriter, r *http.Request, u auth.User, id, group string, p kafka.AdminProvider, parts []string) {
	if len(parts) == 6 && r.Method == "GET" {
		out, e := p.GroupDetail(r.Context(), group)
		if e != nil {
			adminError(w, e)
			return
		}
		for _, topic := range out.Topics {
			if !a.allowed(u, id, "read", topic, w) {
				return
			}
		}
		respond(w, out)
		return
	}
	if len(parts) != 7 || parts[6] != "reset-offsets" || r.Method != "POST" {
		fail(w, 405, "method_not_allowed", "GET detail or POST reset-offsets required")
		return
	}
	var in model.OffsetReset
	if !decode(w, r, &in) {
		return
	}
	if len(in.Topics) > 100 {
		fail(w, 400, "invalid_request", "Too many topics")
		return
	}
	out, e := p.PreviewOffsets(r.Context(), group, in)
	if e != nil {
		adminError(w, e)
		return
	}
	for _, change := range out.Changes {
		if !a.allowed(u, id, "reset-offsets", change.Topic, w) {
			return
		}
	}
	if in.Preview {
		respond(w, out)
		return
	}
	if in.Confirmation != group || in.PreviewHash == "" {
		fail(w, 400, "confirmation_required", "Group name and reviewed preview hash required")
		return
	}
	if in.PreviewHash != out.PreviewHash {
		adminError(w, kafka.ErrStaleOffsets)
		return
	}
	if a.adminMutation(w, r, u, id, "reset-offsets", group, out.Changes, out.Changes, func() error { var err error; out, err = p.ResetOffsets(r.Context(), group, in); return err }) {
		respond(w, out)
	}
}
