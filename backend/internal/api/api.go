package api

import (
	"cmp"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/balance"
	"github.com/hawxxx/kaflux/backend/internal/integrations"
	"github.com/hawxxx/kaflux/backend/internal/jobs"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Options struct {
	SessionLifetime      time.Duration
	Demo                 bool
	Store                *store.Store
	Providers            map[string]kafka.Provider
	Clusters             []model.Cluster
	AdminUser, AdminHash string
	Grants               []auth.Grant
	Metrics              http.Handler
	ClusterMetrics       func(string) http.Handler
	StaticDir            string
	OIDC                 map[string]*auth.OIDC
	LDAP                 []auth.LDAPConfig
	Integrations         map[string]*integrations.Client
}
type API struct {
	o                  Options
	demo               auth.Session
	mu                 sync.Mutex
	attempts           map[string][]time.Time
	lastAttemptCleanup time.Time
}

func New(o Options) *API {
	if len(o.Grants) == 0 {
		o.Grants = []auth.Grant{{Role: "administrator", Cluster: "*", Action: "*", Pattern: "*"}}
	}
	return &API{o: o, demo: auth.NewSession(auth.User{ID: "demo-admin", Username: "Demo operator", Roles: []string{"administrator"}, Provider: "development-simulator"}), attempts: map[string][]time.Time{}}
}
func respond(w http.ResponseWriter, data any, meta ...any) {
	w.Header().Set("Content-Type", "application/json")
	v := map[string]any{"data": data}
	if len(meta) > 0 {
		v["meta"] = meta[0]
	}
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// requestID correlates audit records with traces. A client-supplied
// X-Request-ID is used only when no trace exists and it is short and plain.
func requestID(r *http.Request) string {
	if span := trace.SpanContextFromContext(r.Context()); span.IsValid() {
		return span.TraceID().String()
	}
	if id := r.Header.Get("X-Request-ID"); requestIDPattern.MatchString(id) {
		return id
	}
	return ""
}

// failCause logs the internal cause of a server-side failure, correlated with
// the request trace, while the client receives only the sanitized message.
func failCause(w http.ResponseWriter, r *http.Request, status int, code, msg string, cause error) {
	attrs := []any{"code", code, "status", status, "path", r.URL.Path, "error", cause}
	if span := trace.SpanContextFromContext(r.Context()); span.IsValid() {
		attrs = append(attrs, "trace_id", span.TraceID().String())
	}
	slog.Error("request failed", attrs...)
	fail(w, status, code, msg)
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		fail(w, 400, "invalid_request", "Invalid request body")
		return false
	}
	var extra any
	if d.Decode(&extra) == io.EOF {
		return true
	}
	fail(w, 400, "invalid_request", "Only one JSON object is allowed")
	return false
}
func (a *API) session(r *http.Request) (auth.Session, error) {
	if a.o.Demo {
		return a.demo, nil
	}
	c, e := r.Cookie("kaflux_session")
	if e != nil {
		return auth.Session{}, e
	}
	return a.o.Store.Session(r.Context(), c.Value)
}
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	securityHeaders(w)
	path := r.URL.Path
	if a.identity(w, r) {
		return
	}
	if path == "/health" {
		respond(w, map[string]string{"status": "ok"})
		return
	}
	if path == "/ready" {
		if a.o.Store.IsPersistent() {
			if e := a.o.Store.Ping(r.Context()); e != nil {
				failCause(w, r, 503, "database_unavailable", "Database unavailable", e)
				return
			}
		}
		respond(w, map[string]string{"status": "ready"})
		return
	}
	if path == "/metrics" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP kaflux_configured_clusters Configured Kafka clusters.\n# TYPE kaflux_configured_clusters gauge\nkaflux_configured_clusters %d\n", len(a.o.Clusters))
		return
	}
	if path == "/api/v1/auth/login" {
		if r.Method != "POST" {
			fail(w, 405, "method_not_allowed", "POST required")
			return
		}
		a.login(w, r)
		return
	}
	s, e := a.session(r)
	if e != nil {
		if !strings.HasPrefix(path, "/api/") && a.o.StaticDir != "" {
			a.static(w, r)
			return
		}
		fail(w, 401, "unauthenticated", "Sign in required")
		return
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		token := r.Header.Get("X-CSRF-Token")
		if subtle.ConstantTimeCompare([]byte(token), []byte(s.CSRF)) != 1 {
			fail(w, 403, "csrf_failed", "CSRF token required")
			return
		}
	}
	if a.adminSessions(w, r, s) {
		return
	}
	if path == "/api/v1/auth/session" {
		respond(w, map[string]any{"user": s.User, "csrfToken": s.CSRF, "demo": a.o.Demo, "canManageSessions": (auth.Authorizer{Grants: a.o.Grants}).Allowed(s.User, "*", "manage-sessions", "*")})
		return
	}
	if path == "/api/v1/auth/logout" {
		if r.Method != "POST" {
			fail(w, 405, "method_not_allowed", "POST required")
			return
		}
		if err := a.o.Store.DeleteSession(r.Context(), s.ID); err != nil {
			failCause(w, r, 503, "session_revocation_failed", "Sign out unavailable; retry", err)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "kaflux_session", Value: "", Path: "/", MaxAge: -1, HttpOnly: true, Secure: !a.o.Demo, SameSite: http.SameSiteLaxMode})
		respond(w, map[string]bool{"ok": true})
		return
	}
	if strings.HasPrefix(path, "/api/v1/metrics/") && a.o.Metrics != nil {
		if path != "/api/v1/metrics/catalog" {
			fail(w, 404, "not_found", "Use cluster-scoped metrics query endpoints")
			return
		}
		if !(auth.Authorizer{Grants: a.o.Grants}).Allowed(s.User, "*", "read", "*") {
			fail(w, 403, "forbidden", "Permission denied")
			return
		}
		http.StripPrefix("/api/v1/metrics", a.o.Metrics).ServeHTTP(w, r)
		return
	}
	if path == "/api/v1/audit" {
		if !a.allowed(s.User, "*", "audit", "*", w) {
			return
		}
		v, e := a.o.Store.Audits(r.Context())
		if e != nil {
			failCause(w, r, 503, "store_unavailable", "Audit unavailable", e)
			return
		}
		respond(w, v)
		return
	}
	if path == "/api/v1/clusters" {
		list := []model.Cluster{}
		// Name overrides are cosmetic; on store failure fall back to configured names.
		names, _ := a.o.Store.ClusterNames(r.Context())
		for _, c := range a.o.Clusters {
			if !(auth.Authorizer{Grants: a.o.Grants}).AnyAllowed(s.User, c.ID, "read") {
				continue
			}
			c.ConfiguredName = c.Name
			if name := names[c.ID]; name != "" {
				c.Name = name
			}
			list = append(list, c)
		}
		bounded, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		slots := make(chan struct{}, 8)
		for i := range list {
			select {
			case slots <- struct{}{}:
			case <-bounded.Done():
				list[i].State = "unavailable"
				continue
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				defer func() { <-slots }()
				c := &list[i]
				provider := a.o.Providers[c.ID]
				if provider == nil {
					c.State = "unavailable"
					return
				}
				snap, e := provider.Snapshot(bounded)
				if e != nil {
					c.State = "unavailable"
					return
				}
				c.State = "healthy"
				c.BrokerCount = len(snap.Brokers)
				c.TopicCount = len(snap.Topics)
				c.PartitionCount = 0
				for _, topic := range snap.Topics {
					c.PartitionCount += len(topic.Partitions)
					if topic.URP > 0 {
						c.State = "degraded"
					}
					for _, p := range topic.Partitions {
						if p.Leader < 0 {
							c.State = "degraded"
						}
					}
				}
			}(i)
		}
		wg.Wait()
		respond(w, list)
		return
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 5 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "clusters" {
		if !strings.HasPrefix(path, "/api/") && a.o.StaticDir != "" {
			a.static(w, r)
			return
		}
		fail(w, 404, "not_found", "Endpoint not found")
		return
	}
	id, endpoint := parts[3], parts[4]
	provider := a.o.Providers[id]
	if provider == nil {
		fail(w, 404, "not_found", "Cluster not found")
		return
	}
	if endpoint == "name" && len(parts) == 5 {
		a.renameCluster(w, r, s.User, id)
		return
	}
	if a.integrationRoutes(w, r, s.User, id, parts) {
		return
	}
	if a.aclAdministration(w, r, s.User, id, provider, parts) {
		return
	}
	if a.administration(w, r, s.User, id, provider, parts) {
		return
	}
	if endpoint == "audit" {
		if r.Method != "GET" || len(parts) != 5 {
			fail(w, 405, "method_not_allowed", "GET required")
			return
		}
		if !a.allowed(s.User, id, "audit", "*", w) {
			return
		}
		all, e := a.o.Store.Audits(r.Context())
		if e != nil {
			failCause(w, r, 503, "store_unavailable", "Audit unavailable", e)
			return
		}
		out := []store.Audit{}
		for _, event := range all {
			if event.ClusterID == id {
				out = append(out, event)
			}
		}
		respond(w, out)
		return
	}
	action, resource := "read", "*"
	if (endpoint == "topics" || endpoint == "consumer-groups") && len(parts) > 5 {
		resource = parts[5]
	}
	if endpoint == "messages" {
		action = "consume"
		resource = r.URL.Query().Get("topic")
		if r.Method == "POST" {
			action = "produce"
			resource = "*"
		}
	}
	if endpoint == "rebalances" && r.Method == "DELETE" {
		action = "plan"
	}
	if endpoint == "rebalances" && r.Method == "POST" {
		action = "plan"
		if len(parts) > 6 {
			action = parts[6]
			if action == "cancel" || action == "throttle" {
				action = "execute"
			}
			if action == "dry-run" {
				action = "plan"
			}
		}
	}
	listScope := (endpoint == "topics" && len(parts) == 5) || (endpoint == "consumer-groups" && len(parts) == 5) || (endpoint == "rebalances") || (endpoint == "messages" && r.Method == "POST")
	if listScope {
		if !(auth.Authorizer{Grants: a.o.Grants}).AnyAllowed(s.User, id, action) {
			fail(w, 403, "forbidden", "Permission denied")
			return
		}
	} else if !a.allowed(s.User, id, action, resource, w) {
		return
	}
	if endpoint == "metrics" && a.o.ClusterMetrics != nil {
		http.StripPrefix("/api/v1/clusters/"+id+"/metrics", a.o.ClusterMetrics(id)).ServeHTTP(w, r)
		return
	}
	if endpoint == "capabilities" || endpoint == "configuration" {
		cap, _ := jobs.Capabilities(r.Context(), provider, false)
		respond(w, cap)
		return
	}
	if endpoint == "rebalances" {
		a.rebalances(w, r, s.User, id, provider, parts)
		return
	}
	if endpoint == "messages" {
		a.messages(w, r, s.User, id, provider)
		return
	}
	if endpoint == "consumer-groups" {
		g, e := provider.Groups(r.Context())
		if e != nil {
			failCause(w, r, 503, "broker_unavailable", "Consumer groups unavailable", e)
			return
		}
		if len(parts) > 5 {
			for _, x := range g {
				if x.ID == parts[5] {
					respond(w, x)
					return
				}
			}
			fail(w, 404, "not_found", "Consumer group not found")
			return
		}
		filtered := []model.Group{}
		for _, x := range g {
			if (auth.Authorizer{Grants: a.o.Grants}).Allowed(s.User, id, "read", x.ID) {
				filtered = append(filtered, x)
			}
		}
		respond(w, filtered)
		return
	}
	if endpoint == "topics" && len(parts) == 7 && parts[6] == "balance" {
		snap, e := provider.Snapshot(r.Context())
		if e != nil {
			failCause(w, r, 503, "broker_unavailable", "Kafka metadata unavailable", e)
			return
		}
		for _, t := range snap.Topics {
			if t.Name == parts[5] {
				respond(w, balance.AnalyzeTopic(snap, t))
				return
			}
		}
		fail(w, 404, "not_found", "Topic not found")
		return
	}
	if endpoint == "topics" && len(parts) > 5 {
		if detail, ok := provider.(interface {
			TopicDetail(context.Context, string) (model.Topic, error)
		}); ok {
			t, e := detail.TopicDetail(r.Context(), parts[5])
			if e != nil {
				fail(w, 404, "not_found", "Topic unavailable")
				return
			}
			v := topicRow(t)
			v["partitions"] = t.Partitions
			respond(w, v)
			return
		}
	}
	snap, e := provider.Snapshot(r.Context())
	if e != nil {
		failCause(w, r, 503, "broker_unavailable", "Kafka metadata unavailable", e)
		return
	}
	switch endpoint {
	case "brokers":
		respond(w, snap.Brokers)
	case "overview":
		tot := map[string]any{"brokers": len(snap.Brokers), "topics": len(snap.Topics), "partitions": 0, "dataSize": nil, "urp": 0, "offline": 0, "controller": nil, "consumerLag": nil}
		tot["controller"] = snap.Controller
		sizeTotal := int64(0)
		sizeKnown := true
		for _, t := range snap.Topics {
			if t.SizeBytes == nil {
				sizeKnown = false
			} else {
				sizeTotal += *t.SizeBytes
			}
			tot["partitions"] = tot["partitions"].(int) + len(t.Partitions)
			tot["urp"] = tot["urp"].(int) + t.URP
			for _, p := range t.Partitions {
				if p.Leader < 0 {
					tot["offline"] = tot["offline"].(int) + 1
				}
			}
		}
		if sizeKnown {
			tot["dataSize"] = sizeTotal
		}
		// Same replica coefficient of variation the balance advisor reports, as a percentage.
		if replicas := balance.Analyze(snap)[0]; replicas.Status != "UNAVAILABLE" {
			tot["balanceSkew"] = replicas.CV * 100
		}
		if a.o.Demo {
			groups, _ := provider.Groups(r.Context())
			lag := int64(0)
			for _, g := range groups {
				if g.Lag != nil {
					lag += *g.Lag
				}
			}
			tot["consumerLag"] = lag
		}
		var c model.Cluster
		for _, x := range a.o.Clusters {
			if x.ID == id {
				c = x
			}
		}
		respond(w, map[string]any{"cluster": c, "brokers": snap.Brokers, "totals": tot, "observedAt": snap.ObservedAt})
	case "topics":
		filtered := []model.Topic{}
		for _, t := range snap.Topics {
			if (auth.Authorizer{Grants: a.o.Grants}).Allowed(s.User, id, "read", t.Name) {
				filtered = append(filtered, t)
			}
		}
		snap.Topics = filtered
		a.topics(w, r, snap, parts)
	case "balance":
		ds := []model.Distribution{}
		for _, b := range snap.Brokers {
			ds = append(ds, model.Distribution{Broker: b.ID, Replicas: b.Partitions, Leaders: b.Leaders})
		}
		respond(w, map[string]any{"dimensions": []string{"replicas", "leaders"}, "distribution": ds, "analysis": balance.Analyze(snap), "formula": "CV = population standard deviation / mean; moderate >= 0.10, high skew >= 0.25", "capacityKnown": false})
	default:
		fail(w, 404, "not_found", "Endpoint not found")
	}
}
func (a *API) allowed(u auth.User, c, action, resource string, w http.ResponseWriter) bool {
	if (auth.Authorizer{Grants: a.o.Grants}).Allowed(u, c, action, resource) {
		return true
	}
	fail(w, 403, "forbidden", "Permission denied")
	return false
}
func (a *API) login(w http.ResponseWriter, r *http.Request) {
	if !loginRequestAllowed(w, r) {
		return
	}
	if !a.loginAllowed(r.RemoteAddr, time.Now()) {
		slog.Warn("login rate limited", "remoteAddr", r.RemoteAddr)
		w.Header().Set("Retry-After", "60")
		fail(w, 429, "rate_limited", "Too many login attempts")
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !decode(w, r, &body) {
		return
	}
	u := auth.User{ID: body.Username, Username: body.Username, Roles: []string{"administrator"}, Provider: "local"}
	// Always run bcrypt so response time does not reveal the local username.
	passwordValid := auth.Verify(a.o.AdminHash, body.Password)
	valid := subtle.ConstantTimeCompare([]byte(body.Username), []byte(a.o.AdminUser)) == 1 && passwordValid
	if !valid {
		for _, provider := range a.o.LDAP {
			candidate, e := auth.AuthenticateLDAP(r.Context(), provider, body.Username, body.Password)
			if e == nil {
				u = candidate
				valid = true
				break
			}
			// LDAP errors are sanitized; they distinguish outages from rejected credentials.
			slog.Warn("ldap authentication failed", "provider", provider.ID, "error", e)
		}
	}
	if !valid {
		slog.Warn("login failed", "username", truncate(body.Username, 128), "remoteAddr", r.RemoteAddr)
		fail(w, 401, "invalid_credentials", "Invalid credentials")
		return
	}
	s, ok := a.establishSession(w, r, u)
	if !ok {
		return
	}
	respond(w, map[string]any{"user": s.User, "csrfToken": s.CSRF, "demo": a.o.Demo, "canManageSessions": (auth.Authorizer{Grants: a.o.Grants}).Allowed(s.User, "*", "manage-sessions", "*")})
}
func topicRow(t model.Topic) map[string]any {
	return map[string]any{"name": t.Name, "partitions": len(t.Partitions), "replicationFactor": t.ReplicationFactor, "sizeBytes": t.SizeBytes, "urp": t.URP, "cleanupPolicy": t.CleanupPolicy, "retentionMs": t.RetentionMs, "observedAt": t.ObservedAt}
}

// compareTopics orders by a topic list column; unknown sizes sort below known ones.
func compareTopics(a, b model.Topic, key string) int {
	switch key {
	case "partitions":
		return cmp.Compare(len(a.Partitions), len(b.Partitions))
	case "replicationFactor":
		return cmp.Compare(a.ReplicationFactor, b.ReplicationFactor)
	case "cleanupPolicy":
		return strings.Compare(a.CleanupPolicy, b.CleanupPolicy)
	case "sizeBytes":
		if a.SizeBytes == nil || b.SizeBytes == nil {
			return cmp.Compare(boolInt(a.SizeBytes != nil), boolInt(b.SizeBytes != nil))
		}
		return cmp.Compare(*a.SizeBytes, *b.SizeBytes)
	case "urp":
		return cmp.Compare(a.URP, b.URP)
	}
	return 0
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
func (a *API) topics(w http.ResponseWriter, r *http.Request, s model.Snapshot, parts []string) {
	if len(parts) > 5 {
		for _, t := range s.Topics {
			if t.Name == parts[5] {
				v := topicRow(t)
				v["partitions"] = t.Partitions
				respond(w, v)
				return
			}
		}
		fail(w, 404, "not_found", "Topic not found")
		return
	}
	q := strings.ToLower(r.URL.Query().Get("q"))
	list := []model.Topic{}
	for _, t := range s.Topics {
		if strings.Contains(strings.ToLower(t.Name), q) {
			list = append(list, t)
		}
	}
	sortKey := r.URL.Query().Get("sort")
	desc := r.URL.Query().Get("order") == "desc"
	sort.SliceStable(list, func(i, j int) bool {
		comparison := strings.Compare(list[i].Name, list[j].Name)
		// Name breaks ties so pagination stays stable across requests.
		if c := compareTopics(list[i], list[j], sortKey); c != 0 {
			comparison = c
		}
		if desc {
			return comparison > 0
		}
		return comparison < 0
	})
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	size, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if size < 1 {
		size = 50
	}
	if size > 200 {
		size = 200
	}
	start := (page - 1) * size
	if start < 0 || start > len(list) {
		start = len(list)
	}
	end := start + size
	if end > len(list) {
		end = len(list)
	}
	rows := []map[string]any{}
	for _, t := range list[start:end] {
		rows = append(rows, topicRow(t))
	}
	respond(w, rows, map[string]int{"total": len(list), "page": page, "pageSize": size})
}
func (a *API) messages(w http.ResponseWriter, r *http.Request, u auth.User, id string, p kafka.Provider) {
	if r.Method == "POST" {
		var m model.Message
		if !decode(w, r, &m) {
			return
		}
		if m.ValueBase64 != "" {
			v, e := base64.StdEncoding.DecodeString(m.ValueBase64)
			if e != nil {
				fail(w, 400, "invalid_request", "Invalid valueBase64")
				return
			}
			m.Value = string(v)
		}
		if m.KeyBase64 != "" {
			v, e := base64.StdEncoding.DecodeString(m.KeyBase64)
			if e != nil {
				fail(w, 400, "invalid_request", "Invalid keyBase64")
				return
			}
			m.Key = string(v)
		}
		if len(m.Value) > 512*1024 || len(m.Key) > 64*1024 || len(m.Headers) > 32 || m.Partition < 0 {
			fail(w, 400, "invalid_request", "Message exceeds bounds")
			return
		}
		if !a.allowed(u, id, "produce", m.Topic, w) {
			return
		}
		if !a.audit(w, r, u, "produce", m.Topic, "intent") {
			return
		}
		out, e := p.Produce(r.Context(), m)
		if e != nil {
			_ = a.audit(w, r, u, "produce", m.Topic, "uncertain")
			failCause(w, r, 503, "produce_uncertain", "Production outcome uncertain; inspect offsets before retrying", e)
			return
		}
		_ = a.audit(w, r, u, "produce", m.Topic, "success")
		respond(w, out)
		return
	}
	if r.Method != "GET" {
		fail(w, 405, "method_not_allowed", "GET or POST required")
		return
	}
	q := r.URL.Query()
	decoder := q.Get("decoder")
	target := q.Get("decoderTarget")
	if target != "" && target != "value" && target != "key" && target != "both" {
		fail(w, 422, "invalid_decoder_target", "Choose value, key or both")
		return
	}
	var registry *integrations.Client
	if decoder != "" {
		if decoder != "avro" && decoder != "protobuf" {
			fail(w, 422, "unsupported_decoder", "Choose Avro or Protobuf decoding")
			return
		}
		registry = a.o.Integrations[q.Get("registry")]
		if registry == nil || registry.Kind() != "schemas" || registry.ClusterID() != id {
			fail(w, 422, "registry_unavailable", "Choose a schema registry configured for this cluster")
			return
		}
	}
	part, e := strconv.ParseInt(q.Get("partition"), 10, 32)
	if e != nil || part < 0 {
		fail(w, 400, "invalid_request", "partition must be nonnegative")
		return
	}
	offset, e := strconv.ParseInt(q.Get("offset"), 10, 64)
	if e != nil || offset < 0 {
		fail(w, 400, "invalid_request", "offset must be nonnegative")
		return
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit < 1 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	if timestamp := q.Get("timestamp"); timestamp != "" {
		at, e := time.Parse(time.RFC3339, timestamp)
		if e != nil {
			fail(w, 400, "invalid_request", "timestamp must be RFC3339")
			return
		}
		resolver, ok := p.(interface {
			OffsetAt(context.Context, string, int32, time.Time) (int64, error)
		})
		if !ok {
			fail(w, 422, "unsupported", "Timestamp lookup unavailable")
			return
		}
		offset, e = resolver.OffsetAt(r.Context(), q.Get("topic"), int32(part), at)
		if e != nil {
			failCause(w, r, 503, "offset_lookup_failed", "Timestamp offset unavailable", e)
			return
		}
	}
	m, e := p.Messages(r.Context(), q.Get("topic"), int32(part), offset, limit)
	if e != nil {
		failCause(w, r, 503, "consume_failed", "Unable to read messages", e)
		return
	}
	if registry != nil {
		a.decodeMessageFields(r.Context(), u, id, registry, m, decoder, target)
	}
	respond(w, m)
}
func (a *API) audit(w http.ResponseWriter, r *http.Request, u auth.User, action, resource, result string) bool {
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	cluster := ""
	segments := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(segments) >= 4 && segments[0] == "api" && segments[1] == "v1" && segments[2] == "clusters" {
		cluster = segments[3]
	}
	e := a.o.Store.Audit(r.Context(), store.Audit{Actor: u.ID, Provider: u.Provider, ClusterID: cluster, Action: action, Resource: resource, Result: result, RequestID: requestID(r), SourceIP: ip})
	if e != nil {
		failCause(w, r, 503, "audit_unavailable", "Audit persistence required", e)
		return false
	}
	return true
}
func (a *API) rebalances(w http.ResponseWriter, r *http.Request, u auth.User, id string, provider kafka.Provider, parts []string) {
	if len(parts) == 5 {
		if r.Method == "GET" {
			all, e := a.o.Store.Jobs(r.Context())
			if e != nil {
				failCause(w, r, 503, "store_unavailable", "Jobs unavailable", e)
				return
			}
			out := []model.Plan{}
			for _, p := range all {
				if p.ClusterID == id {
					visible := len(p.Topics) > 0
					for _, topic := range p.Topics {
						if !(auth.Authorizer{Grants: a.o.Grants}).Allowed(u, id, "read", topic) {
							visible = false
							break
						}
					}
					if !visible {
						continue
					}
					out = append(out, p)
				}
			}
			sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
			respond(w, out)
			return
		}
		if r.Method != "POST" {
			fail(w, 405, "method_not_allowed", "GET or POST required")
			return
		}
		var req balance.Request
		if !decode(w, r, &req) {
			return
		}
		if cap, e := jobs.Capabilities(r.Context(), provider, true); e != nil || !cap.ManualReassignmentAllowed {
			fail(w, 409, "manual_reassignment_blocked", cap.Reason)
			return
		}
		for _, t := range req.Topics {
			if !a.allowed(u, id, "plan", t, w) {
				return
			}
		}
		snap, e := jobs.FreshSnapshot(r.Context(), provider)
		if e != nil {
			failCause(w, r, 503, "broker_unavailable", "Metadata unavailable", e)
			return
		}
		p, e := balance.Generate(snap, req)
		if e != nil {
			fail(w, 422, "unsafe_plan", e.Error())
			return
		}
		if len(p.Changes) > 5000 {
			fail(w, 422, "operation_limit", "Select at most 5000 partitions per plan")
			return
		}
		p.ID = auth.Token()
		p.ClusterID = id
		p.CreatedAt = time.Now().UTC()
		p.Actor = u.ID
		if !a.audit(w, r, u, "plan", p.ID, "generated") {
			return
		}
		if e = a.o.Store.SaveJob(r.Context(), p); e != nil {
			fail(w, 409, "job_conflict", "Unable to save plan")
			return
		}
		respond(w, p)
		return
	}
	p, e := a.o.Store.Job(r.Context(), parts[5])
	if e != nil || p.ClusterID != id {
		fail(w, 404, "not_found", "Plan not found")
		return
	}
	planAction := "read"
	if r.Method == "DELETE" {
		planAction = "plan"
	}
	if len(parts) == 7 {
		planAction = parts[6]
		if planAction == "cancel" || planAction == "throttle" {
			planAction = "execute"
		}
		if planAction == "dry-run" {
			planAction = "plan"
		}
	}
	for _, t := range p.Topics {
		if !a.allowed(u, id, planAction, t, w) {
			return
		}
	}
	if len(parts) == 6 && r.Method == "GET" {
		respond(w, p)
		return
	}
	if len(parts) == 6 && r.Method == "DELETE" {
		if a.adminMutation(w, r, u, id, "delete-plan", p.ID, map[string]string{"state": p.State}, nil, func() error { return a.o.Store.DeleteJob(r.Context(), p.ID) }) {
			respond(w, map[string]string{"deleted": p.ID})
		}
		return
	}
	if len(parts) != 7 || r.Method != "POST" {
		fail(w, 405, "method_not_allowed", "POST action required")
		return
	}
	if parts[6] == "throttle" {
		a.requestThrottle(w, r, u, id, provider, p)
		return
	}
	var approval struct {
		Confirmation bool   `json:"confirmation"`
		PlanHash     string `json:"planHash"`
	}
	if !decode(w, r, &approval) {
		return
	}
	if !approval.Confirmation || approval.PlanHash != p.PlanHash {
		fail(w, 409, "approval_required", "Explicit confirmation and matching plan hash required")
		return
	}
	if parts[6] == "cancel" {
		if a.adminMutation(w, r, u, id, "cancel", p.ID, map[string]string{"state": p.State}, map[string]bool{"cancellationRequested": true}, func() error { return a.o.Store.RequestCancel(r.Context(), p.ID) }) {
			updated, e := a.o.Store.Job(r.Context(), p.ID)
			if e != nil {
				failCause(w, r, 503, "store_unavailable", "Cancellation requested; job status unavailable", e)
				return
			}
			respond(w, updated)
		}
		return
	}
	if cap, e := jobs.Capabilities(r.Context(), provider, true); e != nil || !cap.ManualReassignmentAllowed {
		fail(w, 409, "manual_reassignment_blocked", cap.Reason)
		return
	}
	snap, e := jobs.FreshSnapshot(r.Context(), provider)
	if e != nil {
		failCause(w, r, 503, "broker_unavailable", "Metadata unavailable", e)
		return
	}
	if parts[6] == "rollback" {
		if p.State != "completed" && p.State != "failed" && p.State != "canceled" {
			fail(w, 409, "invalid_state", "Only finished jobs can be rolled back")
			return
		}
		brokers := map[int32]bool{}
		for _, b := range snap.Brokers {
			brokers[b.ID] = true
		}
		for i := range p.Changes {
			c := &p.Changes[i]
			for _, b := range c.Before {
				if !brokers[b] {
					fail(w, 422, "unsafe_rollback", "Original broker no longer exists")
					return
				}
			}
			original := append([]int32{}, c.Before...)
			for _, t := range snap.Topics {
				if t.Name == c.Topic {
					for _, partition := range t.Partitions {
						if partition.ID == c.Partition {
							c.Before = append([]int32{}, partition.Replicas...)
						}
					}
				}
			}
			c.After = original
		}
		if e = jobs.Validate(snap, p.Changes); e != nil {
			fail(w, 422, "unsafe_rollback", e.Error())
			return
		}
		p.ID = auth.Token()
		p.State = "planned"
		p.Actor = u.ID
		p.ApprovedBy = ""
		p.CreatedAt = time.Now().UTC()
		p.Progress = 0
		p.Error = ""
		p.StartedAt = time.Time{}
		p.CleanupPending = false
		p.CancellationRequested = false
		p.TerminalState = ""
		p.TerminalError = ""
		p.Fingerprint = balance.Fingerprint(snap, p.Topics)
		p.PlanHash = balance.Hash(p.Changes)
		if !a.audit(w, r, u, "rollback", p.ID, "planned") {
			return
		}
		if e = a.o.Store.SaveJob(r.Context(), p); e != nil {
			fail(w, 409, "job_conflict", "Cannot persist rollback")
			return
		}
		respond(w, p)
		return
	}
	if parts[6] == "dry-run" {
		if p.State != "planned" {
			fail(w, 409, "invalid_state", "Dry run requires a planned operation")
			return
		}
		if e = jobs.Validate(snap, p.Changes); e != nil {
			fail(w, 422, "unsafe_plan", e.Error())
			return
		}
		if e = jobs.ValidateCluster(snap); e != nil {
			fail(w, 422, "unsafe_cluster", e.Error())
			return
		}
		if balance.Fingerprint(snap, p.Topics) != p.Fingerprint {
			fail(w, 409, "stale_plan", "Assignments changed; generate a new plan")
			return
		}
		pending, e := provider.Pending(r.Context())
		if e != nil {
			failCause(w, r, 503, "broker_unavailable", "Cannot verify active assignments", e)
			return
		}
		if len(pending) > 0 {
			fail(w, 409, "reassignment_conflict", "Cluster has an active reassignment")
			return
		}
		warnings := []string{}
		if !p.CapacityKnown {
			warnings = append(warnings, "Target disk capacity is not verified; confirm available capacity before execution")
		}
		respond(w, map[string]any{"valid": true, "planHash": p.PlanHash, "partitions": len(p.Changes), "capacityKnown": p.CapacityKnown, "warnings": warnings})
		return
	}
	if parts[6] != "execute" {
		fail(w, 404, "not_found", "Unknown action")
		return
	}
	if !a.o.Demo && p.ThrottleBytesPerSec <= 0 {
		fail(w, 422, "execution_policy", "Production execution requires a positive replication throttle")
		return
	}
	if e = jobs.Validate(snap, p.Changes); e != nil {
		fail(w, 422, "unsafe_plan", e.Error())
		return
	}
	if e = jobs.ValidateCluster(snap); e != nil {
		fail(w, 422, "unsafe_cluster", e.Error())
		return
	}
	pending, e := provider.Pending(r.Context())
	if e != nil {
		failCause(w, r, 503, "broker_unavailable", "Cannot verify active assignments", e)
		return
	}
	if len(pending) > 0 {
		fail(w, 409, "reassignment_conflict", "Cluster has an active reassignment")
		return
	}
	if balance.Fingerprint(snap, p.Topics) != p.Fingerprint {
		fail(w, 409, "stale_plan", "Assignments changed; generate a new plan")
		return
	}
	if !a.audit(w, r, u, "execute", p.ID, "approved") {
		return
	}
	if e = jobs.Approve(r.Context(), a.o.Store, p, u.ID, approval.PlanHash); e != nil {
		fail(w, 409, "job_conflict", e.Error())
		return
	}
	p, _ = a.o.Store.Job(r.Context(), p.ID)
	respond(w, p)
}
func (a *API) static(w http.ResponseWriter, r *http.Request) {
	clean := filepath.Clean("/" + r.URL.Path)
	target := filepath.Join(a.o.StaticDir, clean)
	if _, e := os.Stat(target); os.IsNotExist(e) && !strings.Contains(filepath.Base(clean), ".") {
		http.ServeFile(w, r, filepath.Join(a.o.StaticDir, "index.html"))
		return
	}
	http.FileServer(http.Dir(a.o.StaticDir)).ServeHTTP(w, r)
}
