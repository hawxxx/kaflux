package api

import (
	"context"
	"encoding/json"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdministrationRequiresActionPermission(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo"}}, Grants: []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "read", Pattern: "*"}}})
	a.demo.User.Roles = []string{"viewer"}
	for _, route := range []struct{ method, path, body string }{{"POST", "topics", `{"name":"test-topic","partitions":1,"replicationFactor":1}`}, {"DELETE", "topics/orders.created", `{"confirmation":"orders.created"}`}, {"POST", "topics/orders.created/config", `{"config":{"retention.ms":"1000"},"confirmation":true}`}, {"POST", "topics/orders.created/partitions", `{"count":20,"confirmation":true}`}, {"POST", "consumer-groups/demo-analytics/reset-offsets", `{"mode":"latest","preview":true}`}} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(route.method, "/api/v1/clusters/demo/"+route.path, strings.NewReader(route.body))
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		a.ServeHTTP(w, r)
		if w.Code != 403 {
			t.Fatalf("mutation accepted %s: %d %s", route.path, w.Code, w.Body.String())
		}
	}
}

func TestResetOffsetsRequiresReviewedInactivePlan(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}})
	request := func(group, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/api/v1/clusters/demo/consumer-groups/"+group+"/reset-offsets", strings.NewReader(body))
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		a.ServeHTTP(w, r)
		return w
	}
	active := request("demo-analytics", `{"mode":"latest","preview":true}`)
	if active.Code != 409 {
		t.Fatalf("active group accepted %d %s", active.Code, active.Body.String())
	}
	preview := request("demo-inactive", `{"mode":"earliest","preview":true}`)
	var envelope struct{ Data model.OffsetPreview }
	if e := json.Unmarshal(preview.Body.Bytes(), &envelope); e != nil || preview.Code != 200 || len(envelope.Data.Changes) != 12 {
		t.Fatalf("preview %d %s", preview.Code, preview.Body.String())
	}
	noConfirmation := request("demo-inactive", `{"mode":"earliest","preview":false}`)
	if noConfirmation.Code != 400 {
		t.Fatal("missing confirmation accepted")
	}
	stale := request("demo-inactive", `{"mode":"earliest","preview":false,"confirmation":"demo-inactive","previewHash":"stale"}`)
	if stale.Code != 409 {
		t.Fatal("stale plan accepted")
	}
	body, _ := json.Marshal(model.OffsetReset{Mode: "earliest", Confirmation: "demo-inactive", PreviewHash: envelope.Data.PreviewHash})
	applied := request("demo-inactive", string(body))
	if applied.Code != 200 || !strings.Contains(applied.Body.String(), `"applied":true`) {
		t.Fatalf("apply %d %s", applied.Code, applied.Body.String())
	}
	detail, e := a.o.Providers["demo"].(kafka.AdminProvider).GroupDetail(context.Background(), "demo-inactive")
	if e != nil || detail.Offsets[0].CommittedOffset != 0 {
		t.Fatal("reset not persisted")
	}
	events, _ := s.Audits(context.Background())
	if len(events) != 2 || events[0].Result != "intent" || events[1].Result != "success" {
		t.Fatalf("audits %+v", events)
	}
}

func TestRebalanceReadFiltersEveryPlanTopic(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	_ = s.SaveJob(context.Background(), model.Plan{ID: "allowed", ClusterID: "demo", Topics: []string{"orders.created"}, State: "planned"})
	_ = s.SaveJob(context.Background(), model.Plan{ID: "hidden", ClusterID: "demo", Topics: []string{"payments.authorized"}, State: "planned"})
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Grants: []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "read", Pattern: "orders.*"}}})
	a.demo.User.Roles = []string{"viewer"}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/clusters/demo/rebalances", nil))
	if w.Code != 200 || strings.Contains(w.Body.String(), "payments") || !strings.Contains(w.Body.String(), "allowed") {
		t.Fatalf("list exposure %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/clusters/demo/rebalances/hidden", nil))
	if w.Code != 403 {
		t.Fatalf("plan detail exposed %d", w.Code)
	}
}

func TestCancelRebalancePersistsAuditedRequest(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	_ = s.SaveJob(context.Background(), model.Plan{ID: "active", ClusterID: "demo", Topics: []string{"orders.created"}, State: "queued", PlanHash: "reviewed"})
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}})
	request := func(body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/api/v1/clusters/demo/rebalances/active/cancel", strings.NewReader(body))
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		a.ServeHTTP(w, r)
		return w
	}
	denied := request(`{"confirmation":false,"planHash":"reviewed"}`)
	if denied.Code != 409 {
		t.Fatal("unconfirmed cancellation accepted")
	}
	accepted := request(`{"confirmation":true,"planHash":"reviewed"}`)
	if accepted.Code != 200 {
		t.Fatalf("cancel %d %s", accepted.Code, accepted.Body.String())
	}
	p, _ := s.Job(context.Background(), "active")
	if !p.CancellationRequested {
		t.Fatal("request not persisted")
	}
	events, _ := s.Audits(context.Background())
	if len(events) != 2 || events[0].Action != "cancel" || events[0].Result != "intent" || events[1].Result != "success" {
		t.Fatalf("cancel audits %+v", events)
	}
	a.o.Grants = []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "read", Pattern: "*"}}
	a.demo.User.Roles = []string{"viewer"}
	denied = request(`{"confirmation":true,"planHash":"reviewed"}`)
	if denied.Code != 403 {
		t.Fatal("viewer cancellation accepted")
	}
}

func TestTopicAdministrationLifecycle(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo"}}})
	for _, route := range []struct {
		method, path, body string
		status             int
	}{{"POST", "topics", `{"name":"test-topic","partitions":1,"replicationFactor":1,"config":{"retention.ms":"1000"}}`, 200}, {"POST", "topics/test-topic/config", `{"config":{"cleanup.policy":"compact"},"confirmation":true}`, 200}, {"GET", "topics/test-topic/config", "", 200}, {"POST", "topics/test-topic/partitions", `{"count":2,"confirmation":true}`, 200}, {"DELETE", "topics/test-topic", `{"confirmation":"wrong"}`, 400}, {"DELETE", "topics/test-topic", `{"confirmation":"test-topic"}`, 200}, {"POST", "test", `{"confirmation":true}`, 200}} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(route.method, "/api/v1/clusters/demo/"+route.path, strings.NewReader(route.body))
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		a.ServeHTTP(w, r)
		if w.Code != route.status {
			t.Fatalf("%s: %d %s", route.path, w.Code, w.Body.String())
		}
	}
	events, _ := s.Audits(context.Background())
	if len(events) != 8 {
		t.Fatalf("want intent/result for four mutations, got %d", len(events))
	}
	for _, e := range events {
		if e.ClusterID != "demo" || e.Provider == "" {
			t.Fatal("missing audit context")
		}
	}
}

func TestTopicConsumersFiltersReadableGroups(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Grants: []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "read", Pattern: "orders.*"}, {Role: "viewer", Cluster: "demo", Action: "read", Pattern: "demo-order-*"}}})
	a.demo.User.Roles = []string{"viewer"}
	get := func(topic string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/clusters/demo/topics/"+topic+"/consumers", nil))
		return w
	}
	w := get("orders.created")
	var envelope struct{ Data []model.GroupDetail }
	if e := json.Unmarshal(w.Body.Bytes(), &envelope); e != nil || w.Code != 200 || len(envelope.Data) != 1 || envelope.Data[0].ID != "demo-order-service" {
		t.Fatalf("consumers %d %s", w.Code, w.Body.String())
	}
	g := envelope.Data[0]
	total := int64(0)
	for _, o := range g.Offsets {
		if o.Topic != "orders.created" {
			t.Fatalf("foreign topic offset %+v", o)
		}
		total += o.Lag
	}
	if len(g.Offsets) == 0 || g.Lag == nil || *g.Lag != total {
		t.Fatalf("lag %+v", g)
	}
	if denied := get("payments.authorized"); denied.Code != 403 {
		t.Fatalf("unreadable topic %d", denied.Code)
	}
}

func TestClusterRenameIsAuthorizedAuditedAndResettable(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	clusters := []model.Cluster{{ID: "demo", Name: "Development simulator"}}
	request := func(a *API, method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		a.ServeHTTP(w, r)
		return w
	}
	viewer := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: clusters, Grants: []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "read", Pattern: "*"}}})
	viewer.demo.User.Roles = []string{"viewer"}
	if w := request(viewer, "PUT", "/api/v1/clusters/demo/name", `{"name":"Hijacked"}`); w.Code != 403 {
		t.Fatalf("viewer rename accepted %d %s", w.Code, w.Body.String())
	}
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: clusters})
	if w := request(a, "PUT", "/api/v1/clusters/demo/name", `{"name":"bad\nname"}`); w.Code != 400 {
		t.Fatalf("control character accepted %d", w.Code)
	}
	if w := request(a, "PUT", "/api/v1/clusters/demo/name", `{"name":"  Payments prod  "}`); w.Code != 200 {
		t.Fatalf("rename %d %s", w.Code, w.Body.String())
	}
	list := request(a, "GET", "/api/v1/clusters", "")
	if !strings.Contains(list.Body.String(), `"name":"Payments prod","configuredName":"Development simulator"`) {
		t.Fatalf("list missing override %s", list.Body.String())
	}
	audits, _ := s.Audits(context.Background())
	renamed := 0
	for _, event := range audits {
		if event.Action == "rename" && event.Result == "success" && event.AdminAfter == "Payments prod" {
			renamed++
		}
	}
	if renamed != 1 {
		t.Fatalf("rename not audited: %+v", audits)
	}
	if w := request(a, "PUT", "/api/v1/clusters/demo/name", `{"name":""}`); w.Code != 200 {
		t.Fatalf("reset %d %s", w.Code, w.Body.String())
	}
	if names, _ := s.ClusterNames(context.Background()); len(names) != 0 {
		t.Fatalf("reset kept override %v", names)
	}
}

func TestClusterRenameAcrossMultipleClusters(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	clusters := []model.Cluster{{ID: "east", Name: "East"}, {ID: "west", Name: "West"}}
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"east": kafka.NewDemo(), "west": kafka.NewDemo()}, Clusters: clusters})
	request := func(id, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("PUT", "/api/v1/clusters/"+id+"/name", strings.NewReader(body))
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		a.ServeHTTP(w, r)
		return w
	}
	if w := request("east", `{"name":"Payments"}`); w.Code != 200 {
		t.Fatalf("rename east %d %s", w.Code, w.Body.String())
	}
	if w := request("west", `{"name":"payments"}`); w.Code != 409 || !strings.Contains(w.Body.String(), "name_conflict") {
		t.Fatalf("duplicate override accepted %d %s", w.Code, w.Body.String())
	}
	if w := request("west", `{"name":"east"}`); w.Code != 200 {
		t.Fatalf("renamed cluster still reserves configured name %d %s", w.Code, w.Body.String())
	}
	if w := request("east", `{"name":"EAST"}`); w.Code != 409 {
		t.Fatalf("name now used by west accepted for east %d", w.Code)
	}
	names, _ := s.ClusterNames(context.Background())
	if names["east"] != "Payments" || names["west"] != "east" || len(names) != 2 {
		t.Fatalf("overrides not independent %v", names)
	}
	audits, _ := s.Audits(context.Background())
	before := len(audits)
	if w := request("east", `{"name":"Payments"}`); w.Code != 200 {
		t.Fatalf("unchanged rename %d", w.Code)
	}
	if audits, _ = s.Audits(context.Background()); len(audits) != before {
		t.Fatalf("unchanged rename audited")
	}
}

func TestTopicConfigEditsAnyDescribedKey(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo"}}})
	request := func(method, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, "/api/v1/clusters/demo/topics/orders.created/config", strings.NewReader(body))
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		a.ServeHTTP(w, r)
		return w
	}
	entry := func(name string) model.ConfigEntry {
		var envelope struct{ Data []model.ConfigEntry }
		if e := json.Unmarshal(request("GET", "").Body.Bytes(), &envelope); e != nil {
			t.Fatal(e)
		}
		for _, x := range envelope.Data {
			if x.Name == name {
				return x
			}
		}
		t.Fatalf("%s not described", name)
		return model.ConfigEntry{}
	}
	if e := entry("compression.type"); e.Override || *e.Value != "producer" {
		t.Fatalf("default %+v", e)
	}
	if w := request("POST", `{"config":{"compression.type":"zstd","min.insync.replicas":"2"},"confirmation":true}`); w.Code != 200 {
		t.Fatalf("set %d %s", w.Code, w.Body.String())
	}
	if e := entry("compression.type"); !e.Override || *e.Value != "zstd" {
		t.Fatalf("override %+v", e)
	}
	if w := request("POST", `{"reset":["compression.type"],"confirmation":true}`); w.Code != 200 {
		t.Fatalf("reset %d %s", w.Code, w.Body.String())
	}
	if e := entry("compression.type"); e.Override || *e.Value != "producer" {
		t.Fatalf("reset %+v", e)
	}
	for _, body := range []string{`{"config":{"not.a.config":"1"},"confirmation":true}`, `{"config":{"compression.type":"lz4"},"reset":["compression.type"],"confirmation":true}`, `{"config":{"compression.type":"lz4"}}`, `{"confirmation":true}`} {
		if w := request("POST", body); w.Code != 400 {
			t.Fatalf("%s accepted: %d", body, w.Code)
		}
	}
}

func TestDeleteRebalancePlanRejectsActiveJobs(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	_ = s.SaveJob(context.Background(), model.Plan{ID: "done", ClusterID: "demo", Topics: []string{"orders.created"}, State: "completed"})
	_ = s.SaveJob(context.Background(), model.Plan{ID: "active", ClusterID: "demo", Topics: []string{"orders.created"}, State: "running"})
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}})
	request := func(id string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("DELETE", "/api/v1/clusters/demo/rebalances/"+id, nil)
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		a.ServeHTTP(w, r)
		return w
	}
	if w := request("active"); w.Code != 409 {
		t.Fatalf("active plan deleted %d %s", w.Code, w.Body.String())
	}
	if _, e := s.Job(context.Background(), "active"); e != nil {
		t.Fatal("active plan removed")
	}
	if w := request("done"); w.Code != 200 {
		t.Fatalf("delete %d %s", w.Code, w.Body.String())
	}
	if _, e := s.Job(context.Background(), "done"); e == nil {
		t.Fatal("plan not deleted")
	}
	events, _ := s.Audits(context.Background())
	if len(events) != 4 || events[2].Action != "delete-plan" || events[3].Result != "success" {
		t.Fatalf("delete audits %+v", events)
	}
	_ = s.SaveJob(context.Background(), model.Plan{ID: "planned", ClusterID: "demo", Topics: []string{"orders.created"}, State: "planned"})
	a.o.Grants = []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "read", Pattern: "*"}}
	a.demo.User.Roles = []string{"viewer"}
	if w := request("planned"); w.Code != 403 {
		t.Fatalf("viewer deletion accepted %d", w.Code)
	}
}
