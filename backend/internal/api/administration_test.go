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
