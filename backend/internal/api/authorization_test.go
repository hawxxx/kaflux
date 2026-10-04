package api

import (
	"context"
	"encoding/json"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"net/http/httptest"
	"testing"
)

func TestTopicScopedViewerCannotReadAggregates(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo"}}, Grants: []auth.Grant{{Role: "viewer", Cluster: "demo", Action: "read", Pattern: "orders.*"}}})
	a.demo.User.Roles = []string{"viewer"}
	for _, endpoint := range []string{"overview", "brokers", "balance", "metrics/query", "topics/payments.authorized"} {
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/clusters/demo/"+endpoint, nil))
		if w.Code != 403 {
			t.Fatalf("aggregate %s exposed: %d", endpoint, w.Code)
		}
	}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/clusters/demo/topics", nil))
	var envelope struct {
		Data []struct {
			Name string `json:"name"`
		}
	}
	_ = json.Unmarshal(w.Body.Bytes(), &envelope)
	if w.Code != 200 || len(envelope.Data) != 1 || envelope.Data[0].Name != "orders.created" {
		t.Fatalf("topic list wrong %d %s", w.Code, w.Body.String())
	}
}
func TestClusterScopedAuditCannotLeakOtherClusters(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	_ = s.Audit(context.Background(), store.Audit{ClusterID: "allowed", Actor: "a", Resource: "orders.created"})
	_ = s.Audit(context.Background(), store.Audit{ClusterID: "other", Actor: "private-actor", Resource: "private-topic"})
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"allowed": kafka.NewDemo(), "other": kafka.NewDemo()}, Grants: []auth.Grant{{Role: "auditor", Cluster: "allowed", Action: "audit", Pattern: "*"}}})
	a.demo.User.Roles = []string{"auditor"}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/clusters/allowed/audit", nil))
	var envelope struct{ Data []store.Audit }
	_ = json.Unmarshal(w.Body.Bytes(), &envelope)
	if w.Code != 200 || len(envelope.Data) != 1 || envelope.Data[0].ClusterID != "allowed" {
		t.Fatalf("audit scope wrong %d %s", w.Code, w.Body.String())
	}
	for _, path := range []string{"/api/v1/audit", "/api/v1/clusters/other/audit"} {
		w = httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 403 {
			t.Fatalf("audit leak %s: %d", path, w.Code)
		}
	}
}
