package api

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/msk"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDemoReadAndCSRF(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo", Mode: "demo"}}})
	r := httptest.NewRecorder()
	a.ServeHTTP(r, httptest.NewRequest("GET", "/api/v1/clusters/demo/topics", nil))
	if r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	r = httptest.NewRecorder()
	a.ServeHTTP(r, httptest.NewRequest("POST", "/api/v1/clusters/demo/rebalances", nil))
	if r.Code != http.StatusForbidden {
		t.Fatalf("csrf not rejected: %d", r.Code)
	}
}

type blockedMSK struct{ *kafka.Demo }

func (p blockedMSK) Capabilities(context.Context, bool) (msk.Capabilities, error) {
	return msk.Capabilities{Kind: "MSK", RebalancingStatus: "ACTIVE", Reason: "MSK intelligent rebalance is active"}, nil
}
func TestMSKIntelligentRebalancingBlocksManualPlan(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": blockedMSK{kafka.NewDemo()}}, Clusters: []model.Cluster{{ID: "demo"}}})
	r := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/clusters/demo/rebalances", strings.NewReader(`{"topics":["orders.created"]}`))
	req.Header.Set("X-CSRF-Token", a.demo.CSRF)
	a.ServeHTTP(r, req)
	if r.Code != 409 || !strings.Contains(r.Body.String(), "manual_reassignment_blocked") {
		t.Fatalf("MSK ACTIVE accepted: %d %s", r.Code, r.Body.String())
	}
	all, _ := s.Jobs(context.Background())
	if len(all) != 0 {
		t.Fatal("blocked MSK plan persisted")
	}
}
