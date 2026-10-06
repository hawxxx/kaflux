package api

import (
	"context"
	"encoding/json"
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
func TestOverviewReportsReplicaBalanceSkew(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo", Mode: "demo"}}})
	r := httptest.NewRecorder()
	a.ServeHTTP(r, httptest.NewRequest("GET", "/api/v1/clusters/demo/overview", nil))
	if r.Code != 200 || !strings.Contains(r.Body.String(), `"balanceSkew":`) {
		t.Fatalf("overview lacks balance skew: %d %s", r.Code, r.Body.String())
	}
}
func TestTopicBalanceReportsBrokerPlacement(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo", Mode: "demo"}}})
	r := httptest.NewRecorder()
	a.ServeHTTP(r, httptest.NewRequest("GET", "/api/v1/clusters/demo/topics/orders.created/balance", nil))
	if r.Code != 200 || !strings.Contains(r.Body.String(), `"preferredLeaderRatio":1`) || !strings.Contains(r.Body.String(), `"preferredReplicas":2`) {
		t.Fatalf("topic balance missing placement: %d %s", r.Code, r.Body.String())
	}
	r = httptest.NewRecorder()
	a.ServeHTTP(r, httptest.NewRequest("GET", "/api/v1/clusters/demo/topics/missing/balance", nil))
	if r.Code != 404 {
		t.Fatalf("missing topic: %d", r.Code)
	}
}
func TestTopicListSortsByColumn(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo", Mode: "demo"}}})
	for _, key := range []string{"partitions", "replicationFactor", "sizeBytes", "urp"} {
		r := httptest.NewRecorder()
		a.ServeHTTP(r, httptest.NewRequest("GET", "/api/v1/clusters/demo/topics?sort="+key+"&order=desc&pageSize=200", nil))
		var body struct {
			Data []map[string]any `json:"data"`
		}
		if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil || len(body.Data) < 2 {
			t.Fatalf("%s: %v %s", key, err, r.Body.String())
		}
		value := func(i int) float64 { v, _ := body.Data[i][key].(float64); return v }
		for i := 1; i < len(body.Data); i++ {
			if value(i) > value(i-1) {
				t.Fatalf("%s not descending at %d: %v > %v", key, i, value(i), value(i-1))
			}
		}
	}
}
func TestCompareTopicsSortsUnknownSizeLowest(t *testing.T) {
	size := int64(10)
	known, unknown := model.Topic{SizeBytes: &size}, model.Topic{}
	if compareTopics(unknown, known, "sizeBytes") >= 0 || compareTopics(known, unknown, "sizeBytes") <= 0 || compareTopics(unknown, unknown, "sizeBytes") != 0 {
		t.Fatal("unknown sizes must sort below known sizes")
	}
}
