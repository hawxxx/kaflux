package api

import (
	"context"
	"encoding/json"
	"github.com/hawxxx/kaflux/backend/internal/balance"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDryRunDoesNotQueueOrMutateKafka(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	defer s.Close()
	demo := kafka.NewDemo()
	snapshot, _ := demo.Snapshot(ctx)
	plan, e := balance.Generate(snapshot, balance.Request{Topics: []string{"orders.created"}, Brokers: []int32{2, 3, 4}, RackAware: true})
	if e != nil {
		t.Fatal(e)
	}
	plan.ID = "dry-run"
	plan.ClusterID = "demo"
	_ = s.SaveJob(ctx, plan)
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": demo}})
	body, _ := json.Marshal(map[string]any{"confirmation": true, "planHash": plan.PlanHash})
	r := httptest.NewRequest("POST", "/api/v1/clusters/demo/rebalances/dry-run/dry-run", strings.NewReader(string(body)))
	r.Header.Set("X-CSRF-Token", a.demo.CSRF)
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("dry-run %d %s", w.Code, w.Body.String())
	}
	stored, _ := s.Job(ctx, plan.ID)
	if stored.State != "planned" || stored.CancellationRequested {
		t.Fatal("dry-run changed job")
	}
	after, _ := demo.Snapshot(ctx)
	if balance.Fingerprint(snapshot, plan.Topics) != balance.Fingerprint(after, plan.Topics) {
		t.Fatal("dry-run mutated assignments")
	}
}
