package api

import (
	"context"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLiveThrottleRequiresScopeConfirmationAndDurableRequest(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	defer s.Close()
	plan := model.Plan{ID: "active", ClusterID: "demo", State: "running", PlanHash: "reviewed", Topics: []string{"orders.created"}, ThrottleBytesPerSec: 100}
	_ = s.SaveJob(ctx, plan)
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Grants: []auth.Grant{{Role: "operator", Cluster: "demo", Action: "execute", Pattern: "orders.*"}, {Role: "viewer", Cluster: "demo", Action: "read", Pattern: "*"}}})
	a.demo.User.Roles = []string{"operator"}
	call := func(body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/api/v1/clusters/demo/rebalances/active/throttle", strings.NewReader(body))
		r.Header.Set("X-CSRF-Token", a.demo.CSRF)
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		return w
	}
	for _, body := range []string{`{"confirmation":false,"planHash":"reviewed","bytesPerSec":200}`, `{"confirmation":true,"planHash":"wrong","bytesPerSec":200}`} {
		if call(body).Code != 409 {
			t.Fatal("unconfirmed or stale throttle accepted")
		}
	}
	for _, rate := range []int64{0, -1, 1000000000001} {
		w := call(fmt.Sprintf(`{"confirmation":true,"planHash":"reviewed","bytesPerSec":%d}`, rate))
		if w.Code != 422 {
			t.Fatalf("invalid rate response %d %s", w.Code, w.Body.String())
		}
	}
	a.demo.User.Roles = []string{"viewer"}
	if call(`{"confirmation":true,"planHash":"reviewed","bytesPerSec":200}`).Code != 403 {
		t.Fatal("viewer changed throttle")
	}
	a.demo.User.Roles = []string{"operator"}
	w := call(`{"confirmation":true,"planHash":"reviewed","bytesPerSec":200}`)
	if w.Code != 200 {
		t.Fatalf("request %d %s", w.Code, w.Body.String())
	}
	fresh, _ := s.Job(ctx, plan.ID)
	if fresh.ThrottleRequest == nil || fresh.ThrottleRequest.BytesPerSec != 200 || fresh.ThrottleBytesPerSec != 100 {
		t.Fatal("request was synchronously applied or not persisted")
	}
	if call(`{"confirmation":true,"planHash":"reviewed","bytesPerSec":300}`).Code != 409 {
		t.Fatal("pending request overwritten")
	}
	events, _ := s.Audits(ctx)
	if len(events) < 2 || events[0].ClusterID != "demo" {
		t.Fatal("audit missing")
	}
}
