package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/balance"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/msk"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

// capsProvider reports fixed capabilities on top of the demo cluster and records any call that
// would change partition assignments.
type capsProvider struct {
	*kafka.Demo
	caps      msk.Capabilities
	reassigns int
}

func (p *capsProvider) Capabilities(context.Context, bool) (msk.Capabilities, error) {
	return p.caps, nil
}

func (p *capsProvider) Reassign(ctx context.Context, changes []model.Change) error {
	p.reassigns++
	return p.Demo.Reassign(ctx, changes)
}

// intelligentRebalancingActive is what the capability checker reports while AWS owns placement.
var intelligentRebalancingActive = msk.Capabilities{
	Kind: "MSK Express", RebalancingStatus: "ACTIVE", PlanningAllowed: true,
	Reason: "Amazon MSK intelligent rebalancing is ACTIVE.",
}

func rebalanceAPI(t *testing.T, caps msk.Capabilities) (*API, *capsProvider, *store.Store) {
	t.Helper()
	s, _ := store.New(context.Background(), "")
	t.Cleanup(func() { s.Close() })
	p := &capsProvider{Demo: kafka.NewDemo(), caps: caps}
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": p}, Clusters: []model.Cluster{{ID: "demo"}}})
	return a, p, s
}

func post(a *API, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(body))
	r.Header.Set("X-CSRF-Token", a.demo.CSRF)
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)
	return w
}

func savedPlan(t *testing.T, s *store.Store, p *capsProvider, id, state string) model.Plan {
	t.Helper()
	snap, _ := p.Demo.Snapshot(context.Background())
	plan, err := balance.Generate(snap, balance.Request{Topics: []string{"orders.created"}, Brokers: []int32{2, 3, 4}, RackAware: true})
	if err != nil {
		t.Fatal(err)
	}
	plan.ID, plan.ClusterID, plan.State = id, "demo", state
	if err := s.SaveJob(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestPlanningWorksWhileIntelligentRebalancingIsActive(t *testing.T) {
	a, _, s := rebalanceAPI(t, intelligentRebalancingActive)
	w := post(a, "/api/v1/clusters/demo/rebalances", `{"topics":["orders.created"],"brokers":[2,3,4]}`)
	if w.Code != 200 {
		t.Fatalf("plan generation blocked: %d %s", w.Code, w.Body.String())
	}
	var plan model.Plan
	if err := json.Unmarshal(w.Body.Bytes(), &struct {
		Data *model.Plan `json:"data"`
	}{&plan}); err != nil || plan.ID == "" || len(plan.Changes) == 0 {
		t.Fatalf("no plan returned: %v %s", err, w.Body.String())
	}
	all, _ := s.Jobs(context.Background())
	if len(all) != 1 || all[0].State != "planned" {
		t.Fatalf("plan not stored as planned: %+v", all)
	}
}

func TestDryRunAndRollbackPlanningWorkWhileActiveButExecuteDoesNot(t *testing.T) {
	a, p, s := rebalanceAPI(t, intelligentRebalancingActive)
	planned := savedPlan(t, s, p, "planned-1", "planned")
	approval, _ := json.Marshal(map[string]any{"confirmation": true, "planHash": planned.PlanHash})

	if w := post(a, "/api/v1/clusters/demo/rebalances/planned-1/dry-run", string(approval)); w.Code != 200 || !strings.Contains(w.Body.String(), `"valid":true`) {
		t.Fatalf("dry run blocked: %d %s", w.Code, w.Body.String())
	}

	w := post(a, "/api/v1/clusters/demo/rebalances/planned-1/execute", string(approval))
	if w.Code != 409 || !strings.Contains(w.Body.String(), "manual_reassignment_blocked") {
		t.Fatalf("execute was not blocked: %d %s", w.Code, w.Body.String())
	}
	stored, _ := s.Job(context.Background(), "planned-1")
	if stored.State != "planned" || p.reassigns != 0 {
		t.Fatalf("blocked execute still changed something: state=%s reassigns=%d", stored.State, p.reassigns)
	}

	done := savedPlan(t, s, p, "finished-1", "completed")
	rollbackBody, _ := json.Marshal(map[string]any{"confirmation": true, "planHash": done.PlanHash})
	w = post(a, "/api/v1/clusters/demo/rebalances/finished-1/rollback", string(rollbackBody))
	if w.Code == 409 && strings.Contains(w.Body.String(), "manual_reassignment_blocked") {
		t.Fatalf("rollback planning blocked: %d %s", w.Code, w.Body.String())
	}
	t.Logf("rollback answered %d: %.160s", w.Code, w.Body.String())
	if p.reassigns != 0 {
		t.Fatal("rollback planning altered the cluster")
	}
}

func TestThrottleChangeStaysBlockedWhileActive(t *testing.T) {
	a, p, s := rebalanceAPI(t, intelligentRebalancingActive)
	// The handler rejects jobs that are not running before it looks at capabilities, so the job has
	// to be running for this to exercise the capability gate and not the state check.
	running := savedPlan(t, s, p, "running-1", "running")
	body, _ := json.Marshal(map[string]any{"confirmation": true, "planHash": running.PlanHash, "bytesPerSec": 1048576})
	w := post(a, "/api/v1/clusters/demo/rebalances/running-1/throttle", string(body))
	if w.Code != 409 || !strings.Contains(w.Body.String(), "manual_reassignment_blocked") {
		t.Fatalf("throttle change was not stopped by the capability gate: %d %s", w.Code, w.Body.String())
	}
	stored, _ := s.Job(context.Background(), "running-1")
	if stored.ThrottleRequest != nil {
		t.Fatal("a throttle request was stored although running is not allowed")
	}

	// The same request on a cluster that may run plans is accepted, so the 409 above is the gate.
	allowed, ap, as := rebalanceAPI(t, msk.Capabilities{Kind: "Kafka", ManualReassignmentAllowed: true})
	run2 := savedPlan(t, as, ap, "running-2", "running")
	ok, _ := json.Marshal(map[string]any{"confirmation": true, "planHash": run2.PlanHash, "bytesPerSec": 1048576})
	if w := post(allowed, "/api/v1/clusters/demo/rebalances/running-2/throttle", string(ok)); w.Code == 409 && strings.Contains(w.Body.String(), "manual_reassignment_blocked") {
		t.Fatalf("a cluster that may run plans was blocked: %s", w.Body.String())
	}
}

func TestPlanningStaysBlockedWhenTheClusterCannotBeVerified(t *testing.T) {
	for name, caps := range map[string]msk.Capabilities{
		"unknown status": {Kind: "MSK", RebalancingStatus: "UNKNOWN", Reason: "unverified"},
		"serverless":     {Kind: "MSK Serverless", Reason: "managed by AWS"},
		"nothing set":    {Reason: "no information"},
	} {
		t.Run(name, func(t *testing.T) {
			a, _, s := rebalanceAPI(t, caps)
			w := post(a, "/api/v1/clusters/demo/rebalances", `{"topics":["orders.created"]}`)
			if w.Code != 409 || !strings.Contains(w.Body.String(), "manual_reassignment_blocked") {
				t.Fatalf("planning allowed without verification: %d %s", w.Code, w.Body.String())
			}
			if all, _ := s.Jobs(context.Background()); len(all) != 0 {
				t.Fatal("blocked plan was stored")
			}
		})
	}
}

func TestCapabilitiesEndpointExposesPlanningAllowed(t *testing.T) {
	a, _, _ := rebalanceAPI(t, intelligentRebalancingActive)
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/clusters/demo/capabilities", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"planningAllowed":true`) || !strings.Contains(w.Body.String(), `"manualReassignmentAllowed":false`) {
		t.Fatalf("unexpected capabilities: %d %s", w.Code, w.Body.String())
	}
}

func TestFullyAllowedClustersCanPlanAndRun(t *testing.T) {
	a, p, s := rebalanceAPI(t, msk.Capabilities{Kind: "Kafka", ManualReassignmentAllowed: true})
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/clusters/demo/capabilities", nil))
	if !strings.Contains(w.Body.String(), `"planningAllowed":true`) {
		t.Fatalf("a cluster that can run plans must also report planning: %s", w.Body.String())
	}
	planned := savedPlan(t, s, p, "planned-3", "planned")
	approval, _ := json.Marshal(map[string]any{"confirmation": true, "planHash": planned.PlanHash})
	if w := post(a, "/api/v1/clusters/demo/rebalances/planned-3/dry-run", string(approval)); w.Code != 200 {
		t.Fatalf("dry run on an allowed cluster failed: %d %s", w.Code, w.Body.String())
	}
}
