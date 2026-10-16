package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/msk"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

var errCapabilityLookup = errors.New("describe cluster: connection reset")

// A failed capability lookup says nothing about whether the action is allowed, so it must not
// read as a 409 block. It is a 503 that logs its cause and keeps the provider's explanation.
func TestCapabilityLookupFailureIsUnavailableNotBlocked(t *testing.T) {
	notAllowed := msk.Capabilities{Kind: "MSK", RebalancingStatus: "ACTIVE", Reason: "Amazon MSK intelligent rebalancing is ACTIVE."}
	for name, c := range map[string]struct {
		state, action string
		caps          msk.Capabilities
		capsErr       error
		status        int
		code, message string
		logged        bool
	}{
		"resume lookup fails without a reason": {state: "paused", action: "resume", capsErr: errCapabilityLookup, status: 503, code: "capability_unavailable", message: "Cluster capabilities unavailable; retry", logged: true},
		"resume lookup fails with a reason":    {state: "paused", action: "resume", caps: msk.Capabilities{Reason: "MSK rebalance status is unverified"}, capsErr: errCapabilityLookup, status: 503, code: "capability_unavailable", message: "MSK rebalance status is unverified", logged: true},
		"resume blocked by the cluster":        {state: "paused", action: "resume", caps: notAllowed, status: 409, code: "manual_reassignment_blocked", message: notAllowed.Reason},
		"execute lookup fails":                 {state: "planned", action: "execute", capsErr: errCapabilityLookup, status: 503, code: "capability_unavailable", message: "Cluster capabilities unavailable; retry", logged: true},
		"dry run lookup fails":                 {state: "planned", action: "dry-run", capsErr: errCapabilityLookup, status: 503, code: "capability_unavailable", message: "Cluster capabilities unavailable; retry", logged: true},
		"execute blocked by the cluster":       {state: "planned", action: "execute", caps: msk.Capabilities{PlanningAllowed: true, Reason: notAllowed.Reason}, status: 409, code: "manual_reassignment_blocked", message: notAllowed.Reason},
		"throttle lookup fails":                {state: "running", action: "throttle", capsErr: errCapabilityLookup, status: 503, code: "capability_unavailable", message: "Cluster capabilities unavailable; retry", logged: true},
		"throttle blocked by the cluster":      {state: "running", action: "throttle", caps: notAllowed, status: 409, code: "manual_reassignment_blocked", message: notAllowed.Reason},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			defer slog.SetDefault(previous)

			a, p, s := rebalanceAPI(t, c.caps)
			p.capsErr = c.capsErr
			plan := savedPlan(t, s, p, "job", c.state)
			extra := ""
			if c.action == "throttle" {
				extra = `,"bytesPerSec":1048576`
			}
			w := post(a, "/api/v1/clusters/demo/rebalances/job/"+c.action, confirm(plan, extra))
			if w.Code != c.status {
				t.Fatalf("status = %d, want %d: %s", w.Code, c.status, w.Body.String())
			}
			var body struct {
				Error struct{ Code, Message string } `json:"error"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err, w.Body.String())
			}
			if body.Error.Code != c.code || body.Error.Message != c.message {
				t.Fatalf("error = %+v, want %s %q", body.Error, c.code, c.message)
			}
			if got := strings.Contains(logs.String(), errCapabilityLookup.Error()); got != c.logged {
				t.Fatalf("cause logged = %v, want %v: %s", got, c.logged, logs.String())
			}
			if p.reassigns != 0 {
				t.Fatal("a refused request altered the cluster")
			}
		})
	}
}

func TestPlanGenerationReportsCapabilityLookupFailure(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	t.Cleanup(func() { s.Close() })
	p := &capsProvider{Demo: kafka.NewDemo(), capsErr: errCapabilityLookup}
	a := New(Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": p}, Clusters: []model.Cluster{{ID: "demo"}}})
	w := post(a, "/api/v1/clusters/demo/rebalances", `{"topics":["orders.created"],"brokers":[2,3,4]}`)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "capability_unavailable") {
		t.Fatalf("plan generation = %d %s", w.Code, w.Body.String())
	}
	if all, _ := s.Jobs(context.Background()); len(all) != 0 {
		t.Fatal("a plan was stored although capabilities could not be read")
	}
}
