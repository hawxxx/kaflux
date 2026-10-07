package jobs

import (
	"context"
	"strings"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/balance"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/msk"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

// planningOnly reports what MSK reports while intelligent rebalancing is ACTIVE.
type planningOnly struct {
	*kafka.Demo
	reassigns int
}

func (p *planningOnly) Capabilities(context.Context, bool) (msk.Capabilities, error) {
	return msk.Capabilities{Kind: "MSK Express", RebalancingStatus: "ACTIVE", PlanningAllowed: true, Reason: "intelligent rebalancing is ACTIVE"}, nil
}

func (p *planningOnly) Reassign(ctx context.Context, changes []model.Change) error {
	p.reassigns++
	return p.Demo.Reassign(ctx, changes)
}

func TestQueuedPlanFailsClosedWhileOnlyPlanningIsAllowed(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	defer s.Close()
	provider := &planningOnly{Demo: kafka.NewDemo()}
	snapshot, _ := provider.Snapshot(ctx)
	plan, err := balance.Generate(snapshot, balance.Request{Topics: []string{"orders.created"}, Brokers: []int32{2, 3, 4}, RackAware: true})
	if err != nil {
		t.Fatal(err)
	}
	plan.ID, plan.ClusterID, plan.State = "queued-while-active", "demo", "queued"
	_ = s.SaveJob(ctx, plan)
	worker := Worker{Store: s, Providers: map[string]kafka.Provider{"demo": provider}, Owner: "worker"}
	worker.Step(ctx)
	result, _ := s.Job(ctx, plan.ID)
	if result.State != "failed" || !strings.Contains(result.Error, "intelligent rebalancing is ACTIVE") {
		t.Fatalf("a queued plan was not stopped: state=%s error=%q", result.State, result.Error)
	}
	if provider.reassigns != 0 {
		t.Fatal("the worker changed assignments although running is not allowed")
	}
}

func TestCapabilitiesNormalizesPlanningFromManualReassignment(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		in           msk.Capabilities
		wantPlanning bool
		wantRunnable bool
	}{
		"can run, so can plan": {msk.Capabilities{ManualReassignmentAllowed: true}, true, true},
		"planning only":        {msk.Capabilities{PlanningAllowed: true}, true, false},
		"neither":              {msk.Capabilities{}, false, false},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := Capabilities(ctx, fixedCaps{kafka.NewDemo(), tc.in}, false)
			if err != nil || got.PlanningAllowed != tc.wantPlanning || got.ManualReassignmentAllowed != tc.wantRunnable {
				t.Fatalf("%+v err=%v", got, err)
			}
		})
	}
	plain, _ := Capabilities(ctx, kafka.NewDemo(), false)
	if !plain.ManualReassignmentAllowed || !plain.PlanningAllowed {
		t.Fatalf("a plain Kafka provider must allow both: %+v", plain)
	}
}

type fixedCaps struct {
	*kafka.Demo
	caps msk.Capabilities
}

func (f fixedCaps) Capabilities(context.Context, bool) (msk.Capabilities, error) { return f.caps, nil }
