package jobs

import (
	"context"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/balance"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/msk"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"testing"
)

type throttleDemo struct {
	*kafka.Demo
	active  map[string][]int32
	blocked bool
}

func (p throttleDemo) Pending(context.Context) (map[string][]int32, error) { return p.active, nil }
func (p throttleDemo) Capabilities(context.Context, bool) (msk.Capabilities, error) {
	if p.blocked {
		return msk.Capabilities{Reason: "MSK Express intelligent rebalance is ACTIVE"}, nil
	}
	return msk.Capabilities{ManualReassignmentAllowed: true}, nil
}
func TestWorkerVerifiesAndAcknowledgesRequestedThrottle(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	defer s.Close()
	demo := kafka.NewDemo()
	snap, _ := demo.Snapshot(ctx)
	plan, e := balance.Generate(snap, balance.Request{Topics: []string{"orders.created"}, Brokers: []int32{2, 3, 4}, RackAware: true})
	if e != nil {
		t.Fatal(e)
	}
	plan.ID = "throttle-worker"
	plan.ClusterID = "demo"
	plan.State = "running"
	plan.ThrottleBytesPerSec = 100
	_ = s.SaveJob(ctx, plan)
	request := model.ThrottleRequest{Revision: "rate-change", Actor: "operator", BytesPerSec: 200}
	_ = s.RequestThrottle(ctx, plan.ID, request)
	active := map[string][]int32{}
	for _, c := range plan.Changes {
		active[fmt.Sprintf("%s/%d", c.Topic, c.Partition)] = c.After
	}
	provider := throttleDemo{Demo: demo, active: active}
	worker := Worker{Store: s, Providers: map[string]kafka.Provider{"demo": provider}, Owner: "owner"}
	worker.Step(ctx)
	after, _ := s.Job(ctx, plan.ID)
	if after.ThrottleRequest != nil || after.ThrottleBytesPerSec != 200 {
		t.Fatalf("request not acknowledged %+v", after)
	}
	events, _ := s.Audits(ctx)
	if len(events) != 1 || events[0].Actor != "operator" || events[0].ClusterID != "demo" || events[0].Action != "throttle" {
		t.Fatalf("no applied audit %+v", events)
	}
}
func TestWorkerDoesNotChangeThrottleWhenMSKBecomesActive(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	defer s.Close()
	demo := kafka.NewDemo()
	snap, _ := demo.Snapshot(ctx)
	plan, e := balance.Generate(snap, balance.Request{Topics: []string{"orders.created"}, Brokers: []int32{2, 3, 4}, RackAware: true})
	if e != nil {
		t.Fatal(e)
	}
	plan.ID = "throttle-blocked"
	plan.ClusterID = "demo"
	plan.State = "running"
	plan.ThrottleBytesPerSec = 100
	_ = s.SaveJob(ctx, plan)
	_ = s.RequestThrottle(ctx, plan.ID, model.ThrottleRequest{Revision: "change", Actor: "operator", BytesPerSec: 200})
	active := map[string][]int32{}
	for _, c := range plan.Changes {
		active[fmt.Sprintf("%s/%d", c.Topic, c.Partition)] = c.After
	}
	worker := Worker{Store: s, Providers: map[string]kafka.Provider{"demo": throttleDemo{Demo: demo, active: active, blocked: true}}, Owner: "owner"}
	worker.Step(ctx)
	p, _ := s.Job(ctx, plan.ID)
	if p.ThrottleRequest == nil || p.ThrottleBytesPerSec != 100 || p.ThrottleError == "" {
		t.Fatal("blocked change silently applied or discarded")
	}
}
