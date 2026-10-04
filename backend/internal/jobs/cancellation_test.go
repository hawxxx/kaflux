package jobs

import (
	"context"
	"fmt"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/balance"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/msk"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

func TestQueuedCancellationDoesNotMutateKafka(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	demo := kafka.NewDemo()
	snapshot, _ := demo.Snapshot(ctx)
	plan, err := balance.Generate(snapshot, balance.Request{Topics: []string{"orders.created"}, Brokers: []int32{2, 3, 4}, RackAware: true})
	if err != nil {
		t.Fatal(err)
	}
	plan.ID = "cancel-queued"
	plan.ClusterID = "demo"
	plan.State = "queued"
	if err = s.SaveJob(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if err = s.RequestCancel(ctx, plan.ID); err != nil {
		t.Fatal(err)
	}
	worker := Worker{Store: s, Providers: map[string]kafka.Provider{"demo": demo}, Owner: "worker"}
	worker.Step(ctx)
	after, _ := demo.Snapshot(ctx)
	if balance.Fingerprint(snapshot, plan.Topics) != balance.Fingerprint(after, plan.Topics) {
		t.Fatal("canceled queued plan mutated Kafka")
	}
	result, _ := s.Job(ctx, plan.ID)
	if result.State != "canceled" {
		t.Fatalf("cancel state %s", result.State)
	}
}

type cancelProvider struct {
	*kafka.Demo
	pending map[string][]int32
	calls   int
}

func (p *cancelProvider) Pending(context.Context) (map[string][]int32, error) { return p.pending, nil }
func (p *cancelProvider) CancelReassignment(_ context.Context, changes []model.Change) error {
	p.calls++
	for _, c := range changes {
		delete(p.pending, fmt.Sprintf("%s/%d", c.Topic, c.Partition))
	}
	return nil
}
func TestRunningCancellationReconcilesBeforeCompletion(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	demo := kafka.NewDemo()
	snapshot, _ := demo.Snapshot(ctx)
	plan, e := balance.Generate(snapshot, balance.Request{Topics: []string{"orders.created"}, Brokers: []int32{2, 3, 4}, RackAware: true})
	if e != nil {
		t.Fatal(e)
	}
	plan.ID = "cancel-running"
	plan.ClusterID = "demo"
	plan.State = "running"
	plan.CancellationRequested = true
	provider := &cancelProvider{Demo: demo, pending: map[string][]int32{}}
	for _, c := range plan.Changes {
		provider.pending[fmt.Sprintf("%s/%d", c.Topic, c.Partition)] = c.After
	}
	_ = s.SaveJob(ctx, plan)
	worker := Worker{Store: s, Providers: map[string]kafka.Provider{"demo": provider}, Owner: "worker"}
	worker.Step(ctx)
	result, _ := s.Job(ctx, plan.ID)
	if result.State != "running" || provider.calls != 1 {
		t.Fatal("did not wait for cancellation reconciliation")
	}
	worker.Step(ctx)
	result, _ = s.Job(ctx, plan.ID)
	if result.State != "canceled" {
		t.Fatalf("state %s", result.State)
	}
	after, _ := demo.Snapshot(ctx)
	if balance.Fingerprint(snapshot, plan.Topics) != balance.Fingerprint(after, plan.Topics) {
		t.Fatal("cancel applied a reverse reassignment")
	}
}

type blockedProvider struct{ *kafka.Demo }

func (p blockedProvider) Reassign(context.Context, []model.Change) error {
	return &msk.BlockedError{Reason: "MSK Express intelligent rebalance became ACTIVE"}
}
func TestLastMomentMSKDenialIsDefiniteFailure(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	demo := kafka.NewDemo()
	snapshot, _ := demo.Snapshot(ctx)
	plan, e := balance.Generate(snapshot, balance.Request{Topics: []string{"orders.created"}, Brokers: []int32{2, 3, 4}, RackAware: true})
	if e != nil {
		t.Fatal(e)
	}
	plan.ID = "blocked"
	plan.ClusterID = "demo"
	plan.State = "queued"
	_ = s.SaveJob(ctx, plan)
	worker := Worker{Store: s, Providers: map[string]kafka.Provider{"demo": blockedProvider{demo}}, Owner: "worker"}
	worker.Step(ctx)
	result, _ := s.Job(ctx, plan.ID)
	if result.State != "failed" || result.Error != "MSK Express intelligent rebalance became ACTIVE" {
		t.Fatalf("unexpected state %s: %s", result.State, result.Error)
	}
}
