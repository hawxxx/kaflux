package jobs

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/balance"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"testing"
	"time"
)

type unsafeProvider struct{ *kafka.Demo }

func (p unsafeProvider) Snapshot(ctx context.Context) (model.Snapshot, error) {
	s, e := p.Demo.Snapshot(ctx)
	s.Topics[0].Partitions[0].ISR = nil
	return s, e
}
func TestWorkerRejectsDegradedISR(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	demo := kafka.NewDemo()
	snap, _ := demo.Snapshot(ctx)
	p, e := balance.Generate(snap, balance.Request{Topics: []string{snap.Topics[0].Name}})
	if e != nil {
		t.Fatal(e)
	}
	p.ID = "test"
	p.ClusterID = "demo"
	p.State = "queued"
	if e = s.SaveJob(ctx, p); e != nil {
		t.Fatal(e)
	}
	w := Worker{Store: s, Providers: map[string]kafka.Provider{"demo": unsafeProvider{demo}}, Owner: "test"}
	w.Step(ctx)
	out, _ := s.Job(ctx, p.ID)
	if out.State != "failed" {
		t.Fatalf("unsafe plan reached %s", out.State)
	}
}
func TestApprovalAndRecovery(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	demo := kafka.NewDemo()
	snap, _ := demo.Snapshot(ctx)
	p, e := balance.Generate(snap, balance.Request{Topics: []string{"orders.created"}, Brokers: []int32{2, 3, 4}, RackAware: true})
	if e != nil {
		t.Fatal(e)
	}
	p.ID = "recovery"
	p.ClusterID = "demo"
	p.CreatedAt = time.Now()
	if e = s.SaveJob(ctx, p); e != nil {
		t.Fatal(e)
	}
	if Approve(ctx, s, p, "actor", "wrong") == nil {
		t.Fatal("wrong plan hash accepted")
	}
	if e = Approve(ctx, s, p, "actor", p.PlanHash); e != nil {
		t.Fatal(e)
	}
	w := Worker{Store: s, Providers: map[string]kafka.Provider{"demo": demo}, Owner: "test"}
	w.Step(ctx)
	restart := Worker{Store: s, Providers: w.Providers, Owner: "test"}
	restart.Step(ctx)
	out, _ := s.Job(ctx, p.ID)
	if out.State != "completed" || out.Progress != 100 {
		t.Fatalf("recovery failed %+v", out)
	}
}
