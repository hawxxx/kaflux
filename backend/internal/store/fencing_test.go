package store

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"testing"
)

func TestClaimedUpdatesRequireCurrentLease(t *testing.T) {
	ctx := context.Background()
	s, _ := New(ctx, "")
	p := model.Plan{ID: "job", ClusterID: "cluster", State: "queued"}
	_ = s.SaveJob(ctx, p)
	if !s.Claim(ctx, p.ID, "one") || s.Claim(ctx, p.ID, "two") {
		t.Fatal("lease permits concurrent owners")
	}
	p.State = "running"
	if s.SaveClaimedJob(ctx, p, "two") == nil {
		t.Fatal("unfenced owner wrote job")
	}
	if e := s.SaveClaimedJob(ctx, p, "one"); e != nil {
		t.Fatal(e)
	}
}
