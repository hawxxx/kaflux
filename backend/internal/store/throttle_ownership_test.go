package store

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"os"
	"testing"
)

func TestThrottleOwnershipReplacementIsFencedAndKeepsOriginal(t *testing.T) {
	url := os.Getenv("KAFLUX_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("PostgreSQL fixture not configured")
	}
	ctx := context.Background()
	s, e := New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	p := model.Plan{ID: t.Name() + "-" + auth.Token(), ClusterID: t.Name() + "-" + auth.Token(), State: "running", ThrottleBytesPerSec: 100}
	if e = s.SaveJob(ctx, p); e != nil {
		t.Fatal(e)
	}
	if !s.Claim(ctx, p.ID, "owner") {
		t.Fatal("claim")
	}
	request := model.ThrottleRequest{Revision: "update", Actor: "operator", BytesPerSec: 200}
	if e = s.RequestThrottle(ctx, p.ID, request); e != nil {
		t.Fatal(e)
	}
	original := []kafka.ThrottleRecord{{Resource: "broker", Name: "1", Key: "leader.replication.throttled.rate", EffectiveBefore: "-1", Applied: "100"}}
	if e = s.SaveThrottle(ctx, p.ID, original); e != nil {
		t.Fatal(e)
	}
	next, e := kafka.RetargetThrottle(original, 200)
	if e != nil {
		t.Fatal(e)
	}
	if s.ReplaceThrottle(ctx, p.ID, "other", request.Revision, next) == nil {
		t.Fatal("foreign worker replaced ownership")
	}
	bad := append([]kafka.ThrottleRecord(nil), next...)
	bad[0].EffectiveBefore = "100"
	if s.ReplaceThrottle(ctx, p.ID, "owner", request.Revision, bad) == nil {
		t.Fatal("original configuration overwritten")
	}
	if e = s.ReplaceThrottle(ctx, p.ID, "owner", request.Revision, next); e != nil {
		t.Fatal(e)
	}
	restored, e := s.LoadThrottle(ctx, p.ID)
	if e != nil || restored[0].Applied != "200" || restored[0].TransitionFrom != "100" {
		t.Fatal("transition not durable", e)
	}
	if e = s.RequestCancel(ctx, p.ID); e != nil {
		t.Fatal(e)
	}
	if s.ReplaceThrottle(ctx, p.ID, "owner", request.Revision, kafka.FinalizeThrottle(next)) == nil {
		t.Fatal("cancellation did not take precedence")
	}
}
