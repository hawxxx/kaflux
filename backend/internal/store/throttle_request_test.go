package store

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"os"
	"testing"
	"time"
)

func TestThrottleRequestSurvivesStaleWorkerAndRequiresItsLease(t *testing.T) {
	testThrottleRequest(t, "")
}
func TestPostgresThrottleRequestSurvivesStaleWorker(t *testing.T) {
	url := os.Getenv("KAFLUX_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("PostgreSQL fixture not configured")
	}
	testThrottleRequest(t, url)
}
func testThrottleRequest(t *testing.T, url string) {
	ctx := context.Background()
	s, e := New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	plan := model.Plan{ID: t.Name() + "-" + auth.Token(), ClusterID: t.Name() + "-" + auth.Token(), State: "running", ThrottleBytesPerSec: 100}
	if e = s.SaveJob(ctx, plan); e != nil {
		t.Fatal(e)
	}
	if !s.Claim(ctx, plan.ID, "owner") {
		t.Fatal("claim failed")
	}
	request := model.ThrottleRequest{Revision: "revision", BytesPerSec: 200, Actor: "operator", At: time.Now().UTC()}
	if e = s.RequestThrottle(ctx, plan.ID, request); e != nil {
		t.Fatal(e)
	}
	if e = s.RequestThrottle(ctx, plan.ID, request); e == nil {
		t.Fatal("second pending request accepted")
	}
	if e = s.SaveClaimedJob(ctx, plan, "owner"); e != nil {
		t.Fatal(e)
	}
	fresh, _ := s.Job(ctx, plan.ID)
	if fresh.ThrottleRequest == nil || fresh.ThrottleRequest.Revision != request.Revision {
		t.Fatal("pending request lost")
	}
	if e = s.AcknowledgeThrottle(ctx, plan.ID, "other", request.Revision, 200); e == nil {
		t.Fatal("unowned worker acknowledged")
	}
	if e = s.AcknowledgeThrottle(ctx, plan.ID, "owner", "stale", 200); e == nil {
		t.Fatal("stale revision acknowledged")
	}
	if e = s.AcknowledgeThrottle(ctx, plan.ID, "owner", request.Revision, 200); e != nil {
		t.Fatal(e)
	}
	// The old payload must not restore the request or its old verified rate.
	if e = s.SaveClaimedJob(ctx, plan, "owner"); e != nil {
		t.Fatal(e)
	}
	fresh, _ = s.Job(ctx, plan.ID)
	if fresh.ThrottleRequest != nil || fresh.ThrottleBytesPerSec != 200 {
		t.Fatal("acknowledgement overwritten")
	}
	if e = s.RequestCancel(ctx, plan.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.RequestThrottle(ctx, plan.ID, request); e == nil {
		t.Fatal("canceling job accepted throttle")
	}
}
