package store

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/model"
)

func TestCancellationCannotBeOverwrittenByStaleWorkerSave(t *testing.T) {
	testCancellationStore(t, "")
}
func TestPostgresCancellationCannotBeOverwritten(t *testing.T) {
	url := os.Getenv("KAFLUX_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("PostgreSQL integration fixture not configured")
	}
	testCancellationStore(t, url)
}
func testCancellationStore(t *testing.T, url string) {
	ctx := context.Background()
	s, err := New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	plan := model.Plan{ID: fmt.Sprintf("cancel-test-%s", t.Name()), ClusterID: "dev", State: "running"}
	if err = s.SaveJob(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if !s.Claim(ctx, plan.ID, "worker") {
		t.Fatal("lease missing")
	}
	if err = s.RequestCancel(ctx, plan.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.SaveClaimedJob(ctx, plan, "worker"); err != nil {
		t.Fatal(err)
	}
	persisted, err := s.Job(ctx, plan.ID)
	if err != nil || !persisted.CancellationRequested {
		t.Fatal("worker lost cancellation request", err)
	}
	persisted.State = "completed"
	if err = s.SaveClaimedJob(ctx, persisted, "worker"); err != nil {
		t.Fatal(err)
	}
	if err = s.RequestCancel(ctx, plan.ID); err == nil {
		t.Fatal("completed job accepted cancellation")
	}
}
