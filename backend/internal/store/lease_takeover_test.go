package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/model"
)

func TestPostgresLeaseTakeoverFencesPreviousOwner(t *testing.T) {
	database := os.Getenv("KAFLUX_TEST_DATABASE_URL")
	if database == "" {
		t.Skip("set KAFLUX_TEST_DATABASE_URL for PostgreSQL integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	first, err := New(ctx, database)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := New(ctx, database)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	plan := model.Plan{ID: auth.Token(), ClusterID: auth.Token(), State: "queued"}
	if err := first.SaveJob(ctx, plan); err != nil {
		t.Fatal(err)
	}
	defer first.DB.Exec(context.Background(), "DELETE FROM kaflux_jobs WHERE id=$1", plan.ID)
	if !first.Claim(ctx, plan.ID, "old-worker") || second.Claim(ctx, plan.ID, "new-worker") {
		t.Fatal("active lease did not exclude competing worker")
	}
	if _, err := first.DB.Exec(ctx, "UPDATE kaflux_jobs SET lease_until=now()-interval '1 second' WHERE id=$1", plan.ID); err != nil {
		t.Fatal(err)
	}
	if !second.Claim(ctx, plan.ID, "new-worker") {
		t.Fatal("expired lease could not be recovered")
	}
	stale := plan
	stale.State = "failed"
	if err := first.SaveClaimedJob(ctx, stale, "old-worker"); err == nil {
		t.Fatal("previous owner overwrote recovered job")
	}
	plan.State = "running"
	if err := second.SaveClaimedJob(ctx, plan, "new-worker"); err != nil {
		t.Fatalf("replacement worker could not save: %v", err)
	}
	var state string
	if err := first.DB.QueryRow(ctx, "SELECT state FROM kaflux_jobs WHERE id=$1", plan.ID).Scan(&state); err != nil || state != "running" {
		t.Fatalf("replacement state lost: %q, %v", state, err)
	}
}
