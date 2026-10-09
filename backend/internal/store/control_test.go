package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/model"
)

func eachBackend(t *testing.T, run func(t *testing.T, s *Store)) {
	t.Run("memory", func(t *testing.T) {
		s, e := New(context.Background(), "")
		if e != nil {
			t.Fatal(e)
		}
		run(t, s)
	})
	t.Run("sqlite", func(t *testing.T) {
		s, e := NewSQLite(context.Background(), filepath.Join(t.TempDir(), "kaflux.db"))
		if e != nil {
			t.Fatal(e)
		}
		defer s.Close()
		run(t, s)
	})
	if url := os.Getenv("KAFLUX_TEST_DATABASE_URL"); url != "" {
		t.Run("postgres", func(t *testing.T) {
			ctx := context.Background()
			s, e := New(ctx, url)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			// These tests use fixed job IDs; clear them from earlier runs.
			if _, e = s.DB.Exec(ctx, "DELETE FROM kaflux_jobs WHERE cluster_id='c'; DELETE FROM kaflux_job_events WHERE job_id IN ('a','b','r')"); e != nil {
				t.Fatal(e)
			}
			run(t, s)
		})
	}
}

func TestPausedJobHoldsClusterLockButIsNotWorked(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		if e := s.SaveJob(ctx, model.Plan{ID: "a", ClusterID: "c", State: "paused"}); e != nil {
			t.Fatal(e)
		}
		if e := s.SaveJob(ctx, model.Plan{ID: "b", ClusterID: "c", State: "queued"}); e == nil {
			t.Fatal("second active job accepted while a job is paused")
		}
		active, e := s.ActiveJobs(ctx)
		if e != nil {
			t.Fatal(e)
		}
		for _, j := range active {
			if j.ID == "a" {
				t.Fatal("paused job offered to worker")
			}
		}
		if s.Claim(ctx, "a", "owner") {
			t.Fatal("paused job claimed")
		}
		if e := s.DeleteJob(ctx, "a"); e != ErrJobActive {
			t.Fatalf("delete paused = %v", e)
		}
	})
}

func TestPauseResumeAndSkip(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		p := model.Plan{ID: "a", ClusterID: "c", State: "running", Steps: []model.TopicStep{{Topic: "x", State: model.StepFailed, Error: "boom"}, {Topic: "y", State: model.StepPending}}}
		if e := s.SaveJob(ctx, p); e != nil {
			t.Fatal(e)
		}
		if e := s.RequestPause(ctx, "a", "alice"); e != nil {
			t.Fatal(e)
		}
		if !s.Claim(ctx, "a", "w") {
			t.Fatal("claim failed")
		}
		stale := p // worker copy without the request
		stale.State = "paused"
		if e := s.SaveClaimedJob(ctx, stale, "w"); e != nil {
			t.Fatal(e)
		}
		got, _ := s.Job(ctx, "a")
		if !got.PauseRequested || got.PauseRequestedBy != "alice" {
			t.Fatalf("worker save dropped the pause request: %+v", got)
		}
		if _, e := s.Resume(ctx, "a", false); e != nil {
			t.Fatal(e)
		}
		got, _ = s.Job(ctx, "a")
		if got.State != "running" || got.PauseRequested || got.Steps[0].State != model.StepPending || got.Steps[0].Error != "" {
			t.Fatalf("resume did not retry the failed topic: %+v", got)
		}
		if _, e := s.Resume(ctx, "a", false); e == nil {
			t.Fatal("resumed a running job")
		}
		got.State = "paused"
		if e := s.SaveJob(ctx, got); e != nil {
			t.Fatal(e)
		}
		resumed, e := s.Resume(ctx, "a", true)
		if e != nil || resumed.CurrentStep != 1 || resumed.Steps[0].State != model.StepSkipped {
			t.Fatalf("skip = %+v %v", resumed, e)
		}
	})
}

func TestCancelPausedJobReturnsItToTheWorker(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		if e := s.SaveJob(ctx, model.Plan{ID: "a", ClusterID: "c", State: "paused"}); e != nil {
			t.Fatal(e)
		}
		if e := s.RequestCancelRollback(ctx, "a", "bob"); e != nil {
			t.Fatal(e)
		}
		got, _ := s.Job(ctx, "a")
		if got.State != "running" || !got.CancellationRequested || !got.RollbackRequested || got.RollbackRequestedBy != "bob" {
			t.Fatalf("cancel rollback = %+v", got)
		}
	})
}

func TestFinishWithRollbackIsAtomic(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		if e := s.SaveJob(ctx, model.Plan{ID: "a", ClusterID: "c", State: "running"}); e != nil {
			t.Fatal(e)
		}
		if !s.Claim(ctx, "a", "w") {
			t.Fatal("claim failed")
		}
		rollback := model.Plan{ID: "r", ClusterID: "c", State: "queued", RollbackOf: "a"}
		if e := s.FinishWithRollback(ctx, model.Plan{ID: "a", ClusterID: "c", State: "canceled", RollbackJob: "r"}, rollback, "other"); e == nil {
			t.Fatal("stale owner finished the job")
		}
		if _, e := s.Job(ctx, "r"); e == nil {
			t.Fatal("rollback stored although the finish failed")
		}
		if e := s.FinishWithRollback(ctx, model.Plan{ID: "a", ClusterID: "c", State: "canceled", RollbackJob: "r"}, rollback, "w"); e != nil {
			t.Fatal(e)
		}
		a, _ := s.Job(ctx, "a")
		r, e := s.Job(ctx, "r")
		if a.State != "canceled" || e != nil || r.State != "queued" {
			t.Fatalf("finish = %+v rollback = %+v %v", a, r, e)
		}
	})
}

func TestEventsAreCappedOrderedAndDeletedWithTheJob(t *testing.T) {
	eachBackend(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		if e := s.SaveJob(ctx, model.Plan{ID: "a", ClusterID: "c", State: "completed"}); e != nil {
			t.Fatal(e)
		}
		for i := 1; i <= MaxJobEvents+5; i++ {
			if e := s.AppendEvent(ctx, "a", "info", fmt.Sprintf("line %d", i)); e != nil {
				t.Fatal(e)
			}
		}
		all, e := s.Events(ctx, "a", 0, MaxJobEvents+10)
		if e != nil || len(all) != MaxJobEvents || all[0].Message != "line 6" || all[len(all)-1].Seq != int64(MaxJobEvents+5) {
			t.Fatalf("events = %d first=%v %v", len(all), all[0], e)
		}
		tail, _ := s.Events(ctx, "a", int64(MaxJobEvents+3), 10)
		if len(tail) != 2 {
			t.Fatalf("tail = %v", tail)
		}
		if e := s.DeleteJob(ctx, "a"); e != nil {
			t.Fatal(e)
		}
		if left, _ := s.Events(ctx, "a", 0, 10); len(left) != 0 {
			t.Fatalf("events survived the job: %v", left)
		}
	})
}

func TestSQLiteMigratesVersionOneSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	s, e := NewSQLite(ctx, path)
	if e != nil {
		t.Fatal(e)
	}
	for _, q := range []string{
		"DROP INDEX kaflux_one_active_job_v2",
		"CREATE UNIQUE INDEX kaflux_one_active_job ON kaflux_jobs(cluster_id) WHERE state IN ('queued','running','rollback-queued')",
		"DROP TABLE kaflux_job_events",
		"DELETE FROM kaflux_schema",
		"INSERT INTO kaflux_schema(version) VALUES(1)",
	} {
		if _, e = s.sqlite.db.ExecContext(ctx, q); e != nil {
			t.Fatal(q, e)
		}
	}
	s.Close()
	s, e = NewSQLite(ctx, path)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.SaveJob(ctx, model.Plan{ID: "a", ClusterID: "c", State: "paused"}); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveJob(ctx, model.Plan{ID: "b", ClusterID: "c", State: "queued"}); e == nil {
		t.Fatal("migrated index ignores paused jobs")
	}
	if e = s.AppendEvent(ctx, "a", "info", "hello"); e != nil {
		t.Fatal(e)
	}
}
