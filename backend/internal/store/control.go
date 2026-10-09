package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/jackc/pgx/v5"
)

// ErrJobState means the job is not in a state that allows the request.
var ErrJobState = errors.New("invalid job state")

// RequestCancel preserves the request independently of an older worker's payload.
// A paused job goes back to running so the worker can reconcile and finish it.
func (s *Store) RequestCancel(ctx context.Context, id string) error {
	return s.requestCancel(ctx, id, "")
}

// RequestCancelRollback cancels the job and asks the worker to queue a rollback
// of the partitions that moved, approved by actor, once cancellation is reconciled.
func (s *Store) RequestCancelRollback(ctx context.Context, id, actor string) error {
	return s.requestCancel(ctx, id, actor)
}

func (s *Store) requestCancel(ctx context.Context, id, rollbackBy string) error {
	_, e := s.mutateJob(ctx, id, func(p *model.Plan) error {
		if p.State != "queued" && p.State != "running" && p.State != "paused" {
			return fmt.Errorf("%w: only active jobs can be canceled", ErrJobState)
		}
		p.CancellationRequested = true
		if rollbackBy != "" {
			p.RollbackRequested = true
			p.RollbackRequestedBy = rollbackBy
		}
		if p.State == "paused" {
			p.State = "running"
		}
		return nil
	})
	return e
}

// RequestPause asks the worker to stop after the topic it is moving.
func (s *Store) RequestPause(ctx context.Context, id, actor string) error {
	_, e := s.mutateJob(ctx, id, func(p *model.Plan) error {
		if p.State != "running" || p.CancellationRequested {
			return fmt.Errorf("%w: only a running job that is not being canceled can be paused", ErrJobState)
		}
		p.PauseRequested = true
		p.PauseRequestedBy = actor
		return nil
	})
	return e
}

// Resume hands a paused job back to the worker. A failed current topic is
// retried, or skipped when skip is set.
func (s *Store) Resume(ctx context.Context, id string, skip bool) (model.Plan, error) {
	return s.mutateJob(ctx, id, func(p *model.Plan) error {
		if p.State != "paused" {
			return fmt.Errorf("%w: only paused jobs can be resumed", ErrJobState)
		}
		p.State = "running"
		p.PauseRequested = false
		p.PauseRequestedBy = ""
		p.PauseReason = ""
		p.HealthWaitSince = nil
		if p.CurrentStep < len(p.Steps) {
			step := &p.Steps[p.CurrentStep]
			if skip {
				now := time.Now().UTC()
				step.State = model.StepSkipped
				step.FinishedAt = &now
				p.CurrentStep++
			} else if step.State == model.StepFailed {
				step.State = model.StepPending
				step.Error = ""
				step.StartedAt = nil
			}
		}
		return nil
	})
}

// FinishWithRollback stores the worker's terminal job and the rollback job it
// created in one transaction, so a canceled job never loses its rollback.
func (s *Store) FinishWithRollback(ctx context.Context, finished, rollback model.Plan, owner string) error {
	if s.sqlite != nil {
		return s.sqlite.changeJob(ctx, finished.ID, func(old *model.Plan, l lease) error {
			if l.Owner != owner || !time.Now().Before(l.Until) {
				return errors.New("worker lease lost")
			}
			*old = finished
			return nil
		}, func(tx sqliteExec) error {
			b, e := marshalPlan(rollback)
			if e != nil {
				return e
			}
			_, e = tx.ExecContext(ctx, "INSERT INTO kaflux_jobs(id,cluster_id,state,payload) VALUES(?,?,?,?)", rollback.ID, rollback.ClusterID, rollback.State, b)
			return e
		})
	}
	if s.DB != nil {
		return pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
			b, e := marshalPlan(finished)
			if e != nil {
				return e
			}
			r, e := tx.Exec(ctx, "UPDATE kaflux_jobs SET state=$2,payload=$3 WHERE id=$1 AND lease_owner=$4 AND lease_until>now()", finished.ID, finished.State, b, owner)
			if e != nil {
				return e
			}
			if r.RowsAffected() != 1 {
				return errors.New("worker lease lost")
			}
			if b, e = marshalPlan(rollback); e != nil {
				return e
			}
			_, e = tx.Exec(ctx, "INSERT INTO kaflux_jobs(id,cluster_id,state,payload) VALUES($1,$2,$3,$4)", rollback.ID, rollback.ClusterID, rollback.State, b)
			return e
		})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.leases[finished.ID]
	if l.Owner != owner || time.Now().After(l.Until) {
		return errors.New("worker lease lost")
	}
	for id, j := range s.jobs {
		if id != finished.ID && j.ClusterID == rollback.ClusterID && model.IsActiveJobState(j.State) {
			return errors.New("cluster has active job")
		}
	}
	s.jobs[finished.ID] = finished
	s.jobs[rollback.ID] = rollback
	return nil
}

// mutateJob applies change to the stored job under a row lock and returns the result.
func (s *Store) mutateJob(ctx context.Context, id string, change func(*model.Plan) error) (model.Plan, error) {
	var out model.Plan
	if s.sqlite != nil {
		e := s.sqlite.changeJob(ctx, id, func(p *model.Plan, _ lease) error {
			if e := change(p); e != nil {
				return e
			}
			out = *p
			return nil
		})
		return out, e
	}
	if s.DB != nil {
		e := pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
			var b []byte
			if e := tx.QueryRow(ctx, "SELECT payload FROM kaflux_jobs WHERE id=$1 FOR UPDATE", id).Scan(&b); e != nil {
				return e
			}
			if e := unmarshalPlan(b, &out); e != nil {
				return e
			}
			if e := change(&out); e != nil {
				return e
			}
			b, e := marshalPlan(out)
			if e != nil {
				return e
			}
			_, e = tx.Exec(ctx, "UPDATE kaflux_jobs SET state=$2,payload=$3 WHERE id=$1", id, out.State, b)
			return e
		})
		return out, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.jobs[id]
	if !ok {
		return out, errors.New("job not found")
	}
	if e := change(&p); e != nil {
		return out, e
	}
	s.jobs[id] = p
	return p, nil
}
