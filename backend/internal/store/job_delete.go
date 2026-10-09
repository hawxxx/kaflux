package store

import (
	"context"
	"errors"
	"strings"

	"github.com/hawxxx/kaflux/backend/internal/model"
)

// activeStatesSQL is model.ActiveJobStates as a SQL list.
var activeStatesSQL = "('" + strings.Join(model.ActiveJobStates, "','") + "')"

var ErrJobActive = errors.New("active reassignments cannot be deleted")

// DeleteJob removes a plan and its activity log only while no worker owns it;
// the state check and delete are atomic.
func (s *Store) DeleteJob(ctx context.Context, id string) error {
	if s.sqlite != nil {
		tx, e := s.sqlite.db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		defer tx.Rollback()
		r, e := tx.ExecContext(ctx, "DELETE FROM kaflux_jobs WHERE id=? AND state NOT IN "+activeStatesSQL, id)
		if e != nil {
			return e
		}
		if n, e := r.RowsAffected(); e != nil || n != 1 {
			return ErrJobActive
		}
		if _, e = tx.ExecContext(ctx, "DELETE FROM kaflux_job_events WHERE job_id=?", id); e != nil {
			return e
		}
		return tx.Commit()
	}
	if s.DB != nil {
		tx, e := s.DB.Begin(ctx)
		if e != nil {
			return e
		}
		defer tx.Rollback(ctx)
		r, e := tx.Exec(ctx, "DELETE FROM kaflux_jobs WHERE id=$1 AND state NOT IN "+activeStatesSQL, id)
		if e != nil {
			return e
		}
		if r.RowsAffected() != 1 {
			return ErrJobActive
		}
		if _, e = tx.Exec(ctx, "DELETE FROM kaflux_job_events WHERE job_id=$1", id); e != nil {
			return e
		}
		return tx.Commit(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.jobs[id]
	if !ok || model.IsActiveJobState(p.State) {
		return ErrJobActive
	}
	delete(s.jobs, id)
	delete(s.leases, id)
	delete(s.events, id)
	return nil
}
