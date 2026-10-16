package store

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/hawxxx/kaflux/backend/internal/model"
)

// The active states are bound as one parameter so the statements stay constant:
// a JSON array read through json_each on SQLite, a text[] on PostgreSQL.
const (
	deleteIdleJobSQLite   = "DELETE FROM kaflux_jobs WHERE id=? AND state NOT IN (SELECT value FROM json_each(?))"
	deleteIdleJobPostgres = "DELETE FROM kaflux_jobs WHERE id=$1 AND state <> ALL($2::text[])"
)

var ErrJobActive = errors.New("active reassignments cannot be deleted")

// DeleteJob removes a plan and its activity log only while no worker owns it;
// the state check and delete are atomic.
func (s *Store) DeleteJob(ctx context.Context, id string) error {
	if s.sqlite != nil {
		active, e := json.Marshal(model.ActiveJobStates)
		if e != nil {
			return e
		}
		tx, e := s.sqlite.db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		defer tx.Rollback()
		r, e := tx.ExecContext(ctx, deleteIdleJobSQLite, id, string(active))
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
		r, e := tx.Exec(ctx, deleteIdleJobPostgres, id, model.ActiveJobStates)
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
