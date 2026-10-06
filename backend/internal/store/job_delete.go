package store

import (
	"context"
	"errors"
)

var ErrJobActive = errors.New("active reassignments cannot be deleted")

// DeleteJob removes a plan only while no worker owns it; the state check and delete are atomic.
func (s *Store) DeleteJob(ctx context.Context, id string) error {
	if s.sqlite != nil {
		r, e := s.sqlite.db.ExecContext(ctx, "DELETE FROM kaflux_jobs WHERE id=? AND state NOT IN ('queued','running','rollback-queued')", id)
		if e != nil {
			return e
		}
		if n, e := r.RowsAffected(); e != nil || n != 1 {
			return ErrJobActive
		}
		return nil
	}
	if s.DB != nil {
		r, e := s.DB.Exec(ctx, "DELETE FROM kaflux_jobs WHERE id=$1 AND state NOT IN ('queued','running','rollback-queued')", id)
		if e != nil {
			return e
		}
		if r.RowsAffected() != 1 {
			return ErrJobActive
		}
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.jobs[id]
	if !ok || p.State == "queued" || p.State == "running" || p.State == "rollback-queued" {
		return ErrJobActive
	}
	delete(s.jobs, id)
	delete(s.leases, id)
	return nil
}
