package store

import (
	"context"
	"errors"
	"github.com/hawxxx/kaflux/backend/internal/model"
)

// RequestCancel preserves the request independently of an older worker's payload.
func (s *Store) RequestCancel(ctx context.Context, id string) error {
	if s.sqlite != nil {
		return s.sqlite.changeJob(ctx, id, func(p *model.Plan, _ lease) error {
			if p.State != "queued" && p.State != "running" {
				return errors.New("only active jobs can be canceled")
			}
			p.CancellationRequested = true
			return nil
		})
	}
	if s.DB != nil {
		result, err := s.DB.Exec(ctx, `UPDATE kaflux_jobs SET payload=jsonb_set(payload,'{cancellationRequested}','true'::jsonb) WHERE id=$1 AND state IN ('queued','running')`, id)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return errors.New("only active jobs can be canceled")
		}
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	plan, exists := s.jobs[id]
	if !exists || (plan.State != "queued" && plan.State != "running") {
		return errors.New("only active jobs can be canceled")
	}
	plan.CancellationRequested = true
	s.jobs[id] = plan
	return nil
}
