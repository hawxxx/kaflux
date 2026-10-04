package store

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"time"
)

var ErrThrottleConflict = errors.New("job is not running, has a pending request, or is being canceled")

func (s *Store) RequestThrottle(ctx context.Context, id string, request model.ThrottleRequest) error {
	if s.sqlite != nil {
		return s.sqlite.requestThrottle(ctx, id, request)
	}
	if request.BytesPerSec < 1 || request.BytesPerSec > 1000000000000 || request.Revision == "" || request.Actor == "" {
		return errors.New("invalid throttle request")
	}
	if s.DB != nil {
		payload, e := json.Marshal(request)
		if e != nil {
			return e
		}
		result, e := s.DB.Exec(ctx, `UPDATE kaflux_jobs SET payload=jsonb_set(jsonb_set(payload,'{throttleRequest}',$2::jsonb),'{throttleError}','""'::jsonb) WHERE id=$1 AND state='running' AND NOT COALESCE((payload->>'cleanupPending')::boolean,false) AND NOT COALESCE((payload->>'cancellationRequested')::boolean,false) AND (payload->'throttleRequest' IS NULL OR payload->'throttleRequest'='null'::jsonb)`, id, payload)
		if e != nil {
			return e
		}
		if result.RowsAffected() != 1 {
			return ErrThrottleConflict
		}
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.jobs[id]
	if !ok || p.State != "running" || p.CleanupPending || p.CancellationRequested || p.ThrottleRequest != nil {
		return ErrThrottleConflict
	}
	p.ThrottleRequest = &request
	p.ThrottleError = ""
	s.jobs[id] = p
	return nil
}

func (s *Store) AcknowledgeThrottle(ctx context.Context, id, owner, revision string, rate int64) error {
	if s.sqlite != nil {
		return s.sqlite.acknowledgeThrottle(ctx, id, owner, revision, rate)
	}
	if rate < 1 || rate > 1000000000000 {
		return errors.New("invalid applied throttle")
	}
	if s.DB != nil {
		result, e := s.DB.Exec(ctx, `UPDATE kaflux_jobs SET payload=jsonb_set(jsonb_set(jsonb_set(payload,'{throttleRequest}','null'::jsonb),'{throttleBytesPerSec}',to_jsonb($4::bigint)),'{throttleError}','""'::jsonb) WHERE id=$1 AND state='running' AND lease_owner=$2 AND lease_until>now() AND payload->'throttleRequest'->>'revision'=$3 AND (payload->'throttleRequest'->>'bytesPerSec')::bigint=$4`, id, owner, revision, rate)
		if e != nil {
			return e
		}
		if result.RowsAffected() != 1 {
			return errors.New("worker lease or throttle revision lost")
		}
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.jobs[id]
	lease := s.leases[id]
	if !ok || p.State != "running" || lease.Owner != owner || time.Now().After(lease.Until) || p.ThrottleRequest == nil || p.ThrottleRequest.Revision != revision || p.ThrottleRequest.BytesPerSec != rate {
		return errors.New("worker lease or throttle revision lost")
	}
	p.ThrottleRequest = nil
	p.ThrottleError = ""
	p.ThrottleBytesPerSec = rate
	s.jobs[id] = p
	return nil
}
