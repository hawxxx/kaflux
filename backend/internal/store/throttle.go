package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/jackc/pgx/v5"
)

func (s *Store) SaveThrottle(ctx context.Context, jobID string, records []kafka.ThrottleRecord) error {
	if s.sqlite != nil {
		return s.sqlite.saveThrottle(ctx, jobID, records)
	}
	if s.DB == nil {
		return errors.New("durable throttle ownership requires PostgreSQL")
	}
	payload, err := json.Marshal(records)
	if err != nil {
		return err
	}
	// Capture once: retries must not replace the original pre-operation configuration.
	_, err = s.DB.Exec(ctx, "INSERT INTO kaflux_job_throttles(job_id,payload) VALUES($1,$2) ON CONFLICT(job_id) DO NOTHING", jobID, payload)
	return err
}

// ReplaceThrottle commits ownership transitions under the same row lock used
// by cancellation and lease acquisition. Original settings cannot be changed.
func (s *Store) ReplaceThrottle(ctx context.Context, jobID, owner, revision string, records []kafka.ThrottleRecord) error {
	if s.sqlite != nil {
		return s.sqlite.replaceThrottle(ctx, jobID, owner, revision, records)
	}
	if s.DB == nil {
		return errors.New("durable throttle ownership requires PostgreSQL")
	}
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return e
	}
	defer func() {
		bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(bounded)
	}()
	var jobPayload []byte
	e = tx.QueryRow(ctx, `SELECT payload FROM kaflux_jobs WHERE id=$1 AND state='running' AND lease_owner=$2 AND lease_until>now() AND NOT COALESCE((payload->>'cleanupPending')::boolean,false) AND NOT COALESCE((payload->>'cancellationRequested')::boolean,false) AND payload->'throttleRequest'->>'revision'=$3 FOR UPDATE`, jobID, owner, revision).Scan(&jobPayload)
	if e != nil {
		return errors.New("worker lease, pending throttle revision or active job lost")
	}
	var payload []byte
	if e = tx.QueryRow(ctx, "SELECT payload FROM kaflux_job_throttles WHERE job_id=$1 FOR UPDATE", jobID).Scan(&payload); e != nil {
		return e
	}
	var old []kafka.ThrottleRecord
	if e = json.Unmarshal(payload, &old); e != nil {
		return e
	}
	if len(old) != len(records) {
		return errors.New("throttle ownership resources cannot change")
	}
	for i, r := range records {
		previous := old[i]
		if previous.Resource != r.Resource || previous.Name != r.Name || previous.Key != r.Key || previous.EffectiveBefore != r.EffectiveBefore || !reflect.DeepEqual(previous.Previous, r.Previous) {
			return errors.New("original throttle configuration cannot change")
		}
	}
	payload, e = json.Marshal(records)
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "UPDATE kaflux_job_throttles SET payload=$2 WHERE job_id=$1", jobID, payload); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

func (s *Store) LoadThrottle(ctx context.Context, jobID string) ([]kafka.ThrottleRecord, error) {
	if s.sqlite != nil {
		return s.sqlite.loadThrottle(ctx, jobID)
	}
	if s.DB == nil {
		return nil, nil
	}
	var payload []byte
	err := s.DB.QueryRow(ctx, "SELECT payload FROM kaflux_job_throttles WHERE job_id=$1", jobID).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var records []kafka.ThrottleRecord
	err = json.Unmarshal(payload, &records)
	return records, err
}

func (s *Store) DeleteThrottle(ctx context.Context, jobID string) error {
	if s.sqlite != nil {
		_, e := s.sqlite.db.ExecContext(ctx, "DELETE FROM kaflux_job_throttles WHERE job_id=?", jobID)
		return e
	}
	if s.DB == nil {
		return nil
	}
	_, err := s.DB.Exec(ctx, "DELETE FROM kaflux_job_throttles WHERE job_id=$1", jobID)
	return err
}
