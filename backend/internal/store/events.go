package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/jackc/pgx/v5"
)

// MaxJobEvents is how many of the newest activity log lines a job keeps.
const MaxJobEvents = 2000

// AppendEvent adds one line to a job's activity log and trims the oldest
// lines beyond MaxJobEvents. The API and the worker may append concurrently,
// so the sequence number is taken inside the insert and retried on conflict.
func (s *Store) AppendEvent(ctx context.Context, jobID, level, message string) error {
	at := time.Now().UTC()
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if err = s.appendEvent(ctx, jobID, level, message, at); err == nil || !isConflict(err) {
			return err
		}
	}
	return err
}

func (s *Store) appendEvent(ctx context.Context, jobID, level, message string, at time.Time) error {
	if s.sqlite != nil {
		tx, e := s.sqlite.db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		defer tx.Rollback()
		if _, e = tx.ExecContext(ctx, "INSERT INTO kaflux_job_events(job_id,seq,at,level,message) SELECT ?,COALESCE(MAX(seq),0)+1,?,?,? FROM kaflux_job_events WHERE job_id=?", jobID, at.UnixNano(), level, message, jobID); e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, "DELETE FROM kaflux_job_events WHERE job_id=? AND seq<=(SELECT MAX(seq) FROM kaflux_job_events WHERE job_id=?)-?", jobID, jobID, MaxJobEvents); e != nil {
			return e
		}
		return tx.Commit()
	}
	if s.DB != nil {
		return pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
			if _, e := tx.Exec(ctx, "INSERT INTO kaflux_job_events(job_id,seq,at,level,message) SELECT $1,COALESCE(MAX(seq),0)+1,$2,$3,$4 FROM kaflux_job_events WHERE job_id=$1", jobID, at, level, message); e != nil {
				return e
			}
			_, e := tx.Exec(ctx, "DELETE FROM kaflux_job_events WHERE job_id=$1 AND seq<=(SELECT MAX(seq) FROM kaflux_job_events WHERE job_id=$1)-$2", jobID, MaxJobEvents)
			return e
		})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.events == nil {
		s.events = map[string][]model.JobEvent{}
	}
	log := s.events[jobID]
	seq := int64(1)
	if len(log) > 0 {
		seq = log[len(log)-1].Seq + 1
	}
	log = append(log, model.JobEvent{Seq: seq, At: at, Level: level, Message: message})
	if len(log) > MaxJobEvents {
		log = append([]model.JobEvent(nil), log[len(log)-MaxJobEvents:]...)
	}
	s.events[jobID] = log
	return nil
}

// Events returns up to limit log lines with a sequence number above after, oldest first.
func (s *Store) Events(ctx context.Context, jobID string, after int64, limit int) ([]model.JobEvent, error) {
	out := []model.JobEvent{}
	if s.sqlite != nil {
		r, e := s.sqlite.db.QueryContext(ctx, "SELECT seq,at,level,message FROM kaflux_job_events WHERE job_id=? AND seq>? ORDER BY seq LIMIT ?", jobID, after, limit)
		if e != nil {
			return nil, e
		}
		defer r.Close()
		for r.Next() {
			var ev model.JobEvent
			var at int64
			if e = r.Scan(&ev.Seq, &at, &ev.Level, &ev.Message); e != nil {
				return nil, e
			}
			ev.At = time.Unix(0, at).UTC()
			out = append(out, ev)
		}
		return out, r.Err()
	}
	if s.DB != nil {
		r, e := s.DB.Query(ctx, "SELECT seq,at,level,message FROM kaflux_job_events WHERE job_id=$1 AND seq>$2 ORDER BY seq LIMIT $3", jobID, after, limit)
		if e != nil {
			return nil, e
		}
		defer r.Close()
		for r.Next() {
			var ev model.JobEvent
			if e = r.Scan(&ev.Seq, &ev.At, &ev.Level, &ev.Message); e != nil {
				return nil, e
			}
			ev.At = ev.At.UTC()
			out = append(out, ev)
		}
		return out, r.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range s.events[jobID] {
		if ev.Seq > after && len(out) < limit {
			out = append(out, ev)
		}
	}
	return out, nil
}

func isConflict(e error) bool {
	var coded interface{ SQLState() string }
	if errors.As(e, &coded) {
		return coded.SQLState() == "23505"
	}
	// SQLite reports constraint failures in the message.
	return e != nil && (strings.Contains(e.Error(), "UNIQUE constraint failed") || strings.Contains(e.Error(), "SQLITE_BUSY") || strings.Contains(e.Error(), "database is locked"))
}
