package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/jackc/pgx/v5"
	"sort"
	"time"
)

var ErrSessionNotFound = errors.New("session not found")

type SessionSummary struct {
	Handle   string    `json:"handle"`
	UserID   string    `json:"userId"`
	Provider string    `json:"provider"`
	Roles    []string  `json:"roles"`
	Expires  time.Time `json:"expires"`
	Current  bool      `json:"current"`
}

func SessionHandle(id string) string { h := sha256.Sum256([]byte(id)); return hex.EncodeToString(h[:]) }
func SummarizeSession(v auth.Session, current string) SessionSummary {
	return SessionSummary{Handle: SessionHandle(v.ID), UserID: v.User.ID, Provider: v.User.Provider, Roles: append([]string{}, v.User.Roles...), Expires: v.Expires, Current: v.ID == current}
}

// Session IDs issued by auth.Token are ASCII hex, so the indexed PostgreSQL
// id::bytea digest is byte-for-byte identical to SessionHandle.
// ListSessions bounds both the database result and memory retained for pagination.
func (s *Store) ListSessions(ctx context.Context, current string, limit, offset int) ([]SessionSummary, bool, error) {
	if s.sqlite != nil {
		return s.sqlite.listSessions(ctx, current, limit, offset)
	}
	if limit < 1 || limit > 100 || offset < 0 || offset > 10000 {
		return nil, false, fmt.Errorf("invalid session pagination")
	}
	out := []SessionSummary{}
	if s.DB != nil {
		rows, e := s.DB.Query(ctx, `SELECT encode(sha256(id::bytea),'hex'),payload->'user',expires,id=$1 FROM kaflux_sessions WHERE expires>now() ORDER BY expires,encode(sha256(id::bytea),'hex') LIMIT $2 OFFSET $3`, current, limit+1, offset)
		if e != nil {
			return nil, false, e
		}
		defer rows.Close()
		for rows.Next() {
			var v SessionSummary
			var b []byte
			if e = rows.Scan(&v.Handle, &b, &v.Expires, &v.Current); e != nil {
				return nil, false, e
			}
			var u auth.User
			if e = json.Unmarshal(b, &u); e != nil {
				return nil, false, e
			}
			v.UserID = u.ID
			v.Provider = u.Provider
			v.Roles = append([]string{}, u.Roles...)
			out = append(out, v)
		}
		if e = rows.Err(); e != nil {
			return nil, false, e
		}
	} else {
		s.mu.Lock()
		defer s.mu.Unlock()
		now := time.Now()
		capSize := offset + limit + 1
		for _, v := range s.sessions {
			if !v.ValidAt(now) {
				continue
			}
			item := SummarizeSession(v, current)
			i := sort.Search(len(out), func(i int) bool { return !sessionLess(out[i], item) })
			if i >= capSize {
				continue
			}
			out = append(out, SessionSummary{})
			copy(out[i+1:], out[i:])
			out[i] = item
			if len(out) > capSize {
				out = out[:capSize]
			}
		}
		if offset >= len(out) {
			return []SessionSummary{}, false, nil
		}
		out = out[offset:]
	}
	more := len(out) > limit
	if more {
		out = out[:limit]
	}
	return out, more, nil
}
func sessionLess(a, b SessionSummary) bool {
	if a.Expires.Equal(b.Expires) {
		return a.Handle < b.Handle
	}
	return a.Expires.Before(b.Expires)
}

// RevokeSession atomically persists intent and completion alongside deletion.
// Authorization executes while the target row (or memory store) is locked.
func (s *Store) RevokeSession(ctx context.Context, handle, current string, authorize func(SessionSummary) bool, audit Audit) (SessionSummary, error) {
	if s.sqlite != nil {
		return s.sqlite.revokeSession(ctx, handle, current, authorize, audit)
	}
	if s.DB != nil {
		tx, e := s.DB.Begin(ctx)
		if e != nil {
			return SessionSummary{}, e
		}
		defer tx.Rollback(context.Background())
		var id string
		var b []byte
		var expiry time.Time
		e = tx.QueryRow(ctx, `SELECT id,payload->'user',expires FROM kaflux_sessions WHERE encode(sha256(id::bytea),'hex')=$1 AND expires>now() FOR UPDATE`, handle).Scan(&id, &b, &expiry)
		if errors.Is(e, pgx.ErrNoRows) {
			return SessionSummary{}, ErrSessionNotFound
		}
		if e != nil {
			return SessionSummary{}, e
		}
		var u auth.User
		if e = json.Unmarshal(b, &u); e != nil {
			return SessionSummary{}, e
		}
		v := SummarizeSession(auth.Session{ID: id, User: u, Expires: expiry}, current)
		if !authorize(v) {
			return v, ErrSessionForbidden
		}
		for _, result := range []string{"intent", "success"} {
			a := sessionRevokeAudit(audit, v, result)
			payload, e := json.Marshal(a)
			if e != nil {
				return v, e
			}
			if _, e = tx.Exec(ctx, `INSERT INTO kaflux_audit VALUES($1,$2,$3)`, a.ID, payload, a.At); e != nil {
				return v, e
			}
		}
		if _, e = tx.Exec(ctx, `DELETE FROM kaflux_sessions WHERE id=$1`, id); e != nil {
			return v, e
		}
		return v, tx.Commit(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, session := range s.sessions {
		if SessionHandle(id) != handle || !session.ValidAt(time.Now()) {
			continue
		}
		v := SummarizeSession(session, current)
		if !authorize(v) {
			return v, ErrSessionForbidden
		}
		if e := ctx.Err(); e != nil {
			return v, e
		}
		s.audit = append(s.audit, sessionRevokeAudit(audit, v, "intent"), sessionRevokeAudit(audit, v, "success"))
		if len(s.audit) > 1000 {
			s.audit = s.audit[len(s.audit)-1000:]
		}
		delete(s.sessions, id)
		return v, nil
	}
	return SessionSummary{}, ErrSessionNotFound
}

var ErrSessionForbidden = errors.New("session permission denied")

func sessionRevokeAudit(a Audit, v SessionSummary, result string) Audit {
	a.ID = auth.Token()
	a.At = time.Now().UTC()
	a.Result = result
	a.Resource = v.UserID
	a.AdminBefore = v
	a.AdminAfter = map[string]any{"handle": v.Handle, "revoked": true}
	return a
}
