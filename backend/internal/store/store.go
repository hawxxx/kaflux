package store

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"time"
)

type Audit struct {
	ID          string               `json:"id"`
	Actor       string               `json:"actor"`
	Action      string               `json:"action"`
	Resource    string               `json:"resource"`
	Result      string               `json:"result"`
	RequestID   string               `json:"requestId"`
	SourceIP    string               `json:"sourceIp"`
	At          time.Time            `json:"at"`
	ClusterID   string               `json:"clusterId"`
	Provider    string               `json:"provider"`
	Before      []model.Distribution `json:"before,omitempty"`
	After       []model.Distribution `json:"after,omitempty"`
	Changes     []model.Change       `json:"changes,omitempty"`
	AdminBefore any                  `json:"adminBefore,omitempty"`
	AdminAfter  any                  `json:"adminAfter,omitempty"`
}
type Store struct {
	DB       *pgxpool.Pool
	sqlite   *sqliteStore
	mu       sync.Mutex
	sessions map[string]auth.Session
	jobs     map[string]model.Plan
	audit    []Audit
	leases   map[string]lease
	names    map[string]string
}
type lease struct {
	Owner string
	Until time.Time
}

func New(ctx context.Context, url string) (*Store, error) {
	s := &Store{sessions: map[string]auth.Session{}, jobs: map[string]model.Plan{}, audit: []Audit{}, leases: map[string]lease{}}
	if url == "" {
		return s, nil
	}
	db, e := pgxpool.New(ctx, url)
	if e != nil {
		return nil, e
	}
	s.DB = db
	if e = db.Ping(ctx); e != nil {
		db.Close()
		return nil, e
	}
	_, e = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS kaflux_sessions (id text PRIMARY KEY, payload jsonb NOT NULL, expires timestamptz NOT NULL);CREATE INDEX IF NOT EXISTS kaflux_sessions_handle ON kaflux_sessions ((encode(sha256(id::bytea),'hex')));CREATE INDEX IF NOT EXISTS kaflux_sessions_inventory ON kaflux_sessions (expires,(encode(sha256(id::bytea),'hex')));CREATE TABLE IF NOT EXISTS kaflux_jobs (id text PRIMARY KEY, cluster_id text NOT NULL, state text NOT NULL, payload jsonb NOT NULL, lease_until timestamptz, lease_owner text);CREATE TABLE IF NOT EXISTS kaflux_audit (id text PRIMARY KEY, payload jsonb NOT NULL, created_at timestamptz NOT NULL);CREATE TABLE IF NOT EXISTS kaflux_job_throttles(job_id text primary key,payload jsonb not null);CREATE TABLE IF NOT EXISTS kaflux_cluster_names(cluster_id text PRIMARY KEY, name text NOT NULL);CREATE UNIQUE INDEX IF NOT EXISTS kaflux_one_active_job ON kaflux_jobs(cluster_id) WHERE state IN ('queued','running','rollback-queued');`)
	if e != nil {
		db.Close()
		return nil, e
	}
	return s, nil
}
func (s *Store) Close() {
	if s.sqlite != nil {
		s.sqlite.close()
		return
	}
	if s.DB != nil {
		s.DB.Close()
	}
}
func (s *Store) Session(ctx context.Context, id string) (auth.Session, error) {
	if s.sqlite != nil {
		return s.sqlite.session(ctx, id)
	}
	var v auth.Session
	if s.DB != nil {
		var b []byte
		e := s.DB.QueryRow(ctx, "SELECT payload FROM kaflux_sessions WHERE id=$1 AND expires>now()", id).Scan(&b)
		if e != nil {
			return v, e
		}
		e = json.Unmarshal(b, &v)
		v.ID = id
		return v, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.sessions[id]
	if !ok || !v.ValidAt(time.Now()) {
		return v, fmt.Errorf("session expired")
	}
	return v, nil
}
func (s *Store) SaveSession(ctx context.Context, v auth.Session) error {
	if s.sqlite != nil {
		return s.sqlite.saveSession(ctx, v)
	}
	if s.DB != nil {
		b, _ := json.Marshal(v)
		_, e := s.DB.Exec(ctx, "INSERT INTO kaflux_sessions VALUES($1,$2,$3)", v.ID, b, v.Expires)
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[v.ID] = v
	return nil
}
func (s *Store) DeleteSession(ctx context.Context, id string) error {
	if s.sqlite != nil {
		_, e := s.sqlite.db.ExecContext(ctx, "DELETE FROM kaflux_sessions WHERE id=?", id)
		return e
	}
	if s.DB != nil {
		_, e := s.DB.Exec(ctx, "DELETE FROM kaflux_sessions WHERE id=$1", id)
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
	return nil
}
func (s *Store) SaveJob(ctx context.Context, p model.Plan) error {
	if s.sqlite != nil {
		return s.sqlite.saveJob(ctx, p)
	}
	if s.DB != nil {
		b, _ := json.Marshal(p)
		_, e := s.DB.Exec(ctx, "INSERT INTO kaflux_jobs(id,cluster_id,state,payload) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO UPDATE SET state=EXCLUDED.state,payload=EXCLUDED.payload", p.ID, p.ClusterID, p.State, b)
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, j := range s.jobs {
		if id != p.ID && j.ClusterID == p.ClusterID && (j.State == "queued" || j.State == "running") {
			return fmt.Errorf("cluster already has an active job")
		}
	}
	s.jobs[p.ID] = p
	return nil
}
func (s *Store) Job(ctx context.Context, id string) (model.Plan, error) {
	if s.sqlite != nil {
		return s.sqlite.job(ctx, id)
	}
	if s.DB != nil {
		var b []byte
		e := s.DB.QueryRow(ctx, "SELECT payload FROM kaflux_jobs WHERE id=$1", id).Scan(&b)
		var p model.Plan
		if e == nil {
			e = json.Unmarshal(b, &p)
		}
		return p, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.jobs[id]
	if !ok {
		return p, fmt.Errorf("job not found")
	}
	b, _ := json.Marshal(p)
	_ = json.Unmarshal(b, &p)
	return p, nil
}
func (s *Store) Jobs(ctx context.Context) ([]model.Plan, error) {
	if s.sqlite != nil {
		return s.sqlite.jobs(ctx, false)
	}
	out := []model.Plan{}
	if s.DB != nil {
		r, e := s.DB.Query(ctx, "SELECT payload FROM kaflux_jobs ORDER BY payload->>'createdAt' DESC LIMIT 500")
		if e != nil {
			return nil, e
		}
		defer r.Close()
		for r.Next() {
			var b []byte
			var p model.Plan
			if e = r.Scan(&b); e != nil {
				return nil, e
			}
			if e = json.Unmarshal(b, &p); e != nil {
				return nil, e
			}
			out = append(out, p)
		}
		return out, r.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.jobs {
		out = append(out, p)
	}
	return out, nil
}
func (s *Store) ActiveJobs(ctx context.Context) ([]model.Plan, error) {
	if s.sqlite != nil {
		return s.sqlite.jobs(ctx, true)
	}
	if s.DB == nil {
		all, e := s.Jobs(ctx)
		if e != nil {
			return nil, e
		}
		out := []model.Plan{}
		for _, p := range all {
			if p.State == "queued" || p.State == "running" {
				out = append(out, p)
			}
		}
		return out, nil
	}
	rows, e := s.DB.Query(ctx, "SELECT payload FROM kaflux_jobs WHERE state IN ('queued','running') ORDER BY lease_until NULLS FIRST LIMIT 200")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []model.Plan{}
	for rows.Next() {
		var b []byte
		var p model.Plan
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(b, &p); e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *Store) Claim(ctx context.Context, id, owner string) bool {
	if s.sqlite != nil {
		return s.sqlite.claim(ctx, id, owner)
	}
	if s.DB == nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		p, ok := s.jobs[id]
		if !ok || (p.State != "queued" && p.State != "running") {
			return false
		}
		l := s.leases[id]
		if time.Now().Before(l.Until) && l.Owner != owner {
			return false
		}
		s.leases[id] = lease{Owner: owner, Until: time.Now().Add(30 * time.Second)}
		return true
	}
	r, e := s.DB.Exec(ctx, "UPDATE kaflux_jobs SET lease_owner=$2,lease_until=now()+interval '30 seconds' WHERE id=$1 AND state IN ('queued','running') AND (lease_until IS NULL OR lease_until<now() OR lease_owner=$2)", id, owner)
	return e == nil && r.RowsAffected() == 1
}
func (s *Store) SaveClaimedJob(ctx context.Context, p model.Plan, owner string) error {
	if s.sqlite != nil {
		return s.sqlite.changeJob(ctx, p.ID, func(old *model.Plan, l lease) error {
			if l.Owner != owner || !time.Now().Before(l.Until) {
				return fmt.Errorf("worker lease lost")
			}
			p.CancellationRequested = old.CancellationRequested
			if p.ThrottleRequest == nil || old.ThrottleRequest == nil || p.ThrottleRequest.Revision != old.ThrottleRequest.Revision {
				p.ThrottleError = old.ThrottleError
			}
			p.ThrottleRequest = old.ThrottleRequest
			p.ThrottleBytesPerSec = old.ThrottleBytesPerSec
			if p.State != "running" && p.State != "queued" {
				p.ThrottleRequest = nil
			}
			*old = p
			return nil
		})
	}
	if s.DB != nil {
		b, _ := json.Marshal(p)
		r, e := s.DB.Exec(ctx, `UPDATE kaflux_jobs SET state=$2,payload=$3::jsonb || jsonb_build_object(
 'cancellationRequested',COALESCE((payload->>'cancellationRequested')::boolean,false),
 'throttleRequest',CASE WHEN $2 IN ('queued','running') THEN payload->'throttleRequest' ELSE 'null'::jsonb END,
 'throttleBytesPerSec',payload->'throttleBytesPerSec',
 'throttleError',CASE WHEN ($3::jsonb->'throttleRequest'->>'revision')=(payload->'throttleRequest'->>'revision') THEN COALESCE($3::jsonb->'throttleError','""'::jsonb) ELSE COALESCE(payload->'throttleError','""'::jsonb) END
 ) WHERE id=$1 AND lease_owner=$4 AND lease_until>now()`, p.ID, p.State, b, owner)
		if e != nil {
			return e
		}
		if r.RowsAffected() != 1 {
			return fmt.Errorf("worker lease lost")
		}
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.leases[p.ID]
	if l.Owner != owner || time.Now().After(l.Until) {
		return fmt.Errorf("worker lease lost")
	}
	if existing, ok := s.jobs[p.ID]; ok {
		p.CancellationRequested = existing.CancellationRequested
		if p.ThrottleRequest == nil || existing.ThrottleRequest == nil || p.ThrottleRequest.Revision != existing.ThrottleRequest.Revision {
			p.ThrottleError = existing.ThrottleError
		}
		p.ThrottleRequest = existing.ThrottleRequest
		p.ThrottleBytesPerSec = existing.ThrottleBytesPerSec
		if p.State != "running" && p.State != "queued" {
			p.ThrottleRequest = nil
		}
	}
	s.jobs[p.ID] = p
	return nil
}
func (s *Store) QueueJob(ctx context.Context, p model.Plan) error {
	if s.sqlite != nil {
		return s.sqlite.changeJob(ctx, p.ID, func(old *model.Plan, _ lease) error {
			if old.State != "planned" {
				return fmt.Errorf("plan was already approved")
			}
			*old = p
			return nil
		})
	}
	if s.DB != nil {
		b, _ := json.Marshal(p)
		r, e := s.DB.Exec(ctx, "UPDATE kaflux_jobs SET state='queued',payload=$2 WHERE id=$1 AND state='planned'", p.ID, b)
		if e != nil {
			return e
		}
		if r.RowsAffected() != 1 {
			return fmt.Errorf("plan was already approved")
		}
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.jobs[p.ID]
	if !ok || old.State != "planned" {
		return fmt.Errorf("plan was already approved")
	}
	for id, j := range s.jobs {
		if id != p.ID && j.ClusterID == p.ClusterID && (j.State == "queued" || j.State == "running") {
			return fmt.Errorf("cluster has active job")
		}
	}
	s.jobs[p.ID] = p
	return nil
}
func (s *Store) Audit(ctx context.Context, a Audit) error {
	if s.sqlite != nil {
		return s.sqlite.audit(ctx, a)
	}
	a.ID = auth.Token()
	a.At = time.Now().UTC()
	if s.DB != nil {
		b, _ := json.Marshal(a)
		_, e := s.DB.Exec(ctx, "INSERT INTO kaflux_audit VALUES($1,$2,$3)", a.ID, b, a.At)
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audit = append(s.audit, a)
	if len(s.audit) > 1000 {
		s.audit = s.audit[len(s.audit)-1000:]
	}
	return nil
}

// ClusterNames returns display-name overrides keyed by cluster ID.
func (s *Store) ClusterNames(ctx context.Context) (map[string]string, error) {
	if s.sqlite != nil {
		return s.sqlite.clusterNames(ctx)
	}
	out := map[string]string{}
	if s.DB != nil {
		r, e := s.DB.Query(ctx, "SELECT cluster_id,name FROM kaflux_cluster_names")
		if e != nil {
			return nil, e
		}
		defer r.Close()
		for r.Next() {
			var id, name string
			if e = r.Scan(&id, &name); e != nil {
				return nil, e
			}
			out[id] = name
		}
		return out, r.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, name := range s.names {
		out[id] = name
	}
	return out, nil
}

// SetClusterName stores a display-name override; an empty name removes it.
func (s *Store) SetClusterName(ctx context.Context, id, name string) error {
	if s.sqlite != nil {
		return s.sqlite.setClusterName(ctx, id, name)
	}
	if s.DB != nil {
		var e error
		if name == "" {
			_, e = s.DB.Exec(ctx, "DELETE FROM kaflux_cluster_names WHERE cluster_id=$1", id)
		} else {
			_, e = s.DB.Exec(ctx, "INSERT INTO kaflux_cluster_names VALUES($1,$2) ON CONFLICT(cluster_id) DO UPDATE SET name=EXCLUDED.name", id, name)
		}
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if name == "" {
		delete(s.names, id)
		return nil
	}
	if s.names == nil {
		s.names = map[string]string{}
	}
	s.names[id] = name
	return nil
}
func (s *Store) Audits(ctx context.Context) ([]Audit, error) {
	if s.sqlite != nil {
		return s.sqlite.audits(ctx)
	}
	if s.DB != nil {
		r, e := s.DB.Query(ctx, "SELECT payload FROM kaflux_audit ORDER BY created_at DESC LIMIT 200")
		if e != nil {
			return nil, e
		}
		defer r.Close()
		out := []Audit{}
		for r.Next() {
			var b []byte
			var a Audit
			if e = r.Scan(&b); e != nil {
				return nil, e
			}
			if e = json.Unmarshal(b, &a); e != nil {
				return nil, e
			}
			out = append(out, a)
		}
		return out, r.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Audit{}, s.audit...), nil
}
