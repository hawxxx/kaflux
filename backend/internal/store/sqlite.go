package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	_ "modernc.org/sqlite"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"
)

// SQLite is local, durable single-instance storage. The lock must reside on the
// same local persistent volume as the database; network/shared files are unsupported.
type sqliteStore struct {
	db   *sql.DB
	lock *flock.Flock
	once sync.Once
}

func NewSQLite(ctx context.Context, path string) (*Store, error) {
	if path == "" || path == ":memory:" {
		return nil, errors.New("SQLite requires a persistent file path")
	}
	path, e := filepath.Abs(path)
	if e != nil {
		return nil, e
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	// Resolve the containing directory so aliases cannot bypass the process lock.
	dir, e := filepath.EvalSymlinks(filepath.Dir(path))
	if e != nil {
		return nil, e
	}
	path = filepath.Join(dir, filepath.Base(path))
	if info, e := os.Lstat(path); e == nil {
		if !info.Mode().IsRegular() {
			return nil, errors.New("SQLite path must be a regular file")
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	lockPath := path + ".lock"
	if info, e := os.Lstat(lockPath); e == nil && !info.Mode().IsRegular() {
		return nil, errors.New("SQLite lock must be a regular file")
	}
	f, e := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	_ = f.Close()
	if e = os.Chmod(lockPath, 0600); e != nil {
		return nil, e
	}
	l := flock.New(lockPath)
	ok, e := l.TryLock()
	if e != nil {
		return nil, e
	}
	if !ok {
		return nil, errors.New("SQLite database already belongs to another Kaflux instance")
	}
	fail := func(e error) (*Store, error) { _ = l.Unlock(); return nil, e }
	f, e = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return fail(e)
	}
	_ = f.Close()
	if e = os.Chmod(path, 0600); e != nil {
		return fail(e)
	}
	dsn := (&url.URL{Scheme: "file", Path: path}).String() + "?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_txlock=immediate"
	db, e := sql.Open("sqlite", dsn)
	if e != nil {
		return fail(e)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	q := &sqliteStore{db: db, lock: l}
	if e = db.PingContext(ctx); e != nil {
		q.close()
		return nil, e
	}
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		q.close()
		return nil, e
	}
	if _, e = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS kaflux_schema(version INTEGER PRIMARY KEY)"); e != nil {
		_ = tx.Rollback()
		q.close()
		return nil, e
	}
	var version sql.NullInt64
	if e = tx.QueryRowContext(ctx, "SELECT MAX(version) FROM kaflux_schema").Scan(&version); e != nil {
		_ = tx.Rollback()
		q.close()
		return nil, e
	}
	if version.Valid && version.Int64 != 1 && version.Int64 != sqliteSchemaVersion {
		_ = tx.Rollback()
		q.close()
		return nil, fmt.Errorf("unsupported SQLite schema version %d", version.Int64)
	}
	_, e = tx.ExecContext(ctx, `
 CREATE TABLE IF NOT EXISTS kaflux_sessions(id TEXT PRIMARY KEY,handle TEXT NOT NULL UNIQUE,payload BLOB NOT NULL,expires INTEGER NOT NULL);
 CREATE INDEX IF NOT EXISTS kaflux_sessions_inventory ON kaflux_sessions(expires,handle);
 CREATE TABLE IF NOT EXISTS kaflux_jobs(id TEXT PRIMARY KEY,cluster_id TEXT NOT NULL,state TEXT NOT NULL,payload BLOB NOT NULL,lease_until INTEGER,lease_owner TEXT);
 CREATE UNIQUE INDEX IF NOT EXISTS kaflux_one_active_job_v2 ON kaflux_jobs(cluster_id) WHERE state IN ('queued','running','paused','rollback-queued');
 DROP INDEX IF EXISTS kaflux_one_active_job;
 CREATE TABLE IF NOT EXISTS kaflux_job_events(job_id TEXT NOT NULL,seq INTEGER NOT NULL,at INTEGER NOT NULL,level TEXT NOT NULL,message TEXT NOT NULL,PRIMARY KEY(job_id,seq));
 CREATE TABLE IF NOT EXISTS kaflux_audit(id TEXT PRIMARY KEY,payload BLOB NOT NULL,created_at INTEGER NOT NULL);
 CREATE INDEX IF NOT EXISTS kaflux_audit_created ON kaflux_audit(created_at);
 CREATE TABLE IF NOT EXISTS kaflux_job_throttles(job_id TEXT PRIMARY KEY,payload BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS oidc_flows(id TEXT PRIMARY KEY,nonce TEXT NOT NULL,verifier TEXT NOT NULL,expires INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS kaflux_cluster_names(cluster_id TEXT PRIMARY KEY,name TEXT NOT NULL);
 INSERT OR IGNORE INTO kaflux_schema(version) VALUES(2);`)
	if e == nil {
		e = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	if e != nil {
		q.close()
		return nil, e
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if e = os.Chmod(path+suffix, 0600); e != nil && !os.IsNotExist(e) {
			q.close()
			return nil, e
		}
	}
	return &Store{sqlite: q}, nil
}

// sqliteSchemaVersion 2 adds the paused state to the active-job index and the job events table.
const sqliteSchemaVersion = 2

func (s *Store) IsPersistent() bool { return s.DB != nil || s.sqlite != nil }
func (s *Store) Ping(ctx context.Context) error {
	if s.sqlite != nil {
		return s.sqlite.db.PingContext(ctx)
	}
	if s.DB != nil {
		return s.DB.Ping(ctx)
	}
	return ctx.Err()
}
func (q *sqliteStore) close() { q.once.Do(func() { _ = q.db.Close(); _ = q.lock.Unlock() }) }
func (q *sqliteStore) session(ctx context.Context, id string) (auth.Session, error) {
	var p auth.Session
	var b []byte
	e := q.db.QueryRowContext(ctx, "SELECT payload FROM kaflux_sessions WHERE id=? AND expires>?", id, time.Now().UnixNano()).Scan(&b)
	if e == nil {
		e = json.Unmarshal(b, &p)
		p.ID = id
	}
	return p, e
}
func (q *sqliteStore) saveSession(ctx context.Context, p auth.Session) error {
	b, e := json.Marshal(p)
	if e != nil {
		return e
	}
	_, e = q.db.ExecContext(ctx, "INSERT INTO kaflux_sessions(id,handle,payload,expires) VALUES(?,?,?,?)", p.ID, SessionHandle(p.ID), b, p.Expires.UnixNano())
	return e
}
func (q *sqliteStore) saveJob(ctx context.Context, p model.Plan) error {
	b, e := json.Marshal(p)
	if e != nil {
		return e
	}
	_, e = q.db.ExecContext(ctx, "INSERT INTO kaflux_jobs(id,cluster_id,state,payload) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state,payload=excluded.payload", p.ID, p.ClusterID, p.State, b)
	return e
}
func (q *sqliteStore) job(ctx context.Context, id string) (model.Plan, error) {
	var p model.Plan
	var b []byte
	e := q.db.QueryRowContext(ctx, "SELECT payload FROM kaflux_jobs WHERE id=?", id).Scan(&b)
	if e == nil {
		e = json.Unmarshal(b, &p)
	}
	return p, e
}
func (q *sqliteStore) jobs(ctx context.Context, active bool) ([]model.Plan, error) {
	query := "SELECT payload FROM kaflux_jobs ORDER BY json_extract(payload,'$.createdAt') DESC LIMIT 500"
	if active {
		query = "SELECT payload FROM kaflux_jobs WHERE state IN ('queued','running') ORDER BY lease_until LIMIT 200"
	}
	r, e := q.db.QueryContext(ctx, query)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	out := []model.Plan{}
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
func (q *sqliteStore) claim(ctx context.Context, id, owner string) bool {
	now := time.Now()
	r, e := q.db.ExecContext(ctx, "UPDATE kaflux_jobs SET lease_owner=?,lease_until=? WHERE id=? AND state IN ('queued','running') AND (lease_until IS NULL OR lease_until<? OR lease_owner=?)", owner, now.Add(30*time.Second).UnixNano(), id, now.UnixNano(), owner)
	if e != nil {
		return false
	}
	n, e := r.RowsAffected()
	return e == nil && n == 1
}
func readSQLiteJob(ctx context.Context, tx *sql.Tx, id string) (model.Plan, lease, error) {
	var p model.Plan
	var b []byte
	var owner sql.NullString
	var until sql.NullInt64
	e := tx.QueryRowContext(ctx, "SELECT payload,lease_owner,lease_until FROM kaflux_jobs WHERE id=?", id).Scan(&b, &owner, &until)
	if e == nil {
		e = json.Unmarshal(b, &p)
	}
	return p, lease{Owner: owner.String, Until: time.Unix(0, until.Int64)}, e
}

// sqliteExec runs extra statements inside changeJob's transaction.
type sqliteExec interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func marshalPlan(p model.Plan) ([]byte, error)    { return json.Marshal(p) }
func unmarshalPlan(b []byte, p *model.Plan) error { return json.Unmarshal(b, p) }

func (q *sqliteStore) changeJob(ctx context.Context, id string, change func(*model.Plan, lease) error, also ...func(sqliteExec) error) error {
	tx, e := q.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	p, l, e := readSQLiteJob(ctx, tx, id)
	if e != nil {
		return e
	}
	if e = change(&p, l); e != nil {
		return e
	}
	b, e := json.Marshal(p)
	if e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "UPDATE kaflux_jobs SET state=?,payload=? WHERE id=?", p.State, b, id); e != nil {
		return e
	}
	for _, f := range also {
		if e = f(tx); e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (q *sqliteStore) audit(ctx context.Context, a Audit) error {
	a.ID = auth.Token()
	a.At = time.Now().UTC()
	b, e := json.Marshal(a)
	if e != nil {
		return e
	}
	_, e = q.db.ExecContext(ctx, "INSERT INTO kaflux_audit VALUES(?,?,?)", a.ID, b, a.At.UnixNano())
	return e
}
func (q *sqliteStore) audits(ctx context.Context) ([]Audit, error) {
	r, e := q.db.QueryContext(ctx, "SELECT payload FROM kaflux_audit ORDER BY created_at DESC LIMIT 200")
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
func (q *sqliteStore) clusterNames(ctx context.Context) (map[string]string, error) {
	r, e := q.db.QueryContext(ctx, "SELECT cluster_id,name FROM kaflux_cluster_names")
	if e != nil {
		return nil, e
	}
	defer r.Close()
	out := map[string]string{}
	for r.Next() {
		var id, name string
		if e = r.Scan(&id, &name); e != nil {
			return nil, e
		}
		out[id] = name
	}
	return out, r.Err()
}
func (q *sqliteStore) setClusterName(ctx context.Context, id, name string) error {
	var e error
	if name == "" {
		_, e = q.db.ExecContext(ctx, "DELETE FROM kaflux_cluster_names WHERE cluster_id=?", id)
	} else {
		_, e = q.db.ExecContext(ctx, "INSERT INTO kaflux_cluster_names VALUES(?,?) ON CONFLICT(cluster_id) DO UPDATE SET name=excluded.name", id, name)
	}
	return e
}
func (q *sqliteStore) listSessions(ctx context.Context, current string, limit, offset int) ([]SessionSummary, bool, error) {
	if limit < 1 || limit > 100 || offset < 0 || offset > 10000 {
		return nil, false, errors.New("invalid session pagination")
	}
	r, e := q.db.QueryContext(ctx, "SELECT id,payload FROM kaflux_sessions WHERE expires>? ORDER BY expires,handle LIMIT ? OFFSET ?", time.Now().UnixNano(), limit+1, offset)
	if e != nil {
		return nil, false, e
	}
	defer r.Close()
	out := []SessionSummary{}
	for r.Next() {
		var id string
		var b []byte
		var p auth.Session
		if e = r.Scan(&id, &b); e != nil {
			return nil, false, e
		}
		if e = json.Unmarshal(b, &p); e != nil {
			return nil, false, e
		}
		p.ID = id
		out = append(out, SummarizeSession(p, current))
	}
	if e = r.Err(); e != nil {
		return nil, false, e
	}
	more := len(out) > limit
	if more {
		out = out[:limit]
	}
	return out, more, nil
}
func (q *sqliteStore) revokeSession(ctx context.Context, handle, current string, authorize func(SessionSummary) bool, audit Audit) (SessionSummary, error) {
	v := SessionSummary{}
	tx, e := q.db.BeginTx(ctx, nil)
	if e != nil {
		return v, e
	}
	defer tx.Rollback()
	var id string
	var b []byte
	e = tx.QueryRowContext(ctx, "SELECT id,payload FROM kaflux_sessions WHERE handle=? AND expires>?", handle, time.Now().UnixNano()).Scan(&id, &b)
	if errors.Is(e, sql.ErrNoRows) {
		return v, ErrSessionNotFound
	}
	if e != nil {
		return v, e
	}
	var p auth.Session
	if e = json.Unmarshal(b, &p); e != nil {
		return v, e
	}
	p.ID = id
	v = SummarizeSession(p, current)
	if !authorize(v) {
		return v, ErrSessionForbidden
	}
	for _, result := range []string{"intent", "success"} {
		a := sessionRevokeAudit(audit, v, result)
		b, e = json.Marshal(a)
		if e != nil {
			return v, e
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO kaflux_audit VALUES(?,?,?)", a.ID, b, a.At.UnixNano()); e != nil {
			return v, e
		}
	}
	if _, e = tx.ExecContext(ctx, "DELETE FROM kaflux_sessions WHERE id=?", id); e != nil {
		return v, e
	}
	return v, tx.Commit()
}
func (q *sqliteStore) saveOIDC(ctx context.Context, id string, p auth.OIDCPending) error {
	tx, e := q.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, "DELETE FROM oidc_flows WHERE expires<?", time.Now().UnixNano()); e != nil {
		return e
	}
	var n int
	if e = tx.QueryRowContext(ctx, "SELECT count(*) FROM oidc_flows").Scan(&n); e != nil {
		return e
	}
	if n >= 1000 {
		return errors.New("pending OIDC login limit reached")
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO oidc_flows VALUES(?,?,?,?)", id, p.Nonce, p.Verifier, p.Expires.UnixNano()); e != nil {
		return e
	}
	return tx.Commit()
}
func (q *sqliteStore) consumeOIDC(ctx context.Context, id string) (auth.OIDCPending, error) {
	var p auth.OIDCPending
	var expiry int64
	e := q.db.QueryRowContext(ctx, "DELETE FROM oidc_flows WHERE id=? AND expires>? RETURNING nonce,verifier,expires", id, time.Now().UnixNano()).Scan(&p.Nonce, &p.Verifier, &expiry)
	p.Expires = time.Unix(0, expiry)
	return p, e
}
func (q *sqliteStore) saveThrottle(ctx context.Context, id string, records []kafka.ThrottleRecord) error {
	b, e := json.Marshal(records)
	if e != nil {
		return e
	}
	_, e = q.db.ExecContext(ctx, "INSERT INTO kaflux_job_throttles VALUES(?,?) ON CONFLICT(job_id) DO NOTHING", id, b)
	return e
}
func (q *sqliteStore) loadThrottle(ctx context.Context, id string) ([]kafka.ThrottleRecord, error) {
	var b []byte
	e := q.db.QueryRowContext(ctx, "SELECT payload FROM kaflux_job_throttles WHERE job_id=?", id).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var records []kafka.ThrottleRecord
	e = json.Unmarshal(b, &records)
	return records, e
}
func (q *sqliteStore) replaceThrottle(ctx context.Context, id, owner, revision string, records []kafka.ThrottleRecord) error {
	tx, e := q.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	p, l, e := readSQLiteJob(ctx, tx, id)
	if e != nil {
		return e
	}
	if p.State != "running" || l.Owner != owner || !time.Now().Before(l.Until) || p.CleanupPending || p.CancellationRequested || p.ThrottleRequest == nil || p.ThrottleRequest.Revision != revision {
		return errors.New("worker lease, pending throttle revision or active job lost")
	}
	var b []byte
	if e = tx.QueryRowContext(ctx, "SELECT payload FROM kaflux_job_throttles WHERE job_id=?", id).Scan(&b); e != nil {
		return e
	}
	var old []kafka.ThrottleRecord
	if e = json.Unmarshal(b, &old); e != nil {
		return e
	}
	if len(old) != len(records) {
		return errors.New("throttle ownership resources cannot change")
	}
	for i, r := range records {
		o := old[i]
		if o.Resource != r.Resource || o.Name != r.Name || o.Key != r.Key || o.EffectiveBefore != r.EffectiveBefore || !reflect.DeepEqual(o.Previous, r.Previous) {
			return errors.New("original throttle configuration cannot change")
		}
	}
	b, e = json.Marshal(records)
	if e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "UPDATE kaflux_job_throttles SET payload=? WHERE job_id=?", b, id); e != nil {
		return e
	}
	return tx.Commit()
}
func (q *sqliteStore) requestThrottle(ctx context.Context, id string, r model.ThrottleRequest) error {
	if r.BytesPerSec < 1 || r.BytesPerSec > 1000000000000 || r.Revision == "" || r.Actor == "" {
		return errors.New("invalid throttle request")
	}
	return q.changeJob(ctx, id, func(p *model.Plan, _ lease) error {
		if p.State != "running" || p.CleanupPending || p.CancellationRequested || p.ThrottleRequest != nil {
			return ErrThrottleConflict
		}
		p.ThrottleRequest = &r
		p.ThrottleError = ""
		return nil
	})
}
func (q *sqliteStore) acknowledgeThrottle(ctx context.Context, id, owner, revision string, rate int64) error {
	if rate < 1 || rate > 1000000000000 {
		return errors.New("invalid applied throttle")
	}
	return q.changeJob(ctx, id, func(p *model.Plan, l lease) error {
		if p.State != "running" || l.Owner != owner || !time.Now().Before(l.Until) || p.ThrottleRequest == nil || p.ThrottleRequest.Revision != revision || p.ThrottleRequest.BytesPerSec != rate {
			return fmt.Errorf("worker lease or throttle revision lost")
		}
		p.ThrottleRequest = nil
		p.ThrottleError = ""
		p.ThrottleBytesPerSec = rate
		return nil
	})
}
