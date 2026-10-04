package store

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSQLiteRestartAndFencing(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "kaflux.db")
	s, e := NewSQLite(ctx, path)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if other, e := NewSQLite(ctx, path); e == nil {
		other.Close()
		t.Fatal("second instance accepted")
	}
	session := auth.Session{ID: "secret", Expires: time.Now().Add(time.Hour), User: auth.User{ID: "admin"}}
	if e = s.SaveSession(ctx, session); e != nil {
		t.Fatal(e)
	}
	p := model.Plan{ID: "job", ClusterID: "cluster", State: "running", ThrottleBytesPerSec: 100}
	if e = s.SaveJob(ctx, p); e != nil {
		t.Fatal(e)
	}
	if !s.Claim(ctx, p.ID, "owner") {
		t.Fatal("claim failed")
	}
	request := model.ThrottleRequest{Revision: "r", Actor: "admin", BytesPerSec: 200}
	if e = s.RequestThrottle(ctx, p.ID, request); e != nil {
		t.Fatal(e)
	}
	if e = s.RequestCancel(ctx, p.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveClaimedJob(ctx, p, "other"); e == nil {
		t.Fatal("unowned save accepted")
	}
	if e = s.SaveClaimedJob(ctx, p, "owner"); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveOIDCState(ctx, "state", auth.OIDCPending{Nonce: "nonce", Verifier: "verifier", Expires: time.Now().Add(time.Hour)}); e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, e = NewSQLite(ctx, path)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	fresh, e := s.Job(ctx, p.ID)
	if e != nil || !fresh.CancellationRequested || fresh.ThrottleRequest == nil {
		t.Fatal("job lost", fresh, e)
	}
	if _, e = s.Session(ctx, session.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = s.ConsumeOIDCState(ctx, "state"); e != nil {
		t.Fatal(e)
	}
	if _, e = s.ConsumeOIDCState(ctx, "state"); e == nil {
		t.Fatal("OIDC replay accepted")
	}
	rows, _, e := s.ListSessions(ctx, "secret", 10, 0)
	if e != nil || len(rows) != 1 || !rows[0].Current {
		t.Fatal(rows, e)
	}
	if _, e = s.RevokeSession(ctx, SessionHandle("secret"), "secret", func(SessionSummary) bool { return true }, Audit{Actor: "admin"}); e != nil {
		t.Fatal(e)
	}
	audits, e := s.Audits(ctx)
	if e != nil || len(audits) != 2 {
		t.Fatal(audits, e)
	}
	info, e := os.Stat(path)
	if e != nil || info.Mode().Perm()&0077 != 0 {
		t.Fatal("insecure db permissions", e)
	}
}

func TestSQLiteFailureDoesNotFallBackToMemory(t *testing.T) {
	ctx := context.Background()
	s, e := NewSQLite(ctx, filepath.Join(t.TempDir(), "db"))
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	if e = s.SaveJob(ctx, model.Plan{ID: "lost"}); e == nil {
		t.Fatal("closed database accepted write")
	}
	if _, e = s.Job(ctx, "lost"); e == nil {
		t.Fatal("closed database read memory")
	}
}

func TestSQLiteThrottleOwnershipReplacementIsFencedAndKeepsOriginal(t *testing.T) {
	ctx := context.Background()
	s, e := NewSQLite(ctx, filepath.Join(t.TempDir(), "db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	p := model.Plan{ID: t.Name() + "-" + auth.Token(), ClusterID: t.Name() + "-" + auth.Token(), State: "running", ThrottleBytesPerSec: 100}
	if e = s.SaveJob(ctx, p); e != nil {
		t.Fatal(e)
	}
	if !s.Claim(ctx, p.ID, "owner") {
		t.Fatal("claim")
	}
	request := model.ThrottleRequest{Revision: "update", Actor: "operator", BytesPerSec: 200}
	if e = s.RequestThrottle(ctx, p.ID, request); e != nil {
		t.Fatal(e)
	}
	original := []kafka.ThrottleRecord{{Resource: "broker", Name: "1", Key: "leader.replication.throttled.rate", EffectiveBefore: "-1", Applied: "100"}}
	if e = s.SaveThrottle(ctx, p.ID, original); e != nil {
		t.Fatal(e)
	}
	next, e := kafka.RetargetThrottle(original, 200)
	if e != nil {
		t.Fatal(e)
	}
	if s.ReplaceThrottle(ctx, p.ID, "other", request.Revision, next) == nil {
		t.Fatal("foreign worker replaced ownership")
	}
	bad := append([]kafka.ThrottleRecord(nil), next...)
	bad[0].EffectiveBefore = "100"
	if s.ReplaceThrottle(ctx, p.ID, "owner", request.Revision, bad) == nil {
		t.Fatal("original configuration overwritten")
	}
	if e = s.ReplaceThrottle(ctx, p.ID, "owner", request.Revision, next); e != nil {
		t.Fatal(e)
	}
	restored, e := s.LoadThrottle(ctx, p.ID)
	if e != nil || restored[0].Applied != "200" || restored[0].TransitionFrom != "100" {
		t.Fatal("transition not durable", e)
	}
	if e = s.RequestCancel(ctx, p.ID); e != nil {
		t.Fatal(e)
	}
	if s.ReplaceThrottle(ctx, p.ID, "owner", request.Revision, kafka.FinalizeThrottle(next)) == nil {
		t.Fatal("cancellation did not take precedence")
	}
}

func TestSQLiteAuditFailureRollsBackRevocation(t *testing.T) {
	ctx := context.Background()
	s, e := NewSQLite(ctx, filepath.Join(t.TempDir(), "db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	session := auth.NewSession(auth.User{ID: "admin"})
	if e = s.SaveSession(ctx, session); e != nil {
		t.Fatal(e)
	}
	_, e = s.sqlite.db.ExecContext(ctx, `CREATE TRIGGER fail_audit BEFORE INSERT ON kaflux_audit WHEN json_extract(NEW.payload,'$.result')='success' BEGIN SELECT RAISE(ABORT,'audit unavailable'); END`)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.RevokeSession(ctx, SessionHandle(session.ID), "", func(SessionSummary) bool { return true }, Audit{})
	if e == nil {
		t.Fatal("audit failure accepted")
	}
	if _, e = s.Session(ctx, session.ID); e != nil {
		t.Fatal("failed audit deleted session", e)
	}
	audits, e := s.Audits(ctx)
	if e != nil || len(audits) != 0 {
		t.Fatal("partial audit persisted", audits, e)
	}
}
func TestSQLiteOIDCConcurrentOneUseAndJobConstraint(t *testing.T) {
	ctx := context.Background()
	s, e := NewSQLite(ctx, filepath.Join(t.TempDir(), "db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.SaveOIDCState(ctx, "state", auth.OIDCPending{Expires: time.Now().Add(time.Hour)}); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	results := make(chan bool, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, e := s.ConsumeOIDCState(ctx, "state"); results <- e == nil }()
	}
	wg.Wait()
	close(results)
	wins := 0
	for ok := range results {
		if ok {
			wins++
		}
	}
	if wins != 1 {
		t.Fatal("one-use winners", wins)
	}
	first := model.Plan{ID: "a", ClusterID: "c", State: "running"}
	if e = s.SaveJob(ctx, first); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveJob(ctx, model.Plan{ID: "b", ClusterID: "c", State: "queued"}); e == nil {
		t.Fatal("concurrent active jobs accepted")
	}
	if !s.Claim(ctx, "a", "first") {
		t.Fatal("initial claim")
	}
	if s.Claim(ctx, "a", "second") {
		t.Fatal("stole live lease")
	}
	if _, e = s.sqlite.db.ExecContext(ctx, "UPDATE kaflux_jobs SET lease_until=0 WHERE id='a'"); e != nil {
		t.Fatal(e)
	}
	if !s.Claim(ctx, "a", "second") {
		t.Fatal("takeover failed")
	}
	if e = s.SaveClaimedJob(ctx, first, "first"); e == nil {
		t.Fatal("stale owner saved")
	}
}

func TestSQLiteRejectsFutureSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	s, e := NewSQLite(ctx, path)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.sqlite.db.ExecContext(ctx, "UPDATE kaflux_schema SET version=999"); e != nil {
		t.Fatal(e)
	}
	s.Close()
	other, e := NewSQLite(ctx, path)
	if e == nil {
		other.Close()
		t.Fatal("future schema accepted")
	}
}

func TestSQLiteProcessLock(t *testing.T) {
	if path := os.Getenv("KAFLUX_SQLITE_LOCK_TEST"); path != "" {
		s, e := NewSQLite(context.Background(), path)
		if e == nil {
			s.Close()
			t.Fatal("second process acquired database")
		}
		return
	}
	path := filepath.Join(t.TempDir(), "db")
	s, e := NewSQLite(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSQLiteProcessLock$")
	cmd.Env = append(os.Environ(), "KAFLUX_SQLITE_LOCK_TEST="+path)
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("process lock: %v %s", e, out)
	}
}
func TestSQLiteThrottleOwnershipSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	s, e := NewSQLite(ctx, path)
	if e != nil {
		t.Fatal(e)
	}
	records := []kafka.ThrottleRecord{{Resource: "broker", Name: "1", Key: "rate", EffectiveBefore: "-1", Applied: "100"}}
	if e = s.SaveThrottle(ctx, "job", records); e != nil {
		t.Fatal(e)
	}
	altered := []kafka.ThrottleRecord{{Resource: "broker", Name: "1", Key: "rate", EffectiveBefore: "100", Applied: "200"}}
	if e = s.SaveThrottle(ctx, "job", altered); e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, e = NewSQLite(ctx, path)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	got, e := s.LoadThrottle(ctx, "job")
	if e != nil || len(got) != 1 || got[0].EffectiveBefore != "-1" || got[0].Applied != "100" {
		t.Fatal("original ownership lost", got, e)
	}
	if e = s.DeleteThrottle(ctx, "job"); e != nil {
		t.Fatal(e)
	}
	got, e = s.LoadThrottle(ctx, "job")
	if e != nil || len(got) != 0 {
		t.Fatal("ownership not deleted", got, e)
	}
}
