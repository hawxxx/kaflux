package store

import (
	"context"
	"errors"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"os"
	"testing"
	"time"
)

func TestSessionInventoryOrderingAndCanceledRevoke(t *testing.T) {
	s, _ := New(context.Background(), "")
	expiry := time.Now().Add(time.Hour)
	for _, id := range []string{"c", "a", "b"} {
		v := auth.NewSession(auth.User{ID: id})
		v.ID = id
		v.Expires = expiry
		if e := s.SaveSession(context.Background(), v); e != nil {
			t.Fatal(e)
		}
	}
	items, more, e := s.ListSessions(context.Background(), "a", 1, 1)
	if e != nil || !more || len(items) != 1 {
		t.Fatalf("page %v %v %v", items, more, e)
	}
	all, _, _ := s.ListSessions(context.Background(), "a", 3, 0)
	for i := 1; i < len(all); i++ {
		if all[i-1].Handle >= all[i].Handle {
			t.Fatal("unstable order")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e = s.RevokeSession(ctx, SessionHandle("a"), "", func(SessionSummary) bool { return true }, Audit{})
	if !errors.Is(e, context.Canceled) {
		t.Fatalf("cancel %v", e)
	}
	if _, e = s.Session(context.Background(), "a"); e != nil {
		t.Fatal("canceled revocation deleted session")
	}
	audits, _ := s.Audits(context.Background())
	if len(audits) != 0 {
		t.Fatal("canceled mutation emitted success")
	}
}
func TestPostgresSessionRevokeAuditAtomicity(t *testing.T) {
	url := os.Getenv("KAFLUX_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set KAFLUX_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, e := New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	other, e := New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	v := auth.NewSession(auth.User{ID: auth.Token(), Provider: "local"})
	if e = s.SaveSession(ctx, v); e != nil {
		t.Fatal(e)
	}
	defer s.DeleteSession(context.Background(), v.ID)
	actor := auth.Token()
	name := "session_revoke_" + actor
	_, e = s.DB.Exec(ctx, `CREATE FUNCTION `+name+`() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.payload->>'actor'='`+actor+`' AND NEW.payload->>'result'='success' THEN RAISE EXCEPTION 'audit unavailable'; END IF; RETURN NEW; END $$; CREATE TRIGGER `+name+` BEFORE INSERT ON kaflux_audit FOR EACH ROW EXECUTE FUNCTION `+name+`()`)
	if e != nil {
		t.Fatal(e)
	}
	defer s.DB.Exec(context.Background(), `DROP TRIGGER IF EXISTS `+name+` ON kaflux_audit; DROP FUNCTION IF EXISTS `+name+`()`)
	_, e = s.RevokeSession(ctx, SessionHandle(v.ID), v.ID, func(SessionSummary) bool { return true }, Audit{Actor: actor})
	if e == nil {
		t.Fatal("audit failure succeeded")
	}
	if _, e = s.Session(ctx, v.ID); e != nil {
		t.Fatal("audit failure deleted session")
	}
	var n int
	if e = s.DB.QueryRow(ctx, `SELECT count(*) FROM kaflux_audit WHERE payload->>'actor'=$1`, actor).Scan(&n); e != nil || n != 0 {
		t.Fatalf("partial audit persisted %d %v", n, e)
	}
	if _, e = s.DB.Exec(ctx, `DROP TRIGGER `+name+` ON kaflux_audit`); e != nil {
		t.Fatal(e)
	}
	items, _, e := s.ListSessions(ctx, v.ID, 100, 0)
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, item := range items {
		if item.Handle == SessionHandle(v.ID) {
			found = item.Current
		}
	}
	if !found {
		t.Fatal("postgres inventory missing current session")
	}
	_, e = s.RevokeSession(ctx, SessionHandle(v.ID), v.ID, func(SessionSummary) bool { return true }, Audit{Actor: actor})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = other.Session(ctx, v.ID); e == nil {
		t.Fatal("revoke did not persist")
	}
	defer s.DB.Exec(context.Background(), `DELETE FROM kaflux_audit WHERE payload->>'actor'=$1`, actor)
}
