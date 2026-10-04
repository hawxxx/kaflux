package store

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"os"
	"testing"
	"time"
)

func TestPostgresPersistenceAndLease(t *testing.T) {
	url := os.Getenv("KAFLUX_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set KAFLUX_TEST_DATABASE_URL for PostgreSQL integration")
	}
	ctx, c := context.WithTimeout(context.Background(), 20*time.Second)
	defer c()
	s, e := New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	session := auth.NewSession(auth.User{ID: "integration", Roles: []string{"viewer"}})
	if e = s.SaveSession(ctx, session); e != nil {
		t.Fatal(e)
	}
	defer s.DeleteSession(context.Background(), session.ID)
	other, e := New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	out, e := other.Session(ctx, session.ID)
	if e != nil || out.CSRF != session.CSRF {
		t.Fatal("session persistence", e)
	}
	p := model.Plan{ID: auth.Token(), ClusterID: auth.Token(), State: "queued"}
	if e = s.SaveJob(ctx, p); e != nil {
		t.Fatal(e)
	}
	defer s.DB.Exec(context.Background(), "DELETE FROM kaflux_jobs WHERE id=$1", p.ID)
	if !s.Claim(ctx, p.ID, "owner-a") || other.Claim(ctx, p.ID, "owner-b") {
		t.Fatal("lease permits concurrent owner")
	}
}
