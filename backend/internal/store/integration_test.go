package store

import (
	"context"
	"errors"
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

func TestPostgresMCPTokens(t *testing.T) {
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
	id, owner := auth.Token(), auth.Token()
	secret, hash := NewMCPSecret()
	tok := newMCPToken(id, owner, time.Now().Add(time.Hour))
	if e = s.CreateMCPToken(ctx, tok, hash); e != nil {
		t.Fatal(e)
	}
	defer s.DB.Exec(context.Background(), "DELETE FROM kaflux_mcp_tokens WHERE id=$1", id)
	got, e := s.MCPTokenByHash(ctx, HashMCPSecret(secret))
	if e != nil || got.ID != id || got.OwnerID != owner || len(got.Scopes) != 2 {
		t.Fatalf("lookup %+v %v", got, e)
	}
	if e = s.TouchMCPToken(ctx, id, time.Now()); e != nil {
		t.Fatal(e)
	}
	list, e := s.ListMCPTokens(ctx, owner, false)
	if e != nil || len(list) != 1 || list[0].LastUsed == nil {
		t.Fatalf("list %+v %v", list, e)
	}
	if e = s.RevokeMCPToken(ctx, id); e != nil {
		t.Fatal(e)
	}
	if _, e = s.MCPTokenByHash(ctx, hash); !errors.Is(e, ErrMCPTokenNotFound) {
		t.Fatalf("revoked lookup: %v", e)
	}
}
