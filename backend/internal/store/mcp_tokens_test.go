package store

import (
	"context"
	"errors"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

type mcpBackend struct {
	name string
	make func(t *testing.T) *Store
}

func mcpBackends() []mcpBackend {
	return []mcpBackend{
		{"memory", func(t *testing.T) *Store {
			s, e := New(context.Background(), "")
			if e != nil {
				t.Fatal(e)
			}
			return s
		}},
		{"sqlite", func(t *testing.T) *Store {
			s, e := NewSQLite(context.Background(), filepath.Join(t.TempDir(), "kaflux.db"))
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(s.Close)
			return s
		}},
	}
}

func newMCPToken(id, owner string, expires time.Time) MCPToken {
	return MCPToken{ID: id, Name: "tok " + id, OwnerID: owner, OwnerProvider: "local", OwnerRoles: []string{"operator"}, Scopes: []string{"read", "messages"}, Clusters: []string{"c1"}, Expires: expires, Created: time.Now().UTC().Truncate(time.Millisecond)}
}

func TestMCPTokenLifecycle(t *testing.T) {
	for _, b := range mcpBackends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			s := b.make(t)
			secret, hash := NewMCPSecret()
			tok := newMCPToken("t1", "alice", time.Now().Add(time.Hour))
			if e := s.CreateMCPToken(ctx, tok, hash); e != nil {
				t.Fatal(e)
			}
			got, e := s.MCPTokenByHash(ctx, HashMCPSecret(secret))
			if e != nil || got.ID != "t1" || got.OwnerID != "alice" || len(got.OwnerRoles) != 1 || got.OwnerRoles[0] != "operator" || len(got.Scopes) != 2 || got.Clusters[0] != "c1" || got.LastUsed != nil || got.Revoked != nil {
				t.Fatalf("lookup %+v %v", got, e)
			}
			at := time.Now().UTC()
			if e = s.TouchMCPToken(ctx, "t1", at); e != nil {
				t.Fatal(e)
			}
			got, _ = s.MCPTokenByHash(ctx, hash)
			if got.LastUsed == nil || got.LastUsed.Sub(at).Abs() > time.Second {
				t.Fatalf("touch not recorded: %v", got.LastUsed)
			}
			first := *got.LastUsed
			if e = s.TouchMCPToken(ctx, "t1", at.Add(30*time.Second)); e != nil {
				t.Fatal(e)
			}
			got, _ = s.MCPTokenByHash(ctx, hash)
			if !got.LastUsed.Equal(first) {
				t.Fatal("touch within a minute wrote")
			}
			if e = s.TouchMCPToken(ctx, "t1", at.Add(2*time.Minute)); e != nil {
				t.Fatal(e)
			}
			got, _ = s.MCPTokenByHash(ctx, hash)
			if !got.LastUsed.After(first) {
				t.Fatal("touch after a minute did not write")
			}
			if e = s.RevokeMCPToken(ctx, "t1"); e != nil {
				t.Fatal(e)
			}
			if _, e = s.MCPTokenByHash(ctx, hash); !errors.Is(e, ErrMCPTokenNotFound) {
				t.Fatalf("revoked lookup: %v", e)
			}
			if e = s.RevokeMCPToken(ctx, "missing"); !errors.Is(e, ErrMCPTokenNotFound) {
				t.Fatalf("revoke missing: %v", e)
			}
			if _, e = s.MCPTokenByHash(ctx, "nope"); !errors.Is(e, ErrMCPTokenNotFound) {
				t.Fatalf("unknown hash: %v", e)
			}
			list, e := s.ListMCPTokens(ctx, "alice", false)
			if e != nil || len(list) != 1 || list[0].Revoked == nil {
				t.Fatalf("revoked token should list with Revoked set: %+v %v", list, e)
			}
		})
	}
}

func TestMCPTokenExpired(t *testing.T) {
	for _, b := range mcpBackends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			s := b.make(t)
			_, hash := NewMCPSecret()
			if e := s.CreateMCPToken(ctx, newMCPToken("old", "alice", time.Now().Add(-time.Minute)), hash); e != nil {
				t.Fatal(e)
			}
			if _, e := s.MCPTokenByHash(ctx, hash); !errors.Is(e, ErrMCPTokenNotFound) {
				t.Fatalf("expired lookup: %v", e)
			}
		})
	}
}

func TestMCPSecretShape(t *testing.T) {
	a, ha := NewMCPSecret()
	b, hb := NewMCPSecret()
	if a == b || ha == hb {
		t.Fatal("secrets repeat")
	}
	if !regexp.MustCompile(`^kfx_[A-Za-z0-9_-]{43}$`).MatchString(a) {
		t.Fatalf("secret shape %q", a)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(ha) || ha != HashMCPSecret(a) {
		t.Fatalf("hash %q", ha)
	}
}

func TestListMCPTokensOwnerOnly(t *testing.T) {
	for _, b := range mcpBackends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			s := b.make(t)
			for _, c := range []struct{ id, owner string }{{"a1", "alice"}, {"a2", "alice"}, {"b1", "bob"}} {
				_, h := NewMCPSecret()
				if e := s.CreateMCPToken(ctx, newMCPToken(c.id, c.owner, time.Now().Add(time.Hour)), h); e != nil {
					t.Fatal(e)
				}
			}
			mine, e := s.ListMCPTokens(ctx, "alice", false)
			if e != nil || len(mine) != 2 {
				t.Fatalf("alice %+v %v", mine, e)
			}
			for _, m := range mine {
				if m.OwnerID != "alice" {
					t.Fatal("leaked another owner's token")
				}
			}
			all, e := s.ListMCPTokens(ctx, "alice", true)
			if e != nil || len(all) != 3 {
				t.Fatalf("all %+v %v", all, e)
			}
			none, e := s.ListMCPTokens(ctx, "carol", false)
			if e != nil || len(none) != 0 {
				t.Fatalf("carol %+v %v", none, e)
			}
		})
	}
}

// A row written before owner roles were recorded still decodes, with no roles.
func TestMCPPayloadWithoutOwnerRoles(t *testing.T) {
	old := []byte(`{"id":"t0","name":"old","ownerId":"alice","ownerProvider":"local","scopes":["read"],"clusters":[],"expires":"2030-01-01T00:00:00Z","created":"2026-01-01T00:00:00Z"}`)
	got, e := mcpFromPayload(old, nil, nil)
	if e != nil || got.ID != "t0" || got.OwnerRoles == nil || len(got.OwnerRoles) != 0 {
		t.Fatalf("old payload: %+v %v", got, e)
	}
}
