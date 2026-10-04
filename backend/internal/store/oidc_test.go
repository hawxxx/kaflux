package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/auth"
)

func TestPostgresOIDCStateSharedOneUse(t *testing.T) {
	database := os.Getenv("KAFLUX_TEST_DATABASE_URL")
	if database == "" {
		t.Skip("set KAFLUX_TEST_DATABASE_URL for PostgreSQL integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	first, err := New(ctx, database)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := New(ctx, database)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := first.EnsureOIDC(ctx); err != nil {
		t.Fatal(err)
	}
	id := auth.Token()
	want := auth.OIDCPending{Nonce: "fixture-nonce", Verifier: "fixture-verifier", Expires: time.Now().Add(time.Minute)}
	if err := first.SaveOIDCState(ctx, id, want); err != nil {
		t.Fatal(err)
	}
	got, err := second.ConsumeOIDCState(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Nonce != want.Nonce || got.Verifier != want.Verifier {
		t.Fatal("shared state changed")
	}
	if _, err := first.ConsumeOIDCState(ctx, id); err == nil {
		t.Fatal("consumed state replay accepted")
	}
	id = auth.Token()
	want.Expires = time.Now().Add(-time.Minute)
	if err := first.SaveOIDCState(ctx, id, want); err != nil {
		t.Fatal(err)
	}
	if _, err := second.ConsumeOIDCState(ctx, id); err == nil {
		t.Fatal("expired state accepted")
	}
}
