package store

import (
	"context"
	"fmt"

	"github.com/hawxxx/kaflux/backend/internal/auth"
)

func (s *Store) EnsureOIDC(ctx context.Context) error {
	if s.sqlite != nil {
		return s.sqlite.db.PingContext(ctx)
	}
	if s.DB == nil {
		return fmt.Errorf("shared OIDC flows require PostgreSQL")
	}
	_, err := s.DB.Exec(ctx, `CREATE TABLE IF NOT EXISTS oidc_flows (id text PRIMARY KEY, nonce text NOT NULL, verifier text NOT NULL, expires timestamptz NOT NULL)`)
	return err
}

func (s *Store) SaveOIDCState(ctx context.Context, id string, p auth.OIDCPending) error {
	if s.sqlite != nil {
		return s.sqlite.saveOIDC(ctx, id, p)
	}
	if s.DB == nil {
		return fmt.Errorf("shared OIDC flows require PostgreSQL")
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Serialize the bounded queue check across replicas. This short transaction
	// stores no provider credential, only one-use nonce and PKCE verifier.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(736183405)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM oidc_flows WHERE expires < now()`); err != nil {
		return err
	}
	var count int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM oidc_flows`).Scan(&count); err != nil {
		return err
	}
	if count >= 1000 {
		return fmt.Errorf("pending OIDC login limit reached")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO oidc_flows(id,nonce,verifier,expires) VALUES($1,$2,$3,$4)`, id, p.Nonce, p.Verifier, p.Expires); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) ConsumeOIDCState(ctx context.Context, id string) (auth.OIDCPending, error) {
	if s.sqlite != nil {
		return s.sqlite.consumeOIDC(ctx, id)
	}
	var p auth.OIDCPending
	if s.DB == nil {
		return p, fmt.Errorf("shared OIDC flows require PostgreSQL")
	}
	err := s.DB.QueryRow(ctx, `DELETE FROM oidc_flows WHERE id=$1 AND expires > now() RETURNING nonce,verifier,expires`, id).Scan(&p.Nonce, &p.Verifier, &p.Expires)
	return p, err
}
