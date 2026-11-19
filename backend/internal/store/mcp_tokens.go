package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrMCPTokenNotFound is returned when a token is absent, revoked or expired.
var ErrMCPTokenNotFound = errors.New("mcp token not found")

const mcpTouchInterval = time.Minute

// MCPToken is a bearer token for the MCP endpoint. Only the SHA-256 hash of
// the secret is ever persisted.
type MCPToken struct {
	ID            string
	Name          string
	OwnerID       string
	OwnerProvider string
	// OwnerRoles snapshots the owner's roles when the token was created; the
	// owner's grants are resolved from them and the current configuration on
	// every call.
	OwnerRoles []string
	Scopes     []string
	Clusters   []string
	Expires    time.Time
	Created    time.Time
	LastUsed   *time.Time
	Revoked    *time.Time
}

// NewMCPSecret returns a fresh secret and the hex SHA-256 hash to persist.
func NewMCPSecret() (secret, hash string) {
	b := make([]byte, 32)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	secret = "kfx_" + base64.RawURLEncoding.EncodeToString(b)
	return secret, HashMCPSecret(secret)
}

// HashMCPSecret returns the hex SHA-256 of a token secret.
func HashMCPSecret(secret string) string {
	h := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(h[:])
}

// mcpStatic is the immutable part of a token kept as a JSON payload; mutable
// state (last used, revoked) lives in dedicated columns.
type mcpStatic struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	OwnerID       string    `json:"ownerId"`
	OwnerProvider string    `json:"ownerProvider"`
	OwnerRoles    []string  `json:"ownerRoles"`
	Scopes        []string  `json:"scopes"`
	Clusters      []string  `json:"clusters"`
	Expires       time.Time `json:"expires"`
	Created       time.Time `json:"created"`
}

func mcpPayload(t MCPToken) ([]byte, error) {
	return json.Marshal(mcpStatic{t.ID, t.Name, t.OwnerID, t.OwnerProvider, nonNil(t.OwnerRoles), nonNil(t.Scopes), nonNil(t.Clusters), t.Expires.UTC(), t.Created.UTC()})
}

func nonNil(v []string) []string {
	if v == nil {
		return []string{}
	}
	return append([]string{}, v...)
}

func mcpFromPayload(b []byte, lastUsed, revoked *time.Time) (MCPToken, error) {
	var p mcpStatic
	if e := json.Unmarshal(b, &p); e != nil {
		return MCPToken{}, e
	}
	return MCPToken{ID: p.ID, Name: p.Name, OwnerID: p.OwnerID, OwnerProvider: p.OwnerProvider, OwnerRoles: nonNil(p.OwnerRoles), Scopes: p.Scopes, Clusters: p.Clusters, Expires: p.Expires, Created: p.Created, LastUsed: lastUsed, Revoked: revoked}, nil
}

func sortMCP(out []MCPToken) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].Created.Equal(out[j].Created) {
			return out[i].ID < out[j].ID
		}
		return out[i].Created.Before(out[j].Created)
	})
}

func cloneMCP(t MCPToken) MCPToken {
	t.OwnerRoles = nonNil(t.OwnerRoles)
	t.Scopes = nonNil(t.Scopes)
	t.Clusters = nonNil(t.Clusters)
	return t
}

func (s *Store) CreateMCPToken(ctx context.Context, t MCPToken, hash string) error {
	b, e := mcpPayload(t)
	if e != nil {
		return e
	}
	if s.sqlite != nil {
		_, e = s.sqlite.db.ExecContext(ctx, `INSERT INTO kaflux_mcp_tokens(id,hash,owner_id,payload,expires,created) VALUES(?,?,?,?,?,?)`, t.ID, hash, t.OwnerID, b, t.Expires.UnixMilli(), t.Created.UnixMilli())
		return e
	}
	if s.DB != nil {
		_, e = s.DB.Exec(ctx, `INSERT INTO kaflux_mcp_tokens(id,hash,owner_id,payload,expires,created) VALUES($1,$2,$3,$4,$5,$6)`, t.ID, hash, t.OwnerID, b, t.Expires, t.Created)
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mcp == nil {
		s.mcp = map[string]mcpEntry{}
	}
	for id, v := range s.mcp {
		if id == t.ID || v.hash == hash {
			return errors.New("mcp token already exists")
		}
	}
	s.mcp[t.ID] = mcpEntry{hash: hash, token: cloneMCP(t)}
	return nil
}

type mcpEntry struct {
	hash  string
	token MCPToken
}

func (s *Store) MCPTokenByHash(ctx context.Context, hash string) (MCPToken, error) {
	now := time.Now()
	if s.sqlite != nil {
		var b []byte
		var used, revoked sql.NullInt64
		e := s.sqlite.db.QueryRowContext(ctx, `SELECT payload,last_used,revoked FROM kaflux_mcp_tokens WHERE hash=? AND revoked IS NULL AND expires>?`, hash, now.UnixMilli()).Scan(&b, &used, &revoked)
		if errors.Is(e, sql.ErrNoRows) {
			return MCPToken{}, ErrMCPTokenNotFound
		}
		if e != nil {
			return MCPToken{}, e
		}
		return mcpFromPayload(b, msTime(used), msTime(revoked))
	}
	if s.DB != nil {
		var b []byte
		var used, revoked *time.Time
		e := s.DB.QueryRow(ctx, `SELECT payload,last_used,revoked FROM kaflux_mcp_tokens WHERE hash=$1 AND revoked IS NULL AND expires>now()`, hash).Scan(&b, &used, &revoked)
		if errors.Is(e, pgx.ErrNoRows) {
			return MCPToken{}, ErrMCPTokenNotFound
		}
		if e != nil {
			return MCPToken{}, e
		}
		return mcpFromPayload(b, used, revoked)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.mcp {
		if v.hash == hash && v.token.Revoked == nil && v.token.Expires.After(now) {
			return cloneMCP(v.token), nil
		}
	}
	return MCPToken{}, ErrMCPTokenNotFound
}

func msTime(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := time.UnixMilli(v.Int64).UTC()
	return &t
}

// ListMCPTokens returns tokens (including revoked and expired) owned by
// ownerID, or every token when all is true, oldest first.
func (s *Store) ListMCPTokens(ctx context.Context, ownerID string, all bool) ([]MCPToken, error) {
	out := []MCPToken{}
	if s.sqlite != nil {
		rows, e := s.sqlite.db.QueryContext(ctx, `SELECT payload,last_used,revoked FROM kaflux_mcp_tokens WHERE ?1 OR owner_id=?2`, all, ownerID)
		if e != nil {
			return nil, e
		}
		defer rows.Close()
		for rows.Next() {
			var b []byte
			var used, revoked sql.NullInt64
			if e = rows.Scan(&b, &used, &revoked); e != nil {
				return nil, e
			}
			t, e := mcpFromPayload(b, msTime(used), msTime(revoked))
			if e != nil {
				return nil, e
			}
			out = append(out, t)
		}
		if e = rows.Err(); e != nil {
			return nil, e
		}
		sortMCP(out)
		return out, nil
	}
	if s.DB != nil {
		rows, e := s.DB.Query(ctx, `SELECT payload,last_used,revoked FROM kaflux_mcp_tokens WHERE $1 OR owner_id=$2`, all, ownerID)
		if e != nil {
			return nil, e
		}
		defer rows.Close()
		for rows.Next() {
			var b []byte
			var used, revoked *time.Time
			if e = rows.Scan(&b, &used, &revoked); e != nil {
				return nil, e
			}
			t, e := mcpFromPayload(b, used, revoked)
			if e != nil {
				return nil, e
			}
			out = append(out, t)
		}
		if e = rows.Err(); e != nil {
			return nil, e
		}
		sortMCP(out)
		return out, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.mcp {
		if all || v.token.OwnerID == ownerID {
			out = append(out, cloneMCP(v.token))
		}
	}
	sortMCP(out)
	return out, nil
}

// RevokeMCPToken marks a token revoked. Revoking an unknown token returns
// ErrMCPTokenNotFound; revoking twice keeps the first revocation time.
func (s *Store) RevokeMCPToken(ctx context.Context, id string) error {
	now := time.Now().UTC()
	var n int64
	if s.sqlite != nil {
		r, e := s.sqlite.db.ExecContext(ctx, `UPDATE kaflux_mcp_tokens SET revoked=COALESCE(revoked,?) WHERE id=?`, now.UnixMilli(), id)
		if e != nil {
			return e
		}
		n, _ = r.RowsAffected()
	} else if s.DB != nil {
		r, e := s.DB.Exec(ctx, `UPDATE kaflux_mcp_tokens SET revoked=COALESCE(revoked,$1) WHERE id=$2`, now, id)
		if e != nil {
			return e
		}
		n = r.RowsAffected()
	} else {
		s.mu.Lock()
		defer s.mu.Unlock()
		if v, ok := s.mcp[id]; ok {
			n = 1
			if v.token.Revoked == nil {
				v.token.Revoked = &now
				s.mcp[id] = v
			}
		}
	}
	if n == 0 {
		return ErrMCPTokenNotFound
	}
	return nil
}

// TouchMCPToken records use at the given time, skipping the write when the
// stored last-used time is less than a minute old.
func (s *Store) TouchMCPToken(ctx context.Context, id string, at time.Time) error {
	cutoff := at.Add(-mcpTouchInterval)
	if s.sqlite != nil {
		_, e := s.sqlite.db.ExecContext(ctx, `UPDATE kaflux_mcp_tokens SET last_used=? WHERE id=? AND (last_used IS NULL OR last_used<=?)`, at.UnixMilli(), id, cutoff.UnixMilli())
		return e
	}
	if s.DB != nil {
		_, e := s.DB.Exec(ctx, `UPDATE kaflux_mcp_tokens SET last_used=$1 WHERE id=$2 AND (last_used IS NULL OR last_used<=$3)`, at, id, cutoff)
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.mcp[id]
	if !ok || (v.token.LastUsed != nil && v.token.LastUsed.After(cutoff)) {
		return nil
	}
	t := at.UTC()
	v.token.LastUsed = &t
	s.mcp[id] = v
	return nil
}
