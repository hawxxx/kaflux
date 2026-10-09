package auth

import (
	"crypto/rand"
	"encoding/hex"
	"golang.org/x/crypto/bcrypt"
	"path"
	"regexp"
	"time"
)

type User struct {
	ID       string   `json:"id"`
	Username string   `json:"username"`
	Roles    []string `json:"roles"`
	Provider string   `json:"provider"`
}
type Grant struct {
	Role    string `json:"role"`
	Cluster string `json:"cluster"`
	Action  string `json:"action"`
	Pattern string `json:"pattern"`
	Regex   bool   `json:"regex"`
}
type Authorizer struct{ Grants []Grant }

// Actions lists every action the API authorizes. Grants may use "*" or one of these.
var Actions = []string{
	"read", "consume", "produce",
	"create", "delete", "alter-config", "reset-offsets", "rename",
	"plan", "execute", "rollback",
	"manage-acls", "schema-read", "schema-update", "connector-read", "connector-update",
	"audit", "manage-sessions",
}

// Permissions returns the actions the user may perform on at least one resource
// of the cluster. It drives UI affordances; each request is still authorized.
func (a Authorizer) Permissions(u User, cluster string) []string {
	out := []string{}
	for _, action := range Actions {
		if a.AnyAllowed(u, cluster, action) {
			out = append(out, action)
		}
	}
	return out
}

func (a Authorizer) AnyAllowed(u User, cluster, action string) bool {
	for _, g := range a.Grants {
		if g.Cluster != "*" && g.Cluster != cluster {
			continue
		}
		if g.Action != "*" && g.Action != action {
			continue
		}
		for _, r := range u.Roles {
			if r == g.Role {
				return true
			}
		}
	}
	return false
}

func (a Authorizer) Allowed(u User, cluster, action, resource string) bool {
	for _, g := range a.Grants {
		role := false
		for _, r := range u.Roles {
			if r == g.Role {
				role = true
			}
		}
		if !role || (g.Cluster != "*" && g.Cluster != cluster) || (g.Action != "*" && g.Action != action) {
			continue
		}
		if g.Regex {
			if len(g.Pattern) > 512 {
				continue
			}
			r, e := regexp.Compile(g.Pattern)
			if e == nil && r.MatchString(resource) {
				return true
			}
		} else {
			ok, e := path.Match(g.Pattern, resource)
			if e == nil && ok {
				return true
			}
		}
	}
	return false
}

type Session struct {
	ID      string    `json:"-"`
	User    User      `json:"user"`
	CSRF    string    `json:"csrfToken"`
	Expires time.Time `json:"expires"`
}

func Token() string {
	b := make([]byte, 32)
	if _, e := rand.Read(b); e != nil {
		panic("entropy unavailable")
	}
	return hex.EncodeToString(b)
}
func Verify(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

const DefaultSessionLifetime = 8 * time.Hour

// ValidAt rejects a session at or after its absolute expiration.
func (s Session) ValidAt(now time.Time) bool { return now.Before(s.Expires) }

// NewSession defaults to eight hours for callers without an explicit policy.
// Configured lifetimes are validated by the configuration loader at startup.
func NewSession(u User, lifetime ...time.Duration) Session {
	ttl := DefaultSessionLifetime
	if len(lifetime) > 0 && lifetime[0] != 0 {
		ttl = lifetime[0]
	}
	return Session{ID: Token(), User: u, CSRF: Token(), Expires: time.Now().Add(ttl)}
}
