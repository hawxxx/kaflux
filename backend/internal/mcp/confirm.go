package mcp

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// DefaultConfirmTTL is how long a prepared write stays committable.
const DefaultConfirmTTL = 5 * time.Minute

// defaultReplayEntries bounds the replay cache. Entries leave it when their
// token expires; a full cache of live entries refuses new commits instead of
// forgetting a used token.
const defaultReplayEntries = 4096

var (
	ErrConfirmationInvalid  = errors.New("confirmation token is malformed or was not issued by this server")
	ErrConfirmationExpired  = errors.New("confirmation token has expired")
	ErrConfirmationMismatch = errors.New("confirmation token was issued for a different principal, tool, cluster or arguments")
	ErrConfirmationBusy     = errors.New("too many confirmations in use")
)

const (
	tokenVersion = 1
	nonceLen     = 16
	// payload: version | expiry (unix seconds, big endian) | nonce | binding digest
	payloadLen = 1 + 8 + nonceLen + sha256.Size
	tokenLen   = payloadLen + sha256.Size
)

// outcome is the recorded result of the first commit with a token.
type outcome struct {
	res       Result
	terr      *ToolError
	uncertain bool
}

type replayEntry struct {
	expires time.Time
	owner   string        // principalID, for the per-token cap
	done    chan struct{} // closed when the outcome is recorded or the claim released
	out     *outcome      // nil while pending
}

// Confirmer mints and verifies the confirmation tokens of the two-phase
// write flow, and remembers each used token's outcome so a replayed commit
// returns the original result instead of writing again.
//
// The signing key and the replay cache live in this process only. Tokens
// minted by another process, or before a restart, fail verification
// (closed, never open). A multi-replica deployment therefore needs sticky
// routing for /mcp, so a prepare and its commit reach the same replica.
type Confirmer struct {
	key        []byte
	ttl        time.Duration
	now        func() time.Time
	mu         sync.Mutex
	used       map[[nonceLen]byte]*replayEntry
	perOwner   map[string]int
	maxEntries int
}

// NewConfirmer returns a Confirmer. Production passes a nil key, which
// draws a random per-process key; a fixed key exists for tests only and
// must not be fed from configuration. ttl <= 0 takes DefaultConfirmTTL;
// nil now takes time.Now.
func NewConfirmer(key []byte, ttl time.Duration, now func() time.Time) *Confirmer {
	if len(key) == 0 {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			panic("mcp: no randomness for the confirmation key: " + err.Error())
		}
	} else {
		key = append([]byte{}, key...)
	}
	if ttl <= 0 {
		ttl = DefaultConfirmTTL
	}
	if now == nil {
		now = time.Now
	}
	return &Confirmer{key: key, ttl: ttl, now: now, used: map[[nonceLen]byte]*replayEntry{}, perOwner: map[string]int{}, maxEntries: defaultReplayEntries}
}

// principalID identifies the caller a token is bound to: the token and its
// owner, so neither a sibling token nor another owner can use it.
func principalID(p Principal) string { return p.Session.User.ID + "\x00" + p.TokenID }

// argsHash is the SHA-256 of the canonical JSON of the arguments without the
// MCP confirmation argument. encoding/json sorts map keys at every level.
func argsHash(args map[string]any) [sha256.Size]byte {
	clean := make(map[string]any, len(args))
	for k, v := range args {
		if k != "confirmation" {
			clean[k] = v
		}
	}
	b, err := json.Marshal(clean)
	if err != nil {
		// Unmarshalable arguments can never be dispatched; bind them to
		// a value no valid call produces.
		b = []byte("\x00unmarshalable")
	}
	return sha256.Sum256(b)
}

// binding digests what a token authorizes, with length-prefixed fields.
func binding(p Principal, t Tool, c Call) [sha256.Size]byte {
	h := sha256.New()
	ah := argsHash(c.Args)
	for _, f := range [][]byte{[]byte("kaflux-mcp-confirm/v1"), []byte(principalID(p)), []byte(t.Name), []byte(t.Method + " " + t.Path), []byte(c.Cluster), ah[:]} {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(f)))
		h.Write(n[:])
		h.Write(f)
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

func (c *Confirmer) mac(payload []byte) []byte {
	m := hmac.New(sha256.New, c.key)
	m.Write(payload)
	return m.Sum(nil)
}

// Mint issues a token authorizing exactly call of t by p until expires.
func (c *Confirmer) Mint(p Principal, t Tool, call Call) (token string, expires time.Time) {
	expires = c.now().Add(c.ttl).Truncate(time.Second)
	payload := make([]byte, 0, tokenLen)
	payload = append(payload, tokenVersion)
	payload = binary.BigEndian.AppendUint64(payload, uint64(expires.Unix()))
	nonce := make([]byte, nonceLen)
	if _, err := rand.Read(nonce); err != nil {
		panic("mcp: no randomness for a confirmation nonce: " + err.Error())
	}
	payload = append(payload, nonce...)
	b := binding(p, t, call)
	payload = append(payload, b[:]...)
	return base64.RawURLEncoding.EncodeToString(append(payload, c.mac(payload)...)), expires
}

type claims struct {
	nonce   [nonceLen]byte
	expires time.Time
	owner   string
}

// check authenticates token and its binding to p, t and call.
func (c *Confirmer) check(token string, p Principal, t Tool, call Call) (claims, error) {
	var cl claims
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(raw) != tokenLen {
		return cl, ErrConfirmationInvalid
	}
	payload, sig := raw[:payloadLen], raw[payloadLen:]
	if !hmac.Equal(sig, c.mac(payload)) || payload[0] != tokenVersion {
		return cl, ErrConfirmationInvalid
	}
	cl.expires = time.Unix(int64(binary.BigEndian.Uint64(payload[1:9])), 0)
	copy(cl.nonce[:], payload[9:9+nonceLen])
	if !c.now().Before(cl.expires) {
		return cl, ErrConfirmationExpired
	}
	want := binding(p, t, call)
	if subtle.ConstantTimeCompare(payload[9+nonceLen:], want[:]) != 1 {
		return cl, ErrConfirmationMismatch
	}
	cl.owner = principalID(p)
	return cl, nil
}

// perOwnerCap bounds one token's live entries, so a single token cannot
// fill the cache for everyone.
func (c *Confirmer) perOwnerCap() int { return max(1, c.maxEntries/16) }

// claim reserves a verified token for one commit. It returns the recorded
// outcome when the token was already used, waiting for an in-flight commit
// with the same token to finish first. A nil outcome with a nil error means
// the caller holds the claim and must finish or release it.
func (c *Confirmer) claim(ctx context.Context, cl claims) (*outcome, error) {
	for {
		c.mu.Lock()
		e, ok := c.used[cl.nonce]
		if ok && e.out != nil {
			out := e.out
			c.mu.Unlock()
			return out, nil
		}
		if !ok {
			// Re-check under the lock: the token may have expired since check.
			if !c.now().Before(cl.expires) {
				c.mu.Unlock()
				return nil, ErrConfirmationExpired
			}
			if len(c.used) >= c.maxEntries || c.perOwner[cl.owner] >= c.perOwnerCap() {
				c.pruneLocked()
			}
			if len(c.used) >= c.maxEntries || c.perOwner[cl.owner] >= c.perOwnerCap() {
				c.mu.Unlock()
				return nil, ErrConfirmationBusy
			}
			c.used[cl.nonce] = &replayEntry{expires: cl.expires, owner: cl.owner, done: make(chan struct{})}
			c.perOwner[cl.owner]++
			c.mu.Unlock()
			return nil, nil
		}
		done := e.done
		c.mu.Unlock()
		select {
		case <-done:
			// Recorded or released; look again.
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *Confirmer) deleteLocked(nonce [nonceLen]byte, e *replayEntry) {
	delete(c.used, nonce)
	if c.perOwner[e.owner]--; c.perOwner[e.owner] <= 0 {
		delete(c.perOwner, e.owner)
	}
}

// pruneLocked drops finished entries a full TTL past their expiry. The
// grace keeps a backward clock step from reviving a forgotten token, since
// check rejects it as expired as long as the step is shorter than the TTL.
func (c *Confirmer) pruneLocked() {
	now := c.now()
	for k, e := range c.used {
		if e.out != nil && now.After(e.expires.Add(c.ttl)) {
			c.deleteLocked(k, e)
		}
	}
}

// finish records the outcome of a claimed token and wakes waiters. The
// first outcome wins.
func (c *Confirmer) finish(cl claims, out outcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.used[cl.nonce]
	if !ok {
		e = &replayEntry{expires: cl.expires, owner: cl.owner, done: make(chan struct{})}
		c.used[cl.nonce] = e
		c.perOwner[cl.owner]++
	}
	if e.out != nil {
		return
	}
	e.out = &out
	close(e.done)
}

// release frees a claim whose commit never reached the REST handler, so the
// token stays usable.
func (c *Confirmer) release(cl claims) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.used[cl.nonce]; ok && e.out == nil {
		c.deleteLocked(cl.nonce, e)
		close(e.done)
	}
}

// verify checks token against p, t and call without claiming it.
func (c *Confirmer) verify(token string, p Principal, t Tool, call Call) error {
	_, err := c.check(token, p, t, call)
	return err
}

func (c *Confirmer) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.used)
}
