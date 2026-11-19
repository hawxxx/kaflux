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
	done    chan struct{} // closed when the outcome is recorded or the claim released
	out     *outcome      // nil while pending, or after a released claim
}

// Confirmer mints and verifies the confirmation tokens of the two-phase
// write flow, and remembers each used token's outcome until it expires so a
// replayed commit returns the original result instead of writing again.
type Confirmer struct {
	key        []byte
	ttl        time.Duration
	now        func() time.Time
	mu         sync.Mutex
	used       map[[nonceLen]byte]*replayEntry
	maxEntries int
}

// NewConfirmer returns a Confirmer signing with key. An empty key draws a
// random per-process key, so tokens do not survive a restart and are not
// accepted by other replicas. ttl <= 0 takes DefaultConfirmTTL; nil now
// takes time.Now.
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
	return &Confirmer{key: key, ttl: ttl, now: now, used: map[[nonceLen]byte]*replayEntry{}, maxEntries: defaultReplayEntries}
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
	return cl, nil
}

// nonceOf extracts the nonce of an authentic token.
func (c *Confirmer) nonceOf(token string) ([nonceLen]byte, time.Time, bool) {
	var n [nonceLen]byte
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(raw) != tokenLen || !hmac.Equal(raw[payloadLen:], c.mac(raw[:payloadLen])) {
		return n, time.Time{}, false
	}
	copy(n[:], raw[9:9+nonceLen])
	return n, time.Unix(int64(binary.BigEndian.Uint64(raw[1:9])), 0), true
}

// claim reserves a verified token for one commit. It returns the recorded
// outcome when the token was already used, waiting for an in-flight commit
// with the same token to finish first.
func (c *Confirmer) claim(ctx context.Context, cl claims) (*outcome, error) {
	for {
		c.mu.Lock()
		e, ok := c.used[cl.nonce]
		if !ok {
			if len(c.used) >= c.maxEntries {
				c.pruneLocked()
			}
			if len(c.used) >= c.maxEntries {
				c.mu.Unlock()
				return nil, ErrConfirmationBusy
			}
			c.used[cl.nonce] = &replayEntry{expires: cl.expires, done: make(chan struct{})}
			c.mu.Unlock()
			return nil, nil
		}
		if e.out != nil {
			out := e.out
			c.mu.Unlock()
			return out, nil
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

// pruneLocked drops entries whose token has expired; such tokens fail
// verification anyway, so forgetting them cannot enable a replay.
func (c *Confirmer) pruneLocked() {
	now := c.now()
	for k, e := range c.used {
		if !now.Before(e.expires) && (e.out != nil || isClosed(e.done)) {
			delete(c.used, k)
		}
	}
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// finish records the outcome of a claimed token and wakes waiters.
func (c *Confirmer) finish(nonce [nonceLen]byte, expires time.Time, out outcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.used[nonce]
	if !ok {
		e = &replayEntry{expires: expires, done: make(chan struct{})}
		c.used[nonce] = e
	}
	if e.out != nil {
		return
	}
	e.out = &out
	if !isClosed(e.done) {
		close(e.done)
	}
}

// release frees a claim whose commit never reached the REST handler, so the
// token stays usable.
func (c *Confirmer) release(nonce [nonceLen]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.used[nonce]; ok && e.out == nil {
		delete(c.used, nonce)
		close(e.done)
	}
}

// Verify checks token against p, t and call. A token used before returns
// its recorded result (or, when the first commit failed, that error). A nil
// prior with a nil error claims the token: the caller must then Record the
// outcome.
func (c *Confirmer) Verify(token string, p Principal, t Tool, call Call) (prior *Result, err error) {
	cl, err := c.check(token, p, t, call)
	if err != nil {
		return nil, err
	}
	out, err := c.claim(context.Background(), cl)
	if err != nil || out == nil {
		return nil, err
	}
	if out.terr != nil {
		return nil, out.terr
	}
	res := out.res
	return &res, nil
}

// Record stores the result of the commit with token, so later commits with
// it return r without writing again.
func (c *Confirmer) Record(token string, r Result) {
	if nonce, exp, ok := c.nonceOf(token); ok {
		c.finish(nonce, exp, outcome{res: r})
	}
}

func (c *Confirmer) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.used)
}
