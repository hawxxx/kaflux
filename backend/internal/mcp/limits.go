package mcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"
)

// Limits bound one token's use of the dispatcher. Zero PerMinute or
// Concurrency disables that limit; zero MaxBytes or Timeout takes the default.
type Limits struct {
	MaxBytes    int           // response cap, default 256 KiB
	Timeout     time.Duration // per-call deadline, default 30 s
	PerMinute   int           // token-bucket rate per token
	Concurrency int           // calls in flight per token
}

const (
	DefaultMaxBytes = 256 << 10
	DefaultTimeout  = 30 * time.Second
	// MaxRequestBody mirrors the REST body limit.
	MaxRequestBody = 1 << 20
	// maxCapture bounds the handler output held in memory before truncation.
	maxCapture = 32 << 20
	// bucketPruneAt is the bucket count above which idle buckets are dropped.
	bucketPruneAt = 4096
)

func (l Limits) withDefaults() Limits {
	if l.MaxBytes <= 0 {
		l.MaxBytes = DefaultMaxBytes
	}
	if l.Timeout <= 0 {
		l.Timeout = DefaultTimeout
	}
	return l
}

type bucket struct {
	tokens float64
	last   time.Time
}

// limiter holds per-token rate buckets and in-flight counts in memory.
type limiter struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	inflight map[string]int
}

func newLimiter() *limiter {
	return &limiter{buckets: map[string]*bucket{}, inflight: map[string]int{}}
}

// allow takes one token from the key's bucket. It returns the wait until a
// token is available when the bucket is empty.
func (l *limiter) allow(key string, perMinute int, now time.Time) (time.Duration, bool) {
	if perMinute <= 0 {
		return 0, true
	}
	capacity := float64(perMinute)
	rate := capacity / 60 // tokens per second
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.buckets) > bucketPruneAt {
		for k, b := range l.buckets {
			if now.Sub(b.last) >= time.Minute {
				delete(l.buckets, k)
			}
		}
	}
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: capacity, last: now}
		l.buckets[key] = b
	}
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = math.Min(capacity, b.tokens+elapsed*rate)
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return 0, true
	}
	wait := time.Duration(math.Ceil((1-b.tokens)/rate)) * time.Second
	return wait, false
}

// acquire reserves an in-flight slot; release must follow a true result.
func (l *limiter) acquire(key string, max int) bool {
	if max <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inflight[key] >= max {
		return false
	}
	l.inflight[key]++
	return true
}

func (l *limiter) release(key string, max int) {
	if max <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inflight[key]--; l.inflight[key] <= 0 {
		delete(l.inflight, key)
	}
}

// listView locates the list in a REST response: a top-level array, the
// envelope's data array, or the single array inside an object data (for
// example rebalance events). rebuild re-encodes the response around items.
type listView struct {
	items   []json.RawMessage
	top     map[string]json.RawMessage // nil for a top-level array
	inner   map[string]json.RawMessage // data object holding the list, if any
	innerAt string
}

func findList(body []byte) (*listView, bool) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var items []json.RawMessage
		if json.Unmarshal(trimmed, &items) != nil {
			return nil, false
		}
		return &listView{items: items}, true
	}
	var top map[string]json.RawMessage
	if json.Unmarshal(trimmed, &top) != nil || top == nil {
		return nil, false
	}
	data := bytes.TrimSpace(top["data"])
	if len(data) == 0 {
		return nil, false
	}
	if data[0] == '[' {
		var items []json.RawMessage
		if json.Unmarshal(data, &items) != nil {
			return nil, false
		}
		return &listView{items: items, top: top}, true
	}
	var inner map[string]json.RawMessage
	if json.Unmarshal(data, &inner) != nil {
		return nil, false
	}
	at := ""
	for k, v := range inner {
		if v = bytes.TrimSpace(v); len(v) > 0 && v[0] == '[' {
			if at != "" {
				return nil, false // ambiguous: more than one list
			}
			at = k
		}
	}
	if at == "" {
		return nil, false
	}
	var items []json.RawMessage
	if json.Unmarshal(inner[at], &items) != nil {
		return nil, false
	}
	return &listView{items: items, top: top, inner: inner, innerAt: at}, true
}

// rebuild encodes the response with items in place of the list and extra
// top-level fields (ignored for a top-level array).
func (v *listView) rebuild(items []json.RawMessage, extra map[string]any) []byte {
	if items == nil {
		items = []json.RawMessage{}
	}
	list, _ := json.Marshal(items)
	if v.top == nil {
		return list
	}
	top := make(map[string]any, len(v.top)+len(extra))
	for k, x := range v.top {
		top[k] = x
	}
	if v.inner != nil {
		inner := make(map[string]json.RawMessage, len(v.inner))
		for k, x := range v.inner {
			inner[k] = x
		}
		inner[v.innerAt] = list
		top["data"] = inner
	} else {
		top["data"] = json.RawMessage(list)
	}
	for k, x := range extra {
		top[k] = x
	}
	out, _ := json.Marshal(top)
	return out
}

// fitItems returns how many leading items fit when the response is encoded
// with them, so the result stays within max bytes.
func (v *listView) fitItems(items []json.RawMessage, max int, extra func(kept int) map[string]any) int {
	base := len(v.rebuild(nil, extra(0)))
	size, n := base, 0
	for n < len(items) {
		add := len(items[n])
		if n > 0 {
			add++ // comma
		}
		if size+add > max {
			break
		}
		size += add
		n++
	}
	// The cursor in extra grows with n; when the estimate overshoots, binary
	// search the largest count that fits (size is monotonic in n).
	if n == 0 || len(v.rebuild(items[:n], extra(n))) <= max {
		return n
	}
	lo, hi := 0, n-1 // lo always fits (or is 0)
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if len(v.rebuild(items[:mid], extra(mid))) <= max {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo
}

// preview replaces an oversized non-list response with a valid JSON object
// holding a prefix of the raw text and a narrowing hint, within max bytes.
func preview(body []byte, max int) []byte {
	hint := fmt.Sprintf("Response exceeded %d bytes and was cut; narrow your query (fewer items, a filter, or a more specific tool).", max)
	keep := max / 2
	for keep >= 0 {
		text := bytes.ToValidUTF8(body[:min(keep, len(body))], nil)
		out, _ := json.Marshal(map[string]any{"truncated": true, "hint": hint, "preview": string(text)})
		if len(out) <= max {
			return out
		}
		if keep == 0 {
			break
		}
		keep = keep * 3 / 4
	}
	out, _ := json.Marshal(map[string]any{"truncated": true})
	return out
}
