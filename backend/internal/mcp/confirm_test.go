package mcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/api"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

var testKey = []byte("0123456789abcdef0123456789abcdef")

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newTestConfirmer() (*Confirmer, *testClock) {
	clock := &testClock{now: time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)}
	return NewConfirmer(testKey, 5*time.Minute, clock.Now), clock
}

func deleteCall() Call {
	return Call{Cluster: "prod", Args: map[string]any{"topic": "orders-old", "body": map[string]any{"confirmation": "orders-old"}}}
}

func TestConfirmRoundTrip(t *testing.T) {
	c, _ := newTestConfirmer()
	tool := mustTool(t, "kaflux_topic_delete")
	tok, exp := c.Mint(testPrincipal(), tool, deleteCall())
	if tok == "" || !exp.Equal(time.Date(2026, 11, 19, 12, 5, 0, 0, time.UTC)) {
		t.Fatalf("token %q expires %v", tok, exp)
	}
	// The confirmation argument itself is not part of the binding.
	call := deleteCall()
	call.Args["confirmation"] = tok
	if err := c.verify(tok, testPrincipal(), tool, call); err != nil {
		t.Fatalf("verify: %v", err)
	}
	// Map key order never changes the binding.
	other := Call{Cluster: "prod", Args: map[string]any{"body": map[string]any{"confirmation": "orders-old"}, "topic": "orders-old"}}
	tok2, _ := c.Mint(testPrincipal(), tool, other)
	if err := c.verify(tok2, testPrincipal(), tool, deleteCall()); err != nil {
		t.Fatalf("reordered args: %v", err)
	}
	// A token from another key (another process) is rejected.
	foreign, _ := NewConfirmer(nil, 5*time.Minute, nil).Mint(testPrincipal(), tool, deleteCall())
	if err := c.verify(foreign, testPrincipal(), tool, deleteCall()); !errors.Is(err, ErrConfirmationInvalid) {
		t.Fatalf("foreign key: %v", err)
	}
}

func TestConfirmExpired(t *testing.T) {
	c, clock := newTestConfirmer()
	tool := mustTool(t, "kaflux_topic_delete")
	tok, _ := c.Mint(testPrincipal(), tool, deleteCall())
	clock.Add(5*time.Minute + time.Second)
	if err := c.verify(tok, testPrincipal(), tool, deleteCall()); !errors.Is(err, ErrConfirmationExpired) {
		t.Fatalf("expired: %v", err)
	}
}

func TestConfirmArgsTampered(t *testing.T) {
	c, _ := newTestConfirmer()
	tool := mustTool(t, "kaflux_topic_delete")
	tok, _ := c.Mint(testPrincipal(), tool, deleteCall())
	for name, call := range map[string]Call{
		"path arg": {Cluster: "prod", Args: map[string]any{"topic": "orders", "body": map[string]any{"confirmation": "orders-old"}}},
		"body":     {Cluster: "prod", Args: map[string]any{"topic": "orders-old", "body": map[string]any{"confirmation": "orders"}}},
		"extra":    {Cluster: "prod", Args: map[string]any{"topic": "orders-old", "extra": 1, "body": map[string]any{"confirmation": "orders-old"}}},
		"cluster":  {Cluster: "stage", Args: deleteCall().Args},
	} {
		if err := c.verify(tok, testPrincipal(), tool, call); !errors.Is(err, ErrConfirmationMismatch) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestConfirmOtherPrincipal(t *testing.T) {
	c, _ := newTestConfirmer()
	tool := mustTool(t, "kaflux_topic_delete")
	tok, _ := c.Mint(testPrincipal(), tool, deleteCall())
	otherToken := testPrincipal()
	otherToken.TokenID = "tok-2"
	otherOwner := testPrincipal()
	otherOwner.Session.User.ID = "owner-2"
	for _, p := range []Principal{otherToken, otherOwner} {
		if err := c.verify(tok, p, tool, deleteCall()); !errors.Is(err, ErrConfirmationMismatch) {
			t.Fatalf("principal %s/%s: %v", p.Session.User.ID, p.TokenID, err)
		}
	}
}

func TestConfirmOtherTool(t *testing.T) {
	c, _ := newTestConfirmer()
	tok, _ := c.Mint(testPrincipal(), mustTool(t, "kaflux_topic_delete"), deleteCall())
	if err := c.verify(tok, testPrincipal(), mustTool(t, "kaflux_topic_truncate"), deleteCall()); !errors.Is(err, ErrConfirmationMismatch) {
		t.Fatalf("other tool: %v", err)
	}
}

func TestConfirmBitFlipRejected(t *testing.T) {
	c, _ := newTestConfirmer()
	tool := mustTool(t, "kaflux_topic_delete")
	tok, _ := c.Mint(testPrincipal(), tool, deleteCall())
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		for bit := 0; bit < 8; bit++ {
			flipped := append([]byte{}, raw...)
			flipped[i] ^= 1 << bit
			if err := c.verify(base64.RawURLEncoding.EncodeToString(flipped), testPrincipal(), tool, deleteCall()); !errors.Is(err, ErrConfirmationInvalid) {
				t.Fatalf("byte %d bit %d: %v", i, bit, err)
			}
		}
	}
	for _, bad := range []string{"", "x", tok + "A", tok[:len(tok)-1], "!!!!"} {
		if err := c.verify(bad, testPrincipal(), tool, deleteCall()); !errors.Is(err, ErrConfirmationInvalid) {
			t.Fatalf("%q: %v", bad, err)
		}
	}
}

func TestConfirmReplayCacheBounded(t *testing.T) {
	c, clock := newTestConfirmer()
	c.maxEntries = 32 // per-token cap 2
	tool := mustTool(t, "kaflux_topic_delete")
	use := func(p Principal) error {
		tok, _ := c.Mint(p, tool, deleteCall())
		cl, err := c.check(tok, p, tool, deleteCall())
		if err != nil {
			return err
		}
		if _, err := c.claim(context.Background(), cl); err != nil {
			return err
		}
		c.finish(cl, outcome{res: Result{Status: 200}})
		return nil
	}
	principal := func(i int) Principal {
		p := testPrincipal()
		p.TokenID = fmt.Sprintf("tok-%d", i)
		return p
	}
	// One token cannot take more than its share.
	for i := 0; i < 2; i++ {
		if err := use(principal(0)); err != nil {
			t.Fatal(err)
		}
	}
	if err := use(principal(0)); !errors.Is(err, ErrConfirmationBusy) {
		t.Fatalf("per-token cap: %v", err)
	}
	// Other tokens fill the global cap; full of live entries, refuse rather
	// than forget a used token.
	for i := 1; i < 16; i++ {
		for j := 0; j < 2; j++ {
			if err := use(principal(i)); err != nil {
				t.Fatalf("token %d: %v", i, err)
			}
		}
	}
	if err := use(principal(99)); !errors.Is(err, ErrConfirmationBusy) {
		t.Fatalf("full cache: %v", err)
	}
	// Expired entries stay for a grace TTL, then are pruned.
	clock.Add(6 * time.Minute)
	if err := use(principal(99)); !errors.Is(err, ErrConfirmationBusy) {
		t.Fatalf("within grace: %v", err)
	}
	clock.Add(5 * time.Minute)
	if err := use(principal(99)); err != nil {
		t.Fatalf("after grace: %v", err)
	}
	if n := c.size(); n != 1 {
		t.Fatalf("cache size %d", n)
	}
}

func TestConfirmClaimRechecksExpiry(t *testing.T) {
	c, clock := newTestConfirmer()
	tool := mustTool(t, "kaflux_topic_delete")
	tok, _ := c.Mint(testPrincipal(), tool, deleteCall())
	cl, err := c.check(tok, testPrincipal(), tool, deleteCall())
	if err != nil {
		t.Fatal(err)
	}
	clock.Add(5*time.Minute + time.Second) // expires between check and claim
	if _, err := c.claim(context.Background(), cl); !errors.Is(err, ErrConfirmationExpired) {
		t.Fatalf("claim after expiry: %v", err)
	}
}

func TestCommitSettlesOnPanic(t *testing.T) {
	// A panicking REST handler is recovered by dispatch and recorded as uncertain.
	d, _, _ := newConfirmDispatcher(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			jsonHandler(200, `{"data":{}}`)(w, r)
			return
		}
		panic("boom")
	}))
	tool := mustTool(t, "kaflux_topic_delete")
	out, e := d.Prepare(context.Background(), testPrincipal(), tool, deleteCall())
	if e != nil {
		t.Fatalf("prepare: %+v", e)
	}
	if _, e := d.Commit(context.Background(), testPrincipal(), tool, deleteCall(), out.Confirmation); e == nil || e.Code != "internal" {
		t.Fatalf("panicking handler: %+v", e)
	}
	if _, e := d.Commit(context.Background(), testPrincipal(), tool, deleteCall(), out.Confirmation); e == nil || e.Code != "outcome_uncertain" {
		t.Fatalf("replay after panic: %+v", e)
	}
	// A panic inside dispatch itself still settles the claim instead of
	// leaving it pending forever.
	orig := commitDo
	commitDo = func(*Dispatcher, context.Context, Principal, Tool, Call) (Result, *ToolError, bool) {
		panic("dispatch bug")
	}
	defer func() { commitDo = orig }()
	out, _ = d.Prepare(context.Background(), testPrincipal(), tool, deleteCall())
	if _, e := d.Commit(context.Background(), testPrincipal(), tool, deleteCall(), out.Confirmation); e == nil || e.Code != "internal" {
		t.Fatalf("dispatch panic: %+v", e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e := d.Commit(ctx, testPrincipal(), tool, deleteCall(), out.Confirmation); e == nil || e.Code != "outcome_uncertain" {
		t.Fatalf("replay after dispatch panic (wedged?): %+v", e)
	}
}

func TestPrepareDeniesWithoutGrant(t *testing.T) {
	s := &spy{}
	d, sink, _ := newConfirmDispatcher(s)
	tool := mustTool(t, "kaflux_topic_create")
	call := Call{Cluster: "prod", Args: map[string]any{"body": map[string]any{"name": "orders"}}}
	for name, grants := range map[string][]auth.Grant{
		"none":          {},
		"read only":     {{Role: "administrator", Cluster: "*", Action: "read", Pattern: "*"}},
		"other cluster": {{Role: "administrator", Cluster: "stage", Action: "*", Pattern: "*"}},
	} {
		p := testPrincipal()
		p.Grants = grants
		out, e := d.Prepare(context.Background(), p, tool, call)
		if e == nil || e.Status != 403 || e.Code != "permission_denied" || out.Confirmation != "" {
			t.Fatalf("%s: %+v %+v", name, out, e)
		}
	}
	// A destructive tool with a preview is denied before the preview runs.
	p := testPrincipal()
	p.Grants = []auth.Grant{{Role: "administrator", Cluster: "*", Action: "create", Pattern: "*"}}
	if _, e := d.Prepare(context.Background(), p, mustTool(t, "kaflux_topic_delete"), deleteCall()); e == nil || e.Status != 403 {
		t.Fatalf("destructive with write grant: %+v", e)
	}
	if len(sink.rows) != 0 || len(s.seen()) != 0 {
		t.Fatalf("denied prepares audited %v or reached REST %v", sink.rows, s.seen())
	}
	p.Grants = []auth.Grant{{Role: "administrator", Cluster: "prod", Action: "create", Pattern: "*"}}
	if out, e := d.Prepare(context.Background(), p, tool, call); e != nil || out.Confirmation == "" || len(sink.rows) != 1 {
		t.Fatalf("granted prepare: %+v %+v rows %d", out, e, len(sink.rows))
	}
}

func TestPrepareRateLimited(t *testing.T) {
	s := &spy{}
	d := NewDispatcher(s, Limits{PerMinute: 2})
	c, _ := newTestConfirmer()
	d.SetConfirmer(c)
	sink := &auditSink{}
	d.SetAudit(sink.Audit)
	tool := mustTool(t, "kaflux_topic_create")
	call := Call{Cluster: "prod", Args: map[string]any{"body": map[string]any{"name": "orders"}}}
	for i := 0; i < 2; i++ {
		if _, e := d.Prepare(context.Background(), testPrincipal(), tool, call); e != nil {
			t.Fatalf("prepare %d: %+v", i, e)
		}
	}
	out, e := d.Prepare(context.Background(), testPrincipal(), tool, call)
	if e == nil || e.Code != "rate_limited" || e.Status != 429 || out.Confirmation != "" || len(sink.rows) != 2 {
		t.Fatalf("third prepare: %+v %+v rows %d", out, e, len(sink.rows))
	}
}

// spy is a REST handler that records every method it sees.
type spy struct {
	mu      sync.Mutex
	methods []string
	bodies  []string
	writes  atomic.Int32
	delay   time.Duration
	get     string // body for GETs
	status  int    // status for writes
}

func (s *spy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b := make([]byte, 1<<16)
	n, _ := r.Body.Read(b)
	s.mu.Lock()
	s.methods = append(s.methods, r.Method+" "+r.URL.Path)
	s.bodies = append(s.bodies, string(b[:n]))
	s.mu.Unlock()
	if r.Method == http.MethodGet {
		get := s.get
		if get == "" {
			get = `{"data":{"name":"orders-old","partitions":6}}`
		}
		jsonHandler(200, get)(w, r)
		return
	}
	s.writes.Add(1)
	time.Sleep(s.delay)
	status := s.status
	if status == 0 {
		status = 200
	}
	jsonHandler(status, fmt.Sprintf(`{"data":{"n":%d}}`, s.writes.Load()))(w, r)
}

func (s *spy) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string{}, s.methods...)
}

type auditSink struct {
	mu   sync.Mutex
	rows []store.Audit
	err  error
}

func (a *auditSink) Audit(_ context.Context, row store.Audit) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	a.rows = append(a.rows, row)
	return nil
}

func newConfirmDispatcher(h http.Handler) (*Dispatcher, *auditSink, *testClock) {
	d := NewDispatcher(h, Limits{})
	c, clock := newTestConfirmer()
	sink := &auditSink{}
	d.SetConfirmer(c)
	d.SetAudit(sink.Audit)
	return d, sink, clock
}

func TestPrepareDoesNotMutate(t *testing.T) {
	s := &spy{}
	d, _, _ := newConfirmDispatcher(s)
	for _, tc := range []struct {
		tool string
		call Call
	}{
		{"kaflux_topic_delete", deleteCall()},
		{"kaflux_topic_truncate", deleteCall()},
		{"kaflux_topic_bulk_delete", Call{Cluster: "prod", Args: map[string]any{"body": map[string]any{"topics": []any{"a", "b"}, "confirmation": "delete 2 topics"}}}},
		{"kaflux_rebalance_execute", Call{Cluster: "prod", Args: map[string]any{"job": "job-1", "body": map[string]any{"confirmation": true, "planHash": "h"}}}},
		{"kaflux_acl_delete", Call{Cluster: "prod", Args: map[string]any{"body": map[string]any{"acl": map[string]any{"resourceType": "TOPIC", "resourceName": "orders"}, "confirmation": true}}}},
		{"kaflux_message_produce", Call{Cluster: "prod", Args: map[string]any{"body": map[string]any{"topic": "orders", "value": "x"}}}},
	} {
		if _, e := d.Prepare(context.Background(), testPrincipal(), mustTool(t, tc.tool), tc.call); e != nil {
			t.Fatalf("%s prepare: %+v", tc.tool, e)
		}
	}
	seen := s.seen()
	if len(seen) == 0 {
		t.Fatal("no preview reads happened")
	}
	for _, m := range seen {
		if !strings.HasPrefix(m, "GET ") {
			t.Fatalf("prepare issued %s", m)
		}
	}
	if s.writes.Load() != 0 {
		t.Fatalf("writes %d", s.writes.Load())
	}
}

func TestPrepareWritesAuditRow(t *testing.T) {
	d, sink, _ := newConfirmDispatcher(&spy{})
	out, e := d.Prepare(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_delete"), deleteCall())
	if e != nil {
		t.Fatalf("prepare: %+v", e)
	}
	if len(sink.rows) != 1 {
		t.Fatalf("audit rows %+v", sink.rows)
	}
	row := sink.rows[0]
	if row.Action != "mcp.prepare:kaflux_topic_delete" || row.Actor != "owner-1/ci-bot" || row.Provider != "mcp" || row.SourceIP != "203.0.113.9" || row.ClusterID != "prod" || row.Result != "prepared" || row.Resource != "/api/v1/clusters/prod/topics/orders-old" {
		t.Fatalf("audit row %+v", row)
	}
	if out.Confirmation == "" || !out.Destructive || out.Plan.Method != "DELETE" || out.Plan.Path != "/api/v1/clusters/prod/topics/orders-old" || out.Plan.ScopeRequired != "destructive" {
		t.Fatalf("prepare output %+v", out)
	}
	if !strings.Contains(out.RestConfirmation, `"orders-old"`) {
		t.Fatalf("rest confirmation hint %q", out.RestConfirmation)
	}
	// Failing to persist the audit row fails the prepare and yields no token.
	sink.err = errors.New("disk full")
	out, e = d.Prepare(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_delete"), deleteCall())
	if e == nil || out.Confirmation != "" {
		t.Fatalf("prepare with failing audit: %+v %+v", out, e)
	}
	// No audit sink configured: prepare refuses too.
	bare := NewDispatcher(&spy{}, Limits{})
	if _, e := bare.Prepare(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_delete"), deleteCall()); e == nil {
		t.Fatal("prepare without audit sink succeeded")
	}
}

func TestBulkCap(t *testing.T) {
	d, sink, _ := newConfirmDispatcher(&spy{})
	topics := func(n int) []any {
		out := make([]any, n)
		for i := range out {
			out[i] = fmt.Sprintf("t%d", i)
		}
		return out
	}
	tool := mustTool(t, "kaflux_topic_bulk_delete")
	call := func(n int) Call {
		return Call{Cluster: "prod", Args: map[string]any{"body": map[string]any{"topics": topics(n), "minAgeDays": 30, "confirmation": fmt.Sprintf("delete %d topics", n)}}}
	}
	_, e := d.Prepare(context.Background(), testPrincipal(), tool, call(51))
	if e == nil || e.Code != "too_many_targets" || !strings.Contains(e.Hint, "50") {
		t.Fatalf("51 targets: %+v", e)
	}
	if len(sink.rows) != 0 {
		t.Fatal("refused prepare was audited as prepared")
	}
	out, e := d.Prepare(context.Background(), testPrincipal(), tool, call(50))
	if e != nil {
		t.Fatalf("50 targets: %+v", e)
	}
	if !strings.Contains(out.RestConfirmation, `"delete 50 topics"`) {
		t.Fatalf("bulk hint %q", out.RestConfirmation)
	}
	// Commit enforces the cap too, whatever was minted.
	tok, _ := d.confirm.Mint(testPrincipal(), tool, call(51))
	if _, e := d.Commit(context.Background(), testPrincipal(), tool, call(51), tok); e == nil || e.Code != "too_many_targets" {
		t.Fatalf("commit 51: %+v", e)
	}
}

func TestPrepareRunsPreviewHook(t *testing.T) {
	s := &spy{}
	d, _, _ := newConfirmDispatcher(s)
	tool := mustTool(t, "kaflux_topic_delete")
	var hookCalls int
	tool.Preview = func(ctx context.Context, d *Dispatcher, c Call) (any, error) {
		hookCalls++
		if c.Args["topic"] != "orders-old" {
			return nil, fmt.Errorf("args %v", c.Args)
		}
		return map[string]any{"marker": true}, nil
	}
	out, e := d.Prepare(context.Background(), testPrincipal(), tool, deleteCall())
	if e != nil || hookCalls != 1 {
		t.Fatalf("prepare %+v calls %d", e, hookCalls)
	}
	if m, _ := out.Preview.(map[string]any); m["marker"] != true {
		t.Fatalf("preview %#v", out.Preview)
	}
	// The catalog hook for topic_delete reads the topic through kaflux_topic_get.
	out, e = d.Prepare(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_delete"), deleteCall())
	if e != nil {
		t.Fatalf("prepare: %+v", e)
	}
	b, _ := json.Marshal(out.Preview)
	if !strings.Contains(string(b), `"partitions":6`) || !strings.Contains(strings.Join(s.seen(), ","), "GET /api/v1/clusters/prod/topics/orders-old") {
		t.Fatalf("preview %s seen %v", b, s.seen())
	}
	// A failing hook fails the prepare.
	tool.Preview = func(context.Context, *Dispatcher, Call) (any, error) {
		return nil, &ToolError{Code: "not_found", Message: "Topic not found", Status: 404}
	}
	if _, e := d.Prepare(context.Background(), testPrincipal(), tool, deleteCall()); e == nil || e.Code != "not_found" {
		t.Fatalf("failing hook: %+v", e)
	}
	// Tools without a hook echo their arguments.
	out, e = d.Prepare(context.Background(), testPrincipal(), mustTool(t, "kaflux_message_produce"), Call{Cluster: "prod", Args: map[string]any{"body": map[string]any{"topic": "orders"}}})
	if e != nil || out.Preview != nil || out.Plan.Arguments["body"] == nil {
		t.Fatalf("echo: %+v %+v", out, e)
	}
}

func demoDispatcher(t *testing.T) (*Dispatcher, string) {
	t.Helper()
	s, err := store.New(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	a := api.New(api.Options{Demo: true, Store: s, Providers: map[string]kafka.Provider{"demo": kafka.NewDemo()}, Clusters: []model.Cluster{{ID: "demo"}}})
	d := NewDispatcher(a, Limits{})
	c, _ := newTestConfirmer()
	d.SetConfirmer(c)
	d.SetAudit(s.Audit)
	res, e := d.Do(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_list"), Call{Cluster: "demo", Args: map[string]any{"limit": float64(1)}})
	if e != nil {
		t.Fatalf("topic list: %+v", e)
	}
	var listed struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.Body, &listed); err != nil || len(listed.Data) == 0 {
		t.Fatalf("topics %s", res.Body)
	}
	return d, listed.Data[0].Name
}

func TestPreparePreviewUsesCallerGrants(t *testing.T) {
	d, topic := demoDispatcher(t)
	call := Call{Cluster: "demo", Args: map[string]any{"topic": topic, "body": map[string]any{"confirmation": topic}}}
	out, e := d.Prepare(context.Background(), testPrincipal(), mustTool(t, "kaflux_topic_delete"), call)
	if e != nil || out.Preview == nil {
		t.Fatalf("admin prepare: %+v %+v", out, e)
	}
	// A principal with no grants cannot read, so the preview and prepare fail.
	p := testPrincipal()
	p.Grants = []auth.Grant{}
	if _, e := d.Prepare(context.Background(), p, mustTool(t, "kaflux_topic_delete"), call); e == nil || e.Status != 403 {
		t.Fatalf("ungranted prepare: %+v", e)
	}
}

func TestConfirmReplayReturnsPriorResult(t *testing.T) {
	s := &spy{}
	d, _, _ := newConfirmDispatcher(s)
	tool := mustTool(t, "kaflux_topic_delete")
	out, e := d.Prepare(context.Background(), testPrincipal(), tool, deleteCall())
	if e != nil {
		t.Fatalf("prepare: %+v", e)
	}
	first, e := d.Commit(context.Background(), testPrincipal(), tool, deleteCall(), out.Confirmation)
	if e != nil {
		t.Fatalf("commit: %+v", e)
	}
	second, e := d.Commit(context.Background(), testPrincipal(), tool, deleteCall(), out.Confirmation)
	if e != nil {
		t.Fatalf("replay: %+v", e)
	}
	if string(first.Body) != string(second.Body) || first.Status != second.Status || s.writes.Load() != 1 {
		t.Fatalf("first %s second %s writes %d", first.Body, second.Body, s.writes.Load())
	}
	// REST confirmations are passed through untouched, never auto-filled.
	s.mu.Lock()
	lastBody := s.bodies[len(s.bodies)-1]
	s.mu.Unlock()
	if lastBody != `{"confirmation":"orders-old"}` {
		t.Fatalf("rest body %q", lastBody)
	}
	// Another principal never gets the recorded result.
	other := testPrincipal()
	other.TokenID = "tok-2"
	if _, e := d.Commit(context.Background(), other, tool, deleteCall(), out.Confirmation); e == nil || e.Code != "confirmation_mismatch" {
		t.Fatalf("other principal replay: %+v", e)
	}
	// Failed outcomes are recorded and replayed too, without re-running.
	s.status = 422
	out, _ = d.Prepare(context.Background(), testPrincipal(), tool, deleteCall())
	_, e1 := d.Commit(context.Background(), testPrincipal(), tool, deleteCall(), out.Confirmation)
	_, e2 := d.Commit(context.Background(), testPrincipal(), tool, deleteCall(), out.Confirmation)
	if e1 == nil || e2 == nil || e1.Code != e2.Code || s.writes.Load() != 2 {
		t.Fatalf("failed replay %+v %+v writes %d", e1, e2, s.writes.Load())
	}
}

func TestConfirmUncertainOutcomeRecorded(t *testing.T) {
	s := &spy{delay: 200 * time.Millisecond}
	d := NewDispatcher(s, Limits{Timeout: 20 * time.Millisecond})
	c, _ := newTestConfirmer()
	d.SetConfirmer(c)
	d.SetAudit((&auditSink{}).Audit)
	tool := mustTool(t, "kaflux_topic_delete")
	out, e := d.Prepare(context.Background(), testPrincipal(), tool, deleteCall())
	if e != nil {
		t.Fatalf("prepare: %+v", e)
	}
	_, e1 := d.Commit(context.Background(), testPrincipal(), tool, deleteCall(), out.Confirmation)
	if e1 == nil || e1.Code != "timeout" {
		t.Fatalf("first commit %+v", e1)
	}
	_, e2 := d.Commit(context.Background(), testPrincipal(), tool, deleteCall(), out.Confirmation)
	if e2 == nil || e2.Code != "outcome_uncertain" || e2.Hint != writeUncertainHint || s.writes.Load() != 1 {
		t.Fatalf("replay %+v writes %d", e2, s.writes.Load())
	}
}

func TestConfirmConcurrentCommitOnce(t *testing.T) {
	s := &spy{delay: 30 * time.Millisecond}
	d, _, _ := newConfirmDispatcher(s)
	tool := mustTool(t, "kaflux_topic_delete")
	out, e := d.Prepare(context.Background(), testPrincipal(), tool, deleteCall())
	if e != nil {
		t.Fatalf("prepare: %+v", e)
	}
	const n = 8
	var wg sync.WaitGroup
	bodies := make([]string, n)
	errs := make([]*ToolError, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, e := d.Commit(context.Background(), testPrincipal(), tool, deleteCall(), out.Confirmation)
			bodies[i], errs[i] = string(res.Body), e
		}(i)
	}
	wg.Wait()
	if s.writes.Load() != 1 {
		t.Fatalf("REST handler ran %d times", s.writes.Load())
	}
	for i := range bodies {
		if errs[i] != nil || bodies[i] != bodies[0] {
			t.Fatalf("commit %d: %q %+v (first %q)", i, bodies[i], errs[i], bodies[0])
		}
	}
}

func TestCommitRejectsBadConfirmation(t *testing.T) {
	s := &spy{}
	d, _, clock := newConfirmDispatcher(s)
	tool := mustTool(t, "kaflux_topic_delete")
	out, _ := d.Prepare(context.Background(), testPrincipal(), tool, deleteCall())
	for name, tc := range map[string]struct {
		tool  Tool
		call  Call
		token string
		code  string
	}{
		"missing":  {tool, deleteCall(), "", "confirmation_required"},
		"garbage":  {tool, deleteCall(), "abc", "confirmation_invalid"},
		"tampered": {tool, Call{Cluster: "prod", Args: map[string]any{"topic": "orders", "body": map[string]any{"confirmation": "orders"}}}, out.Confirmation, "confirmation_mismatch"},
		"tool":     {mustTool(t, "kaflux_topic_truncate"), deleteCall(), out.Confirmation, "confirmation_mismatch"},
		"read":     {mustTool(t, "kaflux_topic_get"), Call{Cluster: "prod", Args: map[string]any{"topic": "a"}}, out.Confirmation, "invalid_argument"},
	} {
		if _, e := d.Commit(context.Background(), testPrincipal(), tc.tool, tc.call, tc.token); e == nil || e.Code != tc.code || e.Hint == "" {
			t.Fatalf("%s: %+v", name, e)
		}
	}
	clock.Add(5*time.Minute + time.Second)
	if _, e := d.Commit(context.Background(), testPrincipal(), tool, deleteCall(), out.Confirmation); e == nil || e.Code != "confirmation_expired" {
		t.Fatalf("expired: %+v", e)
	}
	if s.writes.Load() != 0 {
		t.Fatalf("rejected commits wrote %d times", s.writes.Load())
	}
}

func TestRestConfirmationHints(t *testing.T) {
	for name, tc := range map[string]struct {
		call Call
		want string
	}{
		"kaflux_topic_recreate":      {deleteCall(), `"orders-old"`},
		"kaflux_acl_delete":          {Call{Cluster: "prod"}, "true"},
		"kaflux_rebalance_execute":   {Call{Cluster: "prod", Args: map[string]any{"job": "j"}}, "planHash"},
		"kaflux_group_reset_offsets": {Call{Cluster: "prod", Args: map[string]any{"group": "billing", "body": map[string]any{"preview": false}}}, `"billing"`},
		"kaflux_message_produce":     {Call{Cluster: "prod"}, "none"},
	} {
		if got := restConfirmation(mustTool(t, name), tc.call); !strings.Contains(got, tc.want) {
			t.Fatalf("%s: %q lacks %q", name, got, tc.want)
		}
	}
}

func TestPreviewReadRefusesWrites(t *testing.T) {
	d, _, _ := newConfirmDispatcher(&spy{})
	ctx := withPrincipal(context.Background(), testPrincipal())
	if _, err := d.PreviewRead(ctx, "kaflux_topic_delete", Call{Cluster: "prod"}, map[string]any{"topic": "a"}); err == nil {
		t.Fatal("preview ran a write tool")
	}
	if _, err := d.PreviewRead(context.Background(), "kaflux_topic_get", Call{Cluster: "prod"}, map[string]any{"topic": "a"}); err == nil {
		t.Fatal("preview ran without a caller")
	}
}

func TestACLDeletePreviewMatchesBinding(t *testing.T) {
	d, _ := demoDispatcher(t)
	acl := map[string]any{"resourceType": "TOPIC", "resourceName": "orders.", "patternType": "PREFIXED", "principal": "User:preview-check", "host": "*", "operation": "READ", "permission": "ALLOW"}
	if _, e := d.Do(context.Background(), testPrincipal(), mustTool(t, "kaflux_acl_create"), Call{Cluster: "demo", Args: map[string]any{"body": map[string]any{"acl": acl, "confirmation": true}}}); e != nil {
		t.Fatalf("acl create: %+v", e)
	}
	out, e := d.Prepare(context.Background(), testPrincipal(), mustTool(t, "kaflux_acl_delete"), Call{Cluster: "demo", Args: map[string]any{"body": map[string]any{"acl": acl, "confirmation": true}}})
	if e != nil {
		t.Fatalf("prepare: %+v", e)
	}
	if m, _ := out.Preview.(map[string]any); m["count"] != 1 {
		b, _ := json.Marshal(out.Preview)
		t.Fatalf("preview %s", b)
	}
}
