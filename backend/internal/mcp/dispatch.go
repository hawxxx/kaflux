package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/api"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

// Dispatcher runs tool calls against the in-process REST handler, so every
// call takes the same authorization, validation and audit path as the console.
type Dispatcher struct {
	handler http.Handler
	limits  Limits
	limiter *limiter
	now     func() time.Time
	confirm *Confirmer
	audit   AuditFunc
}

// AuditFunc persists one audit row, as store.Store.Audit does.
type AuditFunc func(context.Context, store.Audit) error

// NewDispatcher wraps the REST handler h (the api.API) with limits l.
func NewDispatcher(h http.Handler, l Limits) *Dispatcher {
	return &Dispatcher{handler: h, limits: l.withDefaults(), limiter: newLimiter(), now: time.Now, confirm: NewConfirmer(nil, DefaultConfirmTTL, nil)}
}

// SetConfirmer replaces the default per-process confirmer, e.g. with one
// keyed by a shared secret so tokens work across replicas. Call before use.
func (d *Dispatcher) SetConfirmer(c *Confirmer) { d.confirm = c }

// SetAudit sets where Prepare records its audit rows. Without one, Prepare
// refuses, because every prepared write must be audited. Call before use.
func (d *Dispatcher) SetAudit(f AuditFunc) { d.audit = f }

// Principal is the authenticated MCP caller: the token owner's session and
// the grants narrowed by the token's scopes and cluster allowlist.
type Principal struct {
	Session            auth.Session // the owner's identity; dispatch derives the MCP identity
	Grants             []auth.Grant
	TokenID, TokenName string
	RemoteAddr         string // the MCP HTTP request's RemoteAddr, for audit source IP
}

// Result is a successful REST response. Body is valid JSON, already paged
// and capped. Total and Returned describe the list when the body holds one.
type Result struct {
	Status          int
	Body            json.RawMessage
	Truncated       bool
	Total, Returned int
}

// ToolError is a failed call, safe to show to the agent: Message is the
// sanitized REST message and Hint says what to do next.
type ToolError struct {
	Code, Message, Hint string
	Status              int
}

func (e *ToolError) Error() string { return e.Code + ": " + e.Message }

func invalid(format string, a ...any) *ToolError {
	return &ToolError{Code: "invalid_argument", Message: fmt.Sprintf(format, a...), Hint: "Check the arguments against the tool's input schema.", Status: 400}
}

// writeUncertainHint is the guidance after a write timed out or hit an
// unavailable dependency: it may have been applied, so blind retry is unsafe.
const writeUncertainHint = "The write may have been applied. Read the current state with the matching get or list tool before retrying; do not retry blindly."

var errNotDispatchable = &ToolError{Code: "internal", Message: "Tool is not dispatchable", Hint: "This is a Kaflux defect; report it.", Status: 500}

// mcpSession derives the session the REST handler sees: provider mcp and
// actor <owner id>/<token name>, without the owner's console handle or CSRF.
func mcpSession(p Principal) auth.Session {
	name := p.TokenName
	if name == "" {
		name = p.TokenID
	}
	u := p.Session.User
	u.Roles = append([]string{}, u.Roles...)
	u.ID = u.ID + "/" + name
	u.Provider = "mcp"
	return auth.Session{ID: "mcp:" + p.TokenID, User: u, Expires: p.Session.Expires}
}

// refused reports whether a REST path is one MCP must never reach.
func refused(p string) bool {
	if path.Clean(p) != p {
		return true
	}
	for _, prefix := range []string{APIPrefix + "/auth", APIPrefix + "/admin"} {
		if p == prefix || strings.HasPrefix(p, prefix+"/") {
			return true
		}
	}
	return false
}

// argAllowed reports whether name is a valid argument of t.
func argAllowed(t Tool, name string) bool {
	switch name {
	case "limit", "cursor":
		return t.Paginated
	case "search":
		return t.Search || hasQuery(t, "search")
	case "confirmation":
		return t.IsWrite() || hasQuery(t, "confirmation")
	case "body":
		op, _ := operation(t.Method, t.Path)
		return op != nil && deref(obj(op["requestBody"])) != nil
	}
	_, params := operation(t.Method, t.Path)
	for _, p := range params {
		if p["name"] == name && (p["in"] == "path" || p["in"] == "query") {
			return true
		}
	}
	return false
}

func hasQuery(t Tool, name string) bool {
	_, params := operation(t.Method, t.Path)
	for _, p := range params {
		if p["name"] == name && p["in"] == "query" {
			return true
		}
	}
	return false
}

func intArg(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		if n != float64(int64(n)) {
			return 0, false
		}
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil
	case string:
		i, err := strconv.Atoi(n)
		return i, err == nil
	}
	return 0, false
}

func scalar(name string, v any) (string, *ToolError) {
	switch x := v.(type) {
	case string:
		return x, nil
	case bool:
		return strconv.FormatBool(x), nil
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), nil
	case int:
		return strconv.Itoa(x), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case json.Number:
		return x.String(), nil
	}
	return "", invalid("argument %q must be a string, number or boolean", name)
}

// paging is the resolved limit/offset/search of a paginated call.
type paging struct {
	limit, offset int
	search        string
	native        bool // the route pages itself through page/pageSize
	nativeSearch  bool // the route filters itself through q
	skip          int  // items to drop from the native page to reach offset
}

func resolvePaging(t Tool, args map[string]any) (paging, *ToolError) {
	pg := paging{limit: DefaultPageSize, native: hasQuery(t, "page") && hasQuery(t, "pageSize"), nativeSearch: hasQuery(t, "q")}
	limit, hasLimit := args["limit"]
	if !hasLimit && pg.native {
		limit, hasLimit = args["pageSize"]
	}
	if hasLimit {
		n, ok := intArg(limit)
		if !ok {
			return pg, invalid("limit must be an integer")
		}
		pg.limit = min(max(n, 1), MaxPageSize)
	}
	if c, ok := args["cursor"]; ok {
		s, _ := c.(string)
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return pg, invalid("cursor must be a nextCursor value from a previous page")
		}
		pg.offset = n
	} else if p, ok := args["page"]; ok && pg.native {
		n, ok := intArg(p)
		if !ok || n < 1 {
			return pg, invalid("page must be a positive integer")
		}
		pg.offset = (n - 1) * pg.limit
	}
	if s, ok := args["search"]; ok {
		pg.search, _ = s.(string)
	} else if q, ok := args["q"].(string); ok && pg.nativeSearch {
		pg.search = q
	}
	return pg, nil
}

// buildRequest fills the path template and query string from c.
func (d *Dispatcher) buildRequest(ctx context.Context, p Principal, t Tool, c Call, pg *paging) (*http.Request, *ToolError) {
	args := c.Args
	if args == nil {
		args = map[string]any{}
	}
	for k := range args {
		if !argAllowed(t, k) {
			return nil, invalid("unknown argument %q", k)
		}
	}
	op, params := operation(t.Method, t.Path)
	if op == nil {
		return nil, errNotDispatchable
	}
	var rawPairs, decodedPairs []string
	query := url.Values{}
	for _, prm := range params {
		name, _ := prm["name"].(string)
		switch prm["in"] {
		case "path":
			v, ok := args[name]
			if name == "cluster" && c.Cluster != "" {
				if ok && v != c.Cluster {
					return nil, invalid("cluster argument conflicts with the call's cluster")
				}
				v, ok = c.Cluster, true
			}
			if !ok {
				return nil, invalid("missing required argument %q", name)
			}
			s, err := scalar(name, v)
			if err != nil {
				return nil, err
			}
			if s == "" || s == "." || s == ".." || strings.ContainsAny(s, "/{}") {
				return nil, invalid("argument %q is not a valid name", name)
			}
			rawPairs = append(rawPairs, "{"+name+"}", url.PathEscape(s))
			decodedPairs = append(decodedPairs, "{"+name+"}", s)
		case "query":
			if pg != nil && (pg.native && (name == "page" || name == "pageSize") || pg.nativeSearch && name == "q") {
				continue // set from the generic paging arguments below
			}
			v, ok := args[name]
			if !ok {
				continue
			}
			list, isList := v.([]any)
			if !isList {
				list = []any{v}
			}
			for _, x := range list {
				s, err := scalar(name, x)
				if err != nil {
					return nil, err
				}
				query.Add(name, s)
			}
		}
	}
	// One pass each, so a value can never be re-substituted as a placeholder.
	raw := strings.NewReplacer(rawPairs...).Replace(t.Path)
	decoded := strings.NewReplacer(decodedPairs...).Replace(t.Path)
	if strings.ContainsAny(raw, "{}") {
		return nil, errNotDispatchable
	}
	if pg != nil {
		if pg.native {
			page := pg.offset/pg.limit + 1
			pg.skip = pg.offset - (page-1)*pg.limit
			query.Set("page", strconv.Itoa(page))
			query.Set("pageSize", strconv.Itoa(pg.limit))
		}
		if pg.nativeSearch && pg.search != "" {
			query.Set("q", pg.search)
		}
	}
	u := &url.URL{Path: APIPrefix + decoded, RawPath: APIPrefix + raw, RawQuery: query.Encode()}
	if refused(u.Path) || refused(u.EscapedPath()) {
		return nil, errNotDispatchable
	}
	var body []byte
	if b, ok := args["body"]; ok {
		var err error
		if body, err = json.Marshal(b); err != nil {
			return nil, invalid("body is not valid JSON")
		}
		if len(body) > MaxRequestBody {
			return nil, invalid("body exceeds %d bytes", MaxRequestBody)
		}
	}
	r, err := http.NewRequestWithContext(ctx, t.Method, "http://kaflux.internal/", bytes.NewReader(body))
	if err != nil {
		return nil, errNotDispatchable
	}
	r.URL, r.RequestURI, r.Host = u, u.RequestURI(), "kaflux.internal"
	r.Header.Set("Accept", "application/json")
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	r.RemoteAddr = remoteAddr(p.RemoteAddr)
	return api.WithMCPSession(r, mcpSession(p), p.Grants), nil
}

// remoteAddr keeps host:port form so audit can split the source IP.
func remoteAddr(addr string) string {
	if addr == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	return net.JoinHostPort(strings.Trim(addr, "[]"), "0")
}

// memWriter captures the handler's response in memory, bounded by maxCapture.
// It is safe to abandon: after close, writes from a timed-out handler fail.
type memWriter struct {
	mu       sync.Mutex
	header   http.Header
	status   int
	buf      bytes.Buffer
	overflow bool
	closed   bool
}

func (w *memWriter) Header() http.Header { return w.header }

func (w *memWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 && !w.closed {
		w.status = status
	}
}

func (w *memWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, http.ErrHandlerTimeout
	}
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if w.buf.Len()+len(b) > maxCapture {
		w.overflow = true
		return 0, errors.New("response too large")
	}
	return w.buf.Write(b)
}

// Do runs one tool call as p and returns the paged, capped result or a
// tool error. It never reaches /auth or /admin routes.
func (d *Dispatcher) Do(ctx context.Context, p Principal, t Tool, c Call) (Result, *ToolError) {
	res, terr, _ := d.do(ctx, p, t, c)
	return res, terr
}

// do is Do that also reports whether the REST handler was invoked, so a
// commit refused before reaching it can leave its confirmation unused.
func (d *Dispatcher) do(ctx context.Context, p Principal, t Tool, c Call) (Result, *ToolError, bool) {
	var pg *paging
	if t.Paginated {
		resolved, err := resolvePaging(t, c.Args)
		if err != nil {
			return Result{}, err, false
		}
		pg = &resolved
	}
	key := p.TokenID
	if key == "" {
		return Result{}, &ToolError{Code: "internal", Message: "Call has no token identity", Hint: "This is a Kaflux defect; report it.", Status: 500}, false
	}
	// Concurrency first, so a refused call does not spend a rate token.
	if !d.limiter.acquire(key, d.limits.Concurrency) {
		return Result{}, &ToolError{Code: "rate_limited", Message: "Too many concurrent calls for this token", Hint: fmt.Sprintf("At most %d concurrent calls per token; wait for running calls to finish, then retry.", d.limits.Concurrency), Status: 429}, false
	}
	if wait, ok := d.limiter.allow(key, d.limits.PerMinute, d.now()); !ok {
		d.limiter.release(key, d.limits.Concurrency)
		return Result{}, rateLimited(wait), false
	}
	callCtx, cancel := context.WithTimeout(ctx, d.limits.Timeout)
	defer cancel()
	r, terr := d.buildRequest(callCtx, p, t, c, pg)
	if terr != nil {
		d.limiter.release(key, d.limits.Concurrency)
		return Result{}, terr, false
	}
	w := &memWriter{header: http.Header{}}
	done := make(chan any, 1)
	// The slot is held until the handler returns, even past a timeout, so
	// abandoned calls still count against the token's concurrency cap.
	go func() {
		defer func() { done <- recover() }()
		defer d.limiter.release(key, d.limits.Concurrency)
		d.handler.ServeHTTP(w, r)
	}()
	select {
	case panicked := <-done:
		if panicked != nil {
			return Result{}, &ToolError{Code: "internal", Message: "Internal error", Hint: "Kaflux failed handling the call; retry later.", Status: 500}, true
		}
	case <-callCtx.Done():
		w.mu.Lock()
		w.closed = true
		w.mu.Unlock()
		if ctx.Err() != nil {
			return Result{}, &ToolError{Code: "cancelled", Message: "Call cancelled", Hint: "The client cancelled the call.", Status: 499}, true
		}
		if t.IsWrite() {
			return Result{}, &ToolError{Code: "timeout", Message: fmt.Sprintf("Call exceeded %s; the write may have been applied", d.limits.Timeout), Hint: writeUncertainHint, Status: 504}, true
		}
		return Result{}, &ToolError{Code: "timeout", Message: fmt.Sprintf("Call exceeded %s", d.limits.Timeout), Hint: "Kafka did not answer in time; retry, or narrow the query.", Status: 504}, true
	}
	status, body := w.status, w.buf.Bytes()
	if status == 0 {
		status = http.StatusOK
	}
	if status >= 400 {
		return Result{}, mapError(t, c, status, body), true
	}
	if w.overflow {
		return Result{}, &ToolError{Code: "response_too_large", Message: "Response too large", Hint: "Narrow your query with filters, a smaller limit or a more specific tool.", Status: 502}, true
	}
	return d.shape(t, pg, status, body), nil, true
}

func rateLimited(wait time.Duration) *ToolError {
	secs := max(1, int(math.Ceil(wait.Seconds())))
	return &ToolError{Code: "rate_limited", Message: "Rate limit exceeded for this token", Hint: fmt.Sprintf("Too many calls; retry after %ds.", secs), Status: 429}
}

// mayAttempt reports whether p holds any grant on cluster for an action of
// t's scope, the same coarse check the REST handlers apply before acting.
func mayAttempt(p Principal, t Tool, cluster string) bool {
	az, u := auth.Authorizer{Grants: p.Grants}, mcpSession(p).User
	for _, a := range ScopeActions[t.Scope] {
		if az.AnyAllowed(u, cluster, a) {
			return true
		}
	}
	return false
}

// shape pages and caps a successful response.
func (d *Dispatcher) shape(t Tool, pg *paging, status int, body []byte) Result {
	res := Result{Status: status}
	trimmed := bytes.TrimSpace(body)
	switch {
	case len(trimmed) == 0:
		trimmed = []byte("{}")
	case !json.Valid(trimmed):
		trimmed, _ = json.Marshal(map[string]string{"text": string(bytes.ToValidUTF8(trimmed, nil))})
	}
	view, isList := findList(trimmed)
	if !isList {
		if len(trimmed) > d.limits.MaxBytes {
			trimmed, res.Truncated = preview(trimmed, d.limits.MaxBytes), true
		}
		res.Body = trimmed
		return res
	}
	items, offset, total, more := view.items, 0, len(view.items), false
	if pg != nil {
		offset = pg.offset
		if pg.search != "" && !pg.nativeSearch {
			items = filterByName(items, pg.search)
		}
		if pg.native {
			total = metaTotal(view.top, offset+len(items))
			items = items[min(pg.skip, len(items)):]
		} else {
			total = len(items)
			items = items[min(offset, len(items)):]
			items = items[:min(pg.limit, len(items))]
		}
		more = offset+len(items) < total
	}
	extra := func(kept int) map[string]any {
		if pg == nil || view.top == nil {
			return nil
		}
		m := map[string]any{"truncated": more || kept < len(items)}
		if more || kept < len(items) {
			m["nextCursor"] = strconv.Itoa(offset + kept)
		}
		return m
	}
	kept := len(items)
	out := view.rebuild(items, extra(kept))
	if len(out) > d.limits.MaxBytes {
		kept = view.fitItems(items, d.limits.MaxBytes, extra)
		out = view.rebuild(items[:kept], extra(kept))
		if len(out) > d.limits.MaxBytes {
			out = preview(out, d.limits.MaxBytes)
		}
	}
	res.Body, res.Total, res.Returned = out, total, kept
	res.Truncated = more || kept < len(items)
	return res
}

func metaTotal(top map[string]json.RawMessage, fallback int) int {
	var meta struct {
		Total *int `json:"total"`
	}
	if top != nil && json.Unmarshal(top["meta"], &meta) == nil && meta.Total != nil {
		return *meta.Total
	}
	return fallback
}

// filterByName keeps items whose name (or id, groupId) contains search,
// case-insensitively.
func filterByName(items []json.RawMessage, search string) []json.RawMessage {
	needle := strings.ToLower(search)
	var out []json.RawMessage
	for _, it := range items {
		var row struct {
			Name    string `json:"name"`
			ID      string `json:"id"`
			GroupID string `json:"groupId"`
		}
		if json.Unmarshal(it, &row) != nil {
			continue
		}
		for _, s := range []string{row.Name, row.ID, row.GroupID} {
			if s != "" && strings.Contains(strings.ToLower(s), needle) {
				out = append(out, it)
				break
			}
		}
	}
	return out
}

// listToolFor names the list tool an agent should call after a not_found.
func listToolFor(t Tool) string {
	for _, name := range []string{"kaflux_" + t.Domain + "_list", "kaflux_" + t.Domain + "_subject_list"} {
		if name != t.Name {
			if _, ok := Lookup(name); ok {
				return name
			}
		}
	}
	return "kaflux_cluster_list"
}

// mapError turns a REST error response into a tool error. Messages come only
// from the REST error envelope, which the API already sanitizes.
func mapError(t Tool, c Call, status int, body []byte) *ToolError {
	var env struct {
		Error struct{ Code, Message string } `json:"error"`
	}
	_ = json.Unmarshal(body, &env)
	code, msg := env.Error.Code, env.Error.Message
	if code == "" || msg == "" || status >= 500 && status != 502 && status != 503 && status != 504 {
		code, msg = http.StatusText(status), http.StatusText(status)
		code = strings.ToLower(strings.ReplaceAll(code, " ", "_"))
	}
	if status >= 500 && status != 502 && status != 503 && status != 504 {
		code, msg = "internal", "Internal error"
	}
	if len(msg) > 512 {
		msg = strings.ToValidUTF8(msg[:512], "")
	}
	cluster := c.Cluster
	if cluster == "" {
		cluster, _ = c.Args["cluster"].(string)
	}
	e := &ToolError{Code: code, Message: msg, Status: status}
	switch {
	case status == 401:
		e.Hint = "The MCP token is not authenticated; check it has not expired or been revoked."
	case status == 403:
		e.Hint = fmt.Sprintf("Permission denied for %s (%s %s) on cluster %q: the token's scopes (needs %s) or its owner's grants do not allow it. Ask an administrator for the grant or a token with the %s scope.", t.Name, t.Method, t.Path, cluster, t.Scope, t.Scope)
	case status == 404:
		e.Hint = fmt.Sprintf("Not found; call %s to find valid names.", listToolFor(t))
	case status == 409:
		e.Hint = "The resource changed or already exists; read its current state and retry."
	case status == 429:
		e.Hint = "Rate limited; retry after a short delay."
	case (status == 502 || status == 503 || status == 504) && t.IsWrite():
		e.Hint = writeUncertainHint
	case status == 502 || status == 503 || status == 504:
		e.Hint = "Kafka or a dependency is temporarily unavailable; retry after a few seconds."
	case status >= 500:
		e.Hint = "Kaflux failed handling the call; retry later."
	default:
		e.Hint = "Check the arguments against the tool's input schema; body fields must match the REST API exactly."
	}
	return e
}

// MaxBulkTargets caps list-style targets of one MCP write; the REST API may
// allow more, but an agent must batch.
const MaxBulkTargets = 50

// bulkTargets names the body list holding the targets of a bulk tool.
var bulkTargets = map[string]string{"kaflux_topic_bulk_delete": "topics"}

// PlanSummary describes the REST call a commit will make.
type PlanSummary struct {
	Tool          string         `json:"tool"`
	Method        string         `json:"method"`
	Path          string         `json:"path"`
	Arguments     map[string]any `json:"arguments"`
	ScopeRequired string         `json:"scopeRequired"`
}

// PrepareOutput is the first phase of a write: what would happen, and the
// token that commits exactly this call. RestConfirmation says which values
// the REST body itself must carry; they are never filled in for the agent.
type PrepareOutput struct {
	Plan             PlanSummary `json:"plan"`
	Preview          any         `json:"preview,omitempty"`
	Confirmation     string      `json:"confirmation"`
	ExpiresAt        time.Time   `json:"expiresAt"`
	Destructive      bool        `json:"destructive"`
	RestConfirmation string      `json:"restConfirmation"`
}

// withoutConfirmation copies c without the MCP confirmation argument.
func withoutConfirmation(c Call) Call {
	args := make(map[string]any, len(c.Args))
	for k, v := range c.Args {
		if k != "confirmation" {
			args[k] = v
		}
	}
	return Call{Cluster: c.Cluster, Args: args}
}

func listLen(v any) int {
	switch l := v.(type) {
	case []any:
		return len(l)
	case []string:
		return len(l)
	}
	return 0
}

func checkBulk(t Tool, c Call) *ToolError {
	field, ok := bulkTargets[t.Name]
	if !ok {
		return nil
	}
	if n := listLen(obj(c.Args["body"])[field]); n > MaxBulkTargets {
		return &ToolError{Code: "too_many_targets", Message: fmt.Sprintf("body.%s lists %d targets; MCP allows at most %d per call", field, n, MaxBulkTargets),
			Hint: fmt.Sprintf("Split body.%s into batches of at most %d and prepare each batch separately.", field, MaxBulkTargets), Status: 400}
	}
	return nil
}

func needsNoConfirmation(t Tool) *ToolError {
	return &ToolError{Code: "invalid_argument", Message: fmt.Sprintf("%s is read-only and needs no confirmation", t.Name), Hint: "Call read tools directly.", Status: 400}
}

var errNoConfirmer = &ToolError{Code: "internal", Message: "Write confirmation is not configured", Hint: "This is a Kaflux defect; report it.", Status: 500}

// Prepare is the first phase of a write: it validates the call, runs the
// tool's read-only preview as p, mints a confirmation token bound to p, t
// and c, and audits the prepare. It never sends the write itself.
func (d *Dispatcher) Prepare(ctx context.Context, p Principal, t Tool, c Call) (PrepareOutput, *ToolError) {
	if !t.IsWrite() {
		return PrepareOutput{}, needsNoConfirmation(t)
	}
	if d.confirm == nil {
		return PrepareOutput{}, errNoConfirmer
	}
	if d.audit == nil {
		return PrepareOutput{}, &ToolError{Code: "internal", Message: "Prepare audit is not configured", Hint: "This is a Kaflux defect; report it.", Status: 500}
	}
	if p.TokenID == "" {
		return PrepareOutput{}, &ToolError{Code: "internal", Message: "Call has no token identity", Hint: "This is a Kaflux defect; report it.", Status: 500}
	}
	// A prepare spends a rate token like any call; its preview reads go
	// through Do and so also take the concurrency limit.
	if wait, ok := d.limiter.allow(p.TokenID, d.limits.PerMinute, d.now()); !ok {
		return PrepareOutput{}, rateLimited(wait)
	}
	c = withoutConfirmation(c)
	if e := checkBulk(t, c); e != nil {
		return PrepareOutput{}, e
	}
	// Built only to validate the arguments and name the target; never served.
	r, terr := d.buildRequest(ctx, p, t, c, nil)
	if terr != nil {
		return PrepareOutput{}, terr
	}
	cluster := c.Cluster
	if cluster == "" {
		cluster, _ = c.Args["cluster"].(string)
	}
	// Deny before any preview, token or audit row.
	if !mayAttempt(p, t, cluster) {
		return PrepareOutput{}, mapError(t, c, http.StatusForbidden, []byte(`{"error":{"code":"permission_denied","message":"Permission denied"}}`))
	}
	var pv any
	if t.Preview != nil {
		v, err := t.Preview(withPrincipal(ctx, p), d, c)
		if err != nil {
			var te *ToolError
			if errors.As(err, &te) {
				return PrepareOutput{}, te
			}
			return PrepareOutput{}, &ToolError{Code: "preview_failed", Message: "Could not compute the preview", Hint: "Read the target with the matching get tool, then prepare again.", Status: 502}
		}
		pv = v
	}
	token, expires := d.confirm.Mint(p, t, c)
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	row := store.Audit{Actor: mcpSession(p).User.ID, Provider: "mcp", ClusterID: cluster, Action: "mcp.prepare:" + t.Name, Resource: r.URL.Path, Result: "prepared", SourceIP: ip}
	if err := d.audit(ctx, row); err != nil {
		return PrepareOutput{}, &ToolError{Code: "audit_failed", Message: "Could not record the prepare audit; nothing was prepared", Hint: "Retry later; the audit store is unavailable.", Status: 503}
	}
	return PrepareOutput{
		Plan:             PlanSummary{Tool: t.Name, Method: t.Method, Path: r.URL.Path, Arguments: c.Args, ScopeRequired: t.Scope},
		Preview:          pv,
		Confirmation:     token,
		ExpiresAt:        expires,
		Destructive:      t.Destructive,
		RestConfirmation: restConfirmation(t, c),
	}, nil
}

func confirmationError(err error) *ToolError {
	again := "Call the tool again without confirmation to prepare this exact call, then commit with the returned confirmation unchanged."
	switch {
	case errors.Is(err, ErrConfirmationExpired):
		return &ToolError{Code: "confirmation_expired", Message: "Confirmation has expired", Hint: again, Status: 400}
	case errors.Is(err, ErrConfirmationMismatch):
		return &ToolError{Code: "confirmation_mismatch", Message: "Confirmation does not match this call", Hint: "It was prepared for a different token, tool, cluster or arguments. " + again, Status: 400}
	case errors.Is(err, ErrConfirmationBusy):
		return &ToolError{Code: "confirmation_busy", Message: "Too many confirmations in use", Hint: "Retry after a few minutes.", Status: 503}
	}
	return &ToolError{Code: "confirmation_invalid", Message: "Confirmation is not valid", Hint: again, Status: 400}
}

// uncertain reports whether a write that reached the handler may or may not
// have been applied.
func uncertain(e *ToolError) bool {
	if e == nil {
		return false
	}
	switch e.Code {
	case "timeout", "cancelled", "internal":
		return true
	}
	return e.Status == 502 || e.Status == 503 || e.Status == 504
}

// replayed returns the recorded outcome of an already-used confirmation.
func replayed(o *outcome) (Result, *ToolError) {
	if o.uncertain {
		return Result{}, &ToolError{Code: "outcome_uncertain", Message: "This confirmation was already used; that commit ended without a definite outcome: " + o.terr.Message, Hint: writeUncertainHint, Status: o.terr.Status}
	}
	if o.terr != nil {
		e := *o.terr
		return Result{}, &e
	}
	return o.res, nil
}

// Commit is the second phase of a write: it verifies confirmation against
// p, t and c and sends the write once. A repeated commit with the same
// confirmation returns the first commit's recorded outcome without writing
// again; concurrent commits wait for the first one.
func (d *Dispatcher) Commit(ctx context.Context, p Principal, t Tool, c Call, confirmation string) (Result, *ToolError) {
	if !t.IsWrite() {
		return Result{}, needsNoConfirmation(t)
	}
	if d.confirm == nil {
		return Result{}, errNoConfirmer
	}
	if confirmation == "" {
		return Result{}, &ToolError{Code: "confirmation_required", Message: "This write needs a confirmation", Hint: "Call the tool without confirmation to prepare it, review the plan, then call again with the returned confirmation.", Status: 400}
	}
	c = withoutConfirmation(c)
	if e := checkBulk(t, c); e != nil {
		return Result{}, e
	}
	cl, err := d.confirm.check(confirmation, p, t, c)
	if err != nil {
		return Result{}, confirmationError(err)
	}
	prior, err := d.confirm.claim(ctx, cl)
	if err != nil {
		if ctx.Err() != nil {
			return Result{}, &ToolError{Code: "cancelled", Message: "Call cancelled", Hint: "The client cancelled the call.", Status: 499}
		}
		return Result{}, confirmationError(err)
	}
	if prior != nil {
		return replayed(prior)
	}
	return d.commitClaimed(ctx, p, t, c, cl)
}

// commitClaimed sends the write for a claimed token and always settles the
// claim, even if dispatch panics, so a pending entry never wedges the token.
func (d *Dispatcher) commitClaimed(ctx context.Context, p Principal, t Tool, c Call, cl claims) (res Result, terr *ToolError) {
	settled := false
	defer func() {
		if settled {
			return
		}
		_ = recover() // a dispatch panic becomes an uncertain internal error
		res, terr = Result{}, &ToolError{Code: "internal", Message: "Internal error", Hint: writeUncertainHint, Status: 500}
		d.confirm.finish(cl, outcome{terr: terr, uncertain: true})
	}()
	res, terr, ran := commitDo(d, ctx, p, t, c)
	if !ran {
		// Refused before the REST handler: nothing was written, so the
		// confirmation stays usable.
		d.confirm.release(cl)
	} else {
		d.confirm.finish(cl, outcome{res: res, terr: terr, uncertain: uncertain(terr)})
	}
	settled = true
	return res, terr
}

// commitDo is d.do; tests replace it to exercise the settle guard.
var commitDo = (*Dispatcher).do
