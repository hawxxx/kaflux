package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel/trace"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/buildinfo"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

// This file is the only adapter to the MCP SDK's transport and server types:
// SDK upgrades stay here (and in Tool.SDKTool).

// Options configures the /mcp handler.
type Options struct {
	Store *store.Store
	// Handler is the REST API every tool call is dispatched through.
	Handler http.Handler
	// Grants returns the configured grants; it is called on every tool call,
	// so configuration changes apply to the very next call.
	Grants func() []auth.Grant
	// Writes exposes write and destructive tools (mcp.writes: enabled).
	Writes bool
	// AllowedOrigins lists the browser origins (scheme://host[:port]) that
	// may call /mcp. A request with any other Origin, or with an Origin when
	// the list is empty, is refused; requests without Origin are not checked.
	AllowedOrigins []string
	Limits         Limits
	// ConfirmKey keys write confirmations; nil draws a per-process key.
	ConfirmKey []byte
	// Demo labels the server as serving simulated demo data.
	Demo bool
}

// Handler serves POST /mcp: streamable HTTP, stateless, JSON responses.
type Handler struct {
	o        Options
	d        *Dispatcher
	sdk      http.Handler
	metrics  *metrics
	authFail *failureLimiter
	// lookup resolves a token hash; tests wrap it to count store lookups.
	lookup func(context.Context, string) (store.MCPToken, error)
}

// NewHandler returns the /mcp handler. It authenticates each request with a
// bearer token and never reads cookies.
func NewHandler(o Options) *Handler {
	d := NewDispatcher(o.Handler, o.Limits)
	if len(o.ConfirmKey) > 0 {
		d.SetConfirmer(NewConfirmer(o.ConfirmKey, DefaultConfirmTTL, nil))
	}
	h := &Handler{o: o, d: d, metrics: defaultMetrics, authFail: newFailureLimiter(MaxAuthFailuresPerMinute)}
	if o.Store != nil {
		d.SetAudit(o.Store.Audit)
		h.lookup = o.Store.MCPTokenByHash
	}
	h.sdk = sdk.NewStreamableHTTPHandler(h.server, &sdk.StreamableHTTPOptions{
		Stateless:    true,
		JSONResponse: true,
		// Every request carries a bearer token a rebinding page cannot
		// supply, and Origin is checked above; the SDK's localhost Host check
		// would only break same-host reverse proxies.
		DisableLocalhostProtection: true,
		MaxRequestBodyBytes:        MaxRequestBody + 64<<10,
	})
	return h
}

// caller is the authenticated token of one HTTP request.
type caller struct {
	token    store.MCPToken
	toolsets []string
	remote   string
	reqID    string
	once     sync.Once
	server   *sdk.Server
}

type callerKey struct{}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		httpError(w, http.StatusMethodNotAllowed, "method_not_allowed", "POST required")
		return
	}
	if !h.originAllowed(r) {
		h.metrics.deny(deniedOrigin)
		httpError(w, http.StatusForbidden, "origin_forbidden", "Origin not allowed")
		return
	}
	if h.o.Store == nil || h.o.Handler == nil {
		httpError(w, http.StatusServiceUnavailable, "mcp_unavailable", "MCP is not configured")
		return
	}
	// The failed-auth limiter keys on the direct peer (RemoteAddr). Behind a
	// reverse proxy that is the proxy's address, so a valid token is never
	// refused by it: only failures are counted and answered with 429. If the
	// API gains a trusted-proxy client-IP helper, use it here.
	ip := clientIP(r.RemoteAddr)
	secret, ok := bearer(r)
	if !ok {
		h.unauthenticated(w, ip)
		return
	}
	tok, err := h.lookup(r.Context(), store.HashMCPSecret(secret))
	if errors.Is(err, store.ErrMCPTokenNotFound) {
		h.unauthenticated(w, ip)
		return
	}
	if err != nil {
		slog.Error("mcp token lookup failed", "request_id", requestID(r), "error", err)
		httpError(w, http.StatusServiceUnavailable, "token_store_unavailable", "Token store unavailable; retry")
		return
	}
	if err := h.o.Store.TouchMCPToken(r.Context(), tok.ID, time.Now()); err != nil {
		slog.Warn("mcp token touch failed", "request_id", requestID(r), "token_id", tok.ID, "error", err)
	}
	c := &caller{token: tok, toolsets: toolsets(r.URL.Query().Get("toolsets")), remote: r.RemoteAddr, reqID: requestID(r)}
	h.sdk.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), callerKey{}, c)))
}

// unauthenticated records a failed authentication and answers 401, or 429
// once the client has failed too often in the last minute.
func (h *Handler) unauthenticated(w http.ResponseWriter, ip string) {
	if over := h.authFail.record(ip, time.Now()); over {
		h.metrics.deny(deniedAuthRateLimited)
		w.Header().Set("Retry-After", "60")
		httpError(w, http.StatusTooManyRequests, "rate_limited", "Too many failed authentication attempts; retry later")
		return
	}
	h.metrics.deny(deniedUnauthenticated)
	w.Header().Set("WWW-Authenticate", "Bearer")
	httpError(w, http.StatusUnauthorized, "unauthenticated", "Valid MCP token required")
}

func httpError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

// bearer extracts a kfx_ token from the Authorization header only.
func bearer(r *http.Request) (string, bool) {
	scheme, secret, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	secret = strings.TrimSpace(secret)
	if !ok || !strings.EqualFold(scheme, "Bearer") || !strings.HasPrefix(secret, "kfx_") {
		return "", false
	}
	return secret, true
}

// originAllowed accepts requests without Origin (non-browser clients) and
// browser requests whose Origin exactly matches a configured origin by
// scheme, host and port. The request's Host is never trusted: under DNS
// rebinding it matches the attacker's Origin.
func (h *Handler) originAllowed(r *http.Request) bool {
	values := r.Header.Values("Origin")
	if len(values) == 0 {
		return true
	}
	// A present but repeated or blank Origin is malformed, never "absent".
	if len(values) > 1 || strings.TrimSpace(values[0]) == "" {
		return false
	}
	got, ok := normalizeOrigin(values[0])
	if !ok {
		return false
	}
	for _, o := range h.o.AllowedOrigins {
		if want, ok := normalizeOrigin(o); ok && want == got {
			return true
		}
	}
	return false
}

// normalizeOrigin reduces an http(s) origin to scheme://host:port with the
// scheme and host lowercased and the default port made explicit. Anything
// else, including "null" and origins with a path, query or credentials, is
// not an origin.
func normalizeOrigin(o string) (string, bool) {
	u, err := url.Parse(strings.TrimSpace(o))
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", false
	}
	scheme := strings.ToLower(u.Scheme)
	port := u.Port()
	switch {
	case scheme != "http" && scheme != "https":
		return "", false
	case port == "" && scheme == "http":
		port = "80"
	case port == "":
		port = "443"
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return "", false
	}
	return scheme + "://" + net.JoinHostPort(host, port), true
}

// MaxAuthFailuresPerMinute bounds failed /mcp authentications per client IP.
const MaxAuthFailuresPerMinute = 120

// maxFailureClients bounds the IPs the failure limiter tracks.
const maxFailureClients = 10000

// failureLimiter counts failed authentications per client IP over a sliding
// minute, in the manner of the API's login limiter.
type failureLimiter struct {
	mu        sync.Mutex
	max       int
	hits      map[string][]time.Time
	lastPrune time.Time
}

func newFailureLimiter(max int) *failureLimiter {
	return &failureLimiter{max: max, hits: map[string][]time.Time{}}
}

func (l *failureLimiter) recent(ip string, now time.Time) []time.Time {
	kept := l.hits[ip][:0]
	for _, at := range l.hits[ip] {
		if now.Sub(at) < time.Minute {
			kept = append(kept, at)
		}
	}
	if len(kept) == 0 {
		delete(l.hits, ip)
		return nil
	}
	l.hits[ip] = kept
	return kept
}

// record counts one failure and reports whether the client has now failed
// more than max times within the minute.
func (l *failureLimiter) record(ip string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.lastPrune) >= time.Minute {
		for k := range l.hits {
			l.recent(k, now)
		}
		l.lastPrune = now
	}
	hits := l.recent(ip, now)
	if hits == nil && len(l.hits) >= maxFailureClients {
		return false // full of active offenders; do not evict them for a newcomer
	}
	if len(hits) >= l.max {
		return true // already over: keep the window bounded at max entries
	}
	l.hits[ip] = append(hits, now)
	return false
}

func clientIP(remote string) string {
	if ip, _, err := net.SplitHostPort(remote); err == nil {
		return ip
	}
	return remote
}

func toolsets(q string) []string {
	var out []string
	for _, s := range strings.Split(q, ",") {
		if s = strings.ToLower(strings.TrimSpace(s)); s != "" {
			out = append(out, s)
		}
	}
	return out
}

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// requestID matches the API's audit correlation: the trace id, else a plain
// client X-Request-ID.
func requestID(r *http.Request) string {
	if span := trace.SpanContextFromContext(r.Context()); span.IsValid() {
		return span.TraceID().String()
	}
	if id := r.Header.Get("X-Request-ID"); requestIDPattern.MatchString(id) {
		return id
	}
	return ""
}

// visibility says why a tool is hidden from a token, or "" when it is shown.
func (h *Handler) visibility(t Tool, c *caller) string {
	if len(c.toolsets) > 0 && !slices.Contains(c.toolsets, t.Domain) {
		return deniedToolset
	}
	if t.IsWrite() && !h.o.Writes {
		return deniedWritesDisabled
	}
	if !slices.Contains(c.token.Scopes, t.Scope) {
		return deniedScope
	}
	return ""
}

// server builds the per-request MCP server holding only the tools the
// request's token may see. The SDK asks for it more than once per request.
func (h *Handler) server(r *http.Request) *sdk.Server {
	c, ok := r.Context().Value(callerKey{}).(*caller)
	if !ok {
		return nil
	}
	c.once.Do(func() {
		instructions := "Kaflux Kafka operations console. Write tools are two-phase: call without confirmation to prepare, review the plan, then repeat the identical call with the returned confirmation to commit."
		if h.o.Demo {
			instructions = "DEMO MODE: all data is simulated. " + instructions
		}
		s := sdk.NewServer(&sdk.Implementation{Name: "kaflux", Title: "Kaflux", Version: buildinfo.Current()}, &sdk.ServerOptions{Instructions: instructions, HasTools: true})
		for _, t := range Catalog() {
			if h.visibility(t, c) == "" {
				s.AddTool(sdkTools()[t.Name], h.toolHandler(t, c))
			}
		}
		s.AddReceivingMiddleware(h.hiddenGuard(c))
		c.server = s
	})
	return c.server
}

// hiddenGuard answers tools/call for a catalog tool the token may not see
// with a denial, rather than the SDK's unknown-tool error.
func (h *Handler) hiddenGuard(c *caller) sdk.Middleware {
	return func(next sdk.MethodHandler) sdk.MethodHandler {
		return func(ctx context.Context, method string, req sdk.Request) (sdk.Result, error) {
			if method != "tools/call" {
				return next(ctx, method, req)
			}
			ctr, ok := req.(*sdk.CallToolRequest)
			if !ok || ctr.Params == nil {
				return next(ctx, method, req)
			}
			t, known := Lookup(ctr.Params.Name)
			if !known {
				return next(ctx, method, req)
			}
			if why := h.visibility(t, c); why != "" {
				h.metrics.deny(why)
				h.metrics.call(t.Name, resultDenied, 0)
				h.log(c, t, resultDenied, 0)
				return errorResult(hiddenError(t, why)), nil
			}
			return next(ctx, method, req)
		}
	}
}

func hiddenError(t Tool, why string) *ToolError {
	e := &ToolError{Code: "forbidden", Status: http.StatusForbidden}
	switch why {
	case deniedWritesDisabled:
		e.Message = t.Name + " is unavailable: MCP writes are disabled on this server"
		e.Hint = "An administrator must set mcp.writes: enabled; use read tools meanwhile."
	case deniedToolset:
		e.Message = t.Name + " is outside the toolsets selected for this connection"
		e.Hint = "Reconnect without ?toolsets= or include the " + t.Domain + " toolset."
	default:
		e.Message = t.Name + " needs the " + t.Scope + " scope, which this token lacks"
		e.Hint = "Ask the token owner for a token with the " + t.Scope + " scope."
	}
	return e
}

// principal resolves the caller's identity and grants now, so revoked owner
// grants deny the very next call.
func (h *Handler) principal(c *caller) Principal {
	tok := c.token
	var configured []auth.Grant
	if h.o.Grants != nil {
		configured = h.o.Grants()
	}
	return Principal{
		Session: auth.Session{
			ID:      "mcp:" + tok.ID,
			User:    auth.User{ID: tok.OwnerID, Username: tok.OwnerID, Roles: slices.Clone(tok.OwnerRoles), Provider: tok.OwnerProvider},
			Expires: tok.Expires,
		},
		Grants:     Narrow(configured, tok.Scopes, tok.Clusters),
		TokenID:    tok.ID,
		TokenName:  tok.Name,
		RemoteAddr: c.remote,
	}
}

func (h *Handler) toolHandler(t Tool, c *caller) sdk.ToolHandler {
	return func(ctx context.Context, req *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
		start := time.Now()
		// The same predicate as tools/list, re-checked on every call.
		if why := h.visibility(t, c); why != "" {
			h.metrics.deny(why)
			h.metrics.call(t.Name, resultDenied, time.Since(start))
			h.log(c, t, resultDenied, time.Since(start))
			return errorResult(hiddenError(t, why)), nil
		}
		res, result := h.call(ctx, t, c, req.Params.Arguments)
		d := time.Since(start)
		h.metrics.call(t.Name, result, d)
		h.log(c, t, result, d)
		return res, nil
	}
}

func (h *Handler) log(c *caller, t Tool, result string, d time.Duration) {
	slog.Info("mcp call", "request_id", c.reqID, "token_id", c.token.ID, "tool", t.Name, "result", result, "duration_ms", d.Milliseconds())
}

// call runs one tool and renders its result; the second value is the
// metrics result label.
func (h *Handler) call(ctx context.Context, t Tool, c *caller, raw json.RawMessage) (*sdk.CallToolResult, string) {
	args, terr := decodeArgs(raw)
	if terr != nil {
		return errorResult(terr), resultError
	}
	call := Call{Args: args}
	call.Cluster, _ = args["cluster"].(string)
	p := h.principal(c)
	if !t.IsWrite() {
		res, terr := h.d.Do(ctx, p, t, call)
		if terr != nil {
			return errorResult(terr), h.errorLabel(terr)
		}
		return h.render(t, args, res), resultOK
	}
	conf, has := args["confirmation"]
	if !has {
		out, terr := h.d.Prepare(ctx, p, t, call)
		if terr != nil {
			return errorResult(terr), h.errorLabel(terr)
		}
		return prepared(t, out), resultPrepared
	}
	token, ok := conf.(string)
	if !ok {
		return errorResult(invalid("confirmation must be a string")), resultError
	}
	res, terr := h.d.Commit(ctx, p, t, call, token)
	if terr != nil {
		return errorResult(terr), h.errorLabel(terr)
	}
	return h.render(t, args, res), resultOK
}

// errorLabel counts authorization and rate-limit failures as denials.
func (h *Handler) errorLabel(e *ToolError) string {
	switch e.Status {
	case http.StatusForbidden, http.StatusUnauthorized:
		h.metrics.deny(deniedPermission)
		return resultDenied
	case http.StatusTooManyRequests:
		h.metrics.deny(deniedRateLimited)
		return resultDenied
	}
	return resultError
}

func decodeArgs(raw json.RawMessage) (map[string]any, *ToolError) {
	args := map[string]any{}
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return args, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // keep 64-bit offsets exact
	if err := dec.Decode(&args); err != nil || args == nil {
		return nil, invalid("arguments must be a JSON object")
	}
	return args, nil
}

// errorResult renders a tool error as isError text the agent can act on.
func errorResult(e *ToolError) *sdk.CallToolResult {
	b, _ := json.Marshal(map[string]any{"error": map[string]any{"code": e.Code, "message": e.Message, "hint": e.Hint, "status": e.Status}})
	return &sdk.CallToolResult{IsError: true, Content: []sdk.Content{&sdk.TextContent{Text: string(b)}}}
}

func prepared(t Tool, out PrepareOutput) *sdk.CallToolResult {
	b, _ := json.Marshal(out)
	var structured any
	_ = json.Unmarshal(b, &structured)
	head := fmt.Sprintf("Prepared %s; nothing has changed. Review the plan, then repeat the identical call with \"confirmation\": %q before %s.", t.Name, out.Confirmation, out.ExpiresAt.UTC().Format(time.RFC3339))
	if out.Destructive {
		head += " This change is destructive."
	}
	return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: head + "\n" + string(b)}}, StructuredContent: structured}
}

// render returns a successful result as structured content plus compact text.
// A capped non-list response is text only: its preview object does not match
// the tool's output schema.
func (h *Handler) render(t Tool, args map[string]any, res Result) *sdk.CallToolResult {
	view, isList := findList(res.Body)
	if !isList {
		if res.Truncated {
			return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: string(res.Body)}}}
		}
		var v any
		_ = json.Unmarshal(res.Body, &v)
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: string(res.Body)}}, StructuredContent: v}
	}
	var out map[string]any
	if view.top == nil {
		out = map[string]any{"data": json.RawMessage(res.Body)}
		if res.Truncated && t.Paginated {
			offset := 0
			if s, ok := args["cursor"].(string); ok {
				offset, _ = strconv.Atoi(s)
			}
			out["nextCursor"] = strconv.Itoa(offset + res.Returned)
		}
	} else if err := json.Unmarshal(res.Body, &out); err != nil {
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: string(res.Body)}}}
	}
	out["total"], out["returned"], out["truncated"] = res.Total, res.Returned, res.Truncated
	b, _ := json.Marshal(out)
	var structured any
	_ = json.Unmarshal(b, &structured)
	head := fmt.Sprintf("Returned %d of %d items.", res.Returned, res.Total)
	if res.Truncated {
		head = fmt.Sprintf("Returned %d of %d items (truncated", res.Returned, res.Total)
		if c, ok := out["nextCursor"].(string); ok {
			head += fmt.Sprintf("; pass cursor %q for more", c)
		}
		head += ")."
	}
	return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: head + "\n" + string(b)}}, StructuredContent: structured}
}

// sdkTools caches each tool's SDK definition; output schemas describe the
// list wrapper render adds. Write tools declare none, because prepare and
// commit return different shapes.
var sdkTools = sync.OnceValue(func() map[string]*sdk.Tool {
	m := map[string]*sdk.Tool{}
	for _, t := range Catalog() {
		st := t.SDKTool()
		st.OutputSchema = nil
		if !t.IsWrite() {
			if s := listOutputSchema(t.OutputSchema()); s != nil {
				st.OutputSchema = s
			}
		}
		m[t.Name] = st
	}
	return m
})

func listOutputSchema(s map[string]any) map[string]any {
	if s == nil {
		return nil
	}
	counts := map[string]any{
		"total":     map[string]any{"type": "integer", "description": "Items matching the call before paging"},
		"returned":  map[string]any{"type": "integer", "description": "Items in this response"},
		"truncated": map[string]any{"type": "boolean", "description": "True when more items exist than were returned"},
	}
	switch s["type"] {
	case "array":
		props := map[string]any{"data": s, "nextCursor": map[string]any{"type": "string", "description": "Pass as cursor to fetch the next page"}}
		maps.Copy(props, counts)
		return map[string]any{"type": "object", "properties": props, "required": []string{"data"}}
	case "object":
		out := maps.Clone(s)
		props := maps.Clone(obj(s["properties"]))
		if props == nil {
			props = map[string]any{}
		}
		maps.Copy(props, counts)
		out["properties"] = props
		return out
	}
	return s
}
