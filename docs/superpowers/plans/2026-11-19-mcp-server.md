# MCP Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship an embedded, production-ready MCP server (`/mcp`) exposing the full cluster-scoped Kaflux API as read and write tools, with scoped tokens and two-phase confirmed writes.

**Architecture:** An MCP handler authenticates a scoped API token, then dispatches each tool call in-process through the existing `API.ServeHTTP` with the token owner's session carried in an unexported request-context key. RBAC, audit and validation are therefore the REST behavior. A registry maps every OpenAPI operation to a tool and a parity test keeps them in step. Write tools run prepare-then-commit with HMAC-bound single-use confirmations.

**Tech Stack:** Go 1.26, `github.com/modelcontextprotocol/go-sdk` (pin the version; verify exact API with `npx ctx7@latest docs /websites/pkg_go_dev_github_com_modelcontextprotocol_go-sdk "<question>"` before use, do not trust memory), pgx, SQLite store, React + Vitest, Playwright / agent-browser.

**Spec:** `docs/superpowers/specs/2026-11-19-mcp-server-design.md`

## Global Constraints

- Go backend under `backend/`; four-space indentation Go, two-space TS/YAML/JSON; kebab-case config filenames.
- MCP is off by default: `mcp.enabled: false`, `mcp.writes: disabled`.
- Token secret prefix `kfx_`, 256-bit random, stored only as SHA-256; shown once. Expiry required, default 30 days, max 90.
- Scopes exactly: `read`, `messages`, `write`, `destructive`. A token only narrows its owner's grants (owner grants ∩ scope actions ∩ cluster allowlist).
- Defaults: page size 50, max 200; response cap 256 KiB (`mcp.maxResponseBytes`); confirmation TTL 5 minutes; bulk cap 50 items; REST body limit 1 MiB.
- Tool names `kaflux_<domain>_<verb>`; one tool per REST operation unless listed in the exclusion table.
- Audit: provider `mcp`, actor `<owner id>/<token name>`; persisted intent before Kafka mutation (existing REST behavior). Never log token secrets, `Authorization` headers, or message payloads.
- Numeric inputs in UI use `frontend/src/NumberStepper.tsx`, never native number spinners. Every capability has a UI surface or is documented as API-only by design.
- REST routes never accept bearer tokens; only `/mcp` does.
- Conventional Commits, no AI attribution lines. Do not commit, push or open the PR until the user authorizes it for this work. Do not touch `feat/cluster-version`.
- Do not report builds or tests as passing unless run; verify rendered UI in a real browser.

## Review Focus

- Replayed, expired, tampered or other-principal confirmation token on commit → rejected, nothing mutated, original result returned only for the same principal's already-committed call.
- Token whose owner loses a grant or whose cluster allowlist excludes the cluster → denied on the very next call, not after cache expiry.
- Tool list for a `read`-only token contains no write tool; direct `tools/call` of a hidden write tool is also denied (not just hidden).
- Huge topic/group/message listing → truncated with `truncated: true` and a narrowing hint, never a multi-MB response or a hang.
- Forged context session: an external HTTP request carrying a bearer token or `Cookie` on `/api/v1/*` cannot gain MCP scopes, and `/mcp` ignores cookies.

## File Structure

- `backend/internal/store/mcp_tokens.go` — token entity, hash, CRUD across memory/Postgres/SQLite (+ migration in `sqlite.go`, Postgres DDL in `store.go`).
- `backend/internal/mcp/` (new package, one responsibility per file):
  - `scopes.go` — scope → allowed REST actions; `Narrow(grants, scopes, clusters) []auth.Grant`.
  - `registry.go` — `Tool` struct and `Registry`; `catalog_read.go`, `catalog_write.go` — the tool tables.
  - `schema.go` — input/output schema derivation from embedded OpenAPI.
  - `dispatch.go` — build internal request, run handler, map response/errors.
  - `confirm.go` — confirmation token mint/verify, replay cache.
  - `server.go` — SDK server factory per request, `tools/list` filtering, handler for `/mcp`.
  - `limits.go` — rate limit, concurrency cap, timeout, response truncation.
  - `resources.go`, `prompts.go`, `metrics.go`.
- `backend/internal/api/mcpcontext.go` — unexported context key, `WithMCPSession`, `a.grants(session)` replacing direct `a.o.Grants` use.
- `backend/internal/api/mcptokens.go` — token management REST handlers.
- `backend/internal/config/config.go` — `MCP` config block. `backend/cmd/kaflux/main.go` — wiring.
- `frontend/src/McpAccess.tsx` (+ test, + css) — token UI; hooked into the Administration area.
- `docs/mcp.md`, `docs/mcp/evaluation.xml`, edits to `docs/security.md`, `docs/authentication.md`, `docs/openapi.yaml`, `config.example.yaml`, `deploy/helm/kaflux`, `README.md`.

## Tool Catalog (decides names, scope and exclusions)

Scope column: R=`read`, M=`messages`, W=`write`, D=`destructive`. Paths are under `/api/v1/clusters/{cluster}` unless noted. REST enforces the real RBAC action; the scope is the MCP-side ceiling.

**Reads (R):** `cluster_list` (`/clusters`), `cluster_overview`, `cluster_capabilities`, `cluster_configuration`, `cluster_id`, `cluster_features`, `cluster_quorum`, `cluster_api_versions`, `topic_list`, `topic_get`, `topic_config_get`, `topic_balance`, `topic_offsets`, `topic_replica_verification`, `topic_producers`, `topic_hanging_transactions`, `topic_unused`, `broker_list`, `broker_config_get`, `log_dirs`, `group_list`, `group_get`, `acl_list`, `quota_list`, `client_metrics_list`, `client_metrics_get`, `user_list`, `delegation_token_list`, `transaction_list`, `transaction_get`, `schema_integration_list`, `schema_subject_list`, `schema_get`, `schema_versions`, `schema_version_get`, `schema_config_get`, `connector_integration_list`, `connector_list`, `connector_get`, `connector_config_get`, `connector_status`, `audit_cluster`, `audit_global` (`/audit`), `balance_get` (`/balance`), `rebalance_list`, `rebalance_get`, `rebalance_events`, `metrics_catalog_global` (`/metrics/catalog`), `metrics_catalog`, `metrics_query`, `schema_compatibility_check` (POST but read-only).
**Reads (M):** `message_consume` (GET `/messages`).
**Writes (W):** `topic_create`, `topic_config_update`, `topic_partitions_add`, `topic_copy`, `topic_leader_election`, `topic_abort_transaction`, `acl_create`, `broker_config_update`, `quota_update`, `client_metrics_update`, `scram_upsert`, `delegation_token_create`, `delegation_token_renew`, `feature_update`, `schema_register`, `schema_config_update`, `connector_upsert`, `connector_config_update`, `connector_pause`, `connector_resume`, `connector_restart`, `connector_task_restart`, `probe_produce`, `probe_consume`, `probe_latency`, `cluster_rename`, `message_produce` (POST `/messages`), `rebalance_plan`, `rebalance_dry_run`, `rebalance_pause`, `rebalance_resume`, `rebalance_cancel`, `rebalance_cancel_rollback`, `rebalance_throttle`.
**Writes (D):** `topic_delete`, `topic_truncate`, `topic_recreate`, `topic_bulk_delete`, `topic_delete_records`, `group_delete`, `group_reset_offsets`, `group_delete_offsets`, `streams_reset`, `acl_delete`, `broker_unregister`, `client_metrics_delete`, `scram_delete`, `delegation_token_delete`, `schema_subject_delete`, `schema_version_delete`, `connector_delete`, `rebalance_execute`, `rebalance_rollback`.

**Excluded (with reason, asserted by the parity test):** `/admin/sessions*`, `/auth/*` (identity), `/clusters/{cluster}/test` (connection test, admin UI only), `/admin/mcp-tokens*` (a token must never mint tokens), file downloads `events.txt`, `backup.json`, `report.csv` (data available through `rebalance_events`/`rebalance_get`).

## Tasks

### Task 0: Branch, tracking, dependency

**Files:** Modify `backend/go.mod`, `backend/go.sum`.

- [ ] **Step 1:** `git fetch origin && git switch -c feat/mcp-server origin/main` (leave the dirty `feat/cluster-version` tree alone; if the switch is blocked by local changes, use `git worktree add ../kaflux-mcp -b feat/mcp-server origin/main`).
- [ ] **Step 2:** `bd create` one epic "MCP server" and a child per task below (`--parent`), then `bd update <id> --claim` for Task 1.
- [ ] **Step 3:** `cd backend && go get github.com/modelcontextprotocol/go-sdk@<latest tagged v1>`; confirm `go doc github.com/modelcontextprotocol/go-sdk/mcp Server` and `NewStreamableHTTPHandler`, in-memory transport, and low-level `Server.AddTool` signatures; record them in a comment at the top of `internal/mcp/server.go`.
- [ ] **Step 4:** `go build ./...` → exit 0.

### Task 1: Token store

**Files:** Create `backend/internal/store/mcp_tokens.go`, `mcp_tokens_test.go`. Modify `store.go` (Postgres DDL string, in-memory map on `Store`), `sqlite.go` (new table in the schema block, same `kaflux_schema` version handling: add table with `IF NOT EXISTS`, no version bump needed).

**Interfaces — Produces:**
```go
type MCPToken struct {
    ID, Name, OwnerID, OwnerProvider string
    Scopes   []string   // subset of read|messages|write|destructive
    Clusters []string   // empty = all owner-permitted clusters
    Expires, Created time.Time
    LastUsed, Revoked *time.Time
}
func NewMCPSecret() (secret, hash string) // "kfx_"+base64url(32 bytes); hash=hex(sha256(secret))
func (s *Store) CreateMCPToken(ctx context.Context, t MCPToken, hash string) error
func (s *Store) MCPTokenByHash(ctx context.Context, hash string) (MCPToken, error) // ErrMCPTokenNotFound when absent, revoked or expired
func (s *Store) ListMCPTokens(ctx context.Context, ownerID string, all bool) ([]MCPToken, error)
func (s *Store) RevokeMCPToken(ctx context.Context, id string) error
func (s *Store) TouchMCPToken(ctx context.Context, id string, at time.Time) error // throttled: no write if LastUsed < 1 min old
var ErrMCPTokenNotFound error
```

- [ ] **Step 1:** Write `TestMCPTokenLifecycle` (create → lookup by hash ok → revoke → lookup `ErrMCPTokenNotFound`), `TestMCPTokenExpired` (expired token not found), `TestMCPSecretShape` (prefix `kfx_`, hash is 64 hex, two secrets differ), `TestListMCPTokensOwnerOnly`. Run each against memory and SQLite (`t.TempDir()` DB) via a table of constructors; Postgres variant behind the existing integration-test tag in `integration_test.go`.
- [ ] **Step 2:** `go test ./internal/store -run MCP -v` → FAIL (undefined).
- [ ] **Step 3:** Implement per backend; hash column unique index; never persist the secret. Follow `admin_sessions.go` for the three-backend dispatch pattern.
- [ ] **Step 4:** Run tests → PASS. Run `go test ./internal/store` → PASS.
- [ ] **Step 5:** Commit `feat(store): add MCP token storage`.

### Task 2: Scopes and grant narrowing

**Files:** Create `backend/internal/mcp/scopes.go`, `scopes_test.go`. Create `backend/internal/api/mcpcontext.go`. Modify `backend/internal/api/*.go` (every direct `a.o.Grants` / `auth.Authorizer{Grants: a.o.Grants}` use) and `api.go:122` `session()`.

**Interfaces — Produces:**
```go
// package mcp
var ScopeActions map[string][]string // read→{read,schema-read,connector-read,audit}, messages→{consume}, write→{produce,create,alter-config,rename,plan,manage-acls,manage-quotas,manage-users,manage-tokens,manage-cluster,schema-update,connector-update}, destructive→{delete,reset-offsets,execute,rollback}
func Narrow(owner []auth.Grant, scopes, clusters []string) []auth.Grant // drops grants whose action is outside scopes (keeping "*" grants expanded to allowed actions) and whose cluster is outside allowlist
// package api
func WithMCPSession(r *http.Request, s auth.Session, grants []auth.Grant) *http.Request
func (a *API) grants(r *http.Request) []auth.Grant // context-narrowed grants if present else a.o.Grants
```
`manage-sessions` and any future admin action are in no scope.

- [ ] **Step 1:** Tests in `scopes_test.go`: `TestNarrowDropsOutOfScope` (owner `*` on cluster `a`, scope `read` → only read-class actions), `TestNarrowClusterAllowlist`, `TestNarrowNeverAddsGrant` (property: result ⊆ owner, run over randomized grant sets), `TestAdminActionsInNoScope`. In `api` package: `TestNoDirectGrantAccess` reads every non-test `.go` file in `internal/api` and fails if `a.o.Grants` appears outside `mcpcontext.go`; `TestSessionIgnoresForgedContext` (a request with the context key set by a foreign package value type is not honored; a request with only `Authorization: Bearer x` on `/api/v1/clusters` returns 401).
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement. Context key is an unexported struct type in `mcpcontext.go`. `session()` returns the context session first when present. Replace all `a.o.Grants` uses with `a.grants(r)`; helper signatures that lack `r` get it threaded (mechanical). In `ServeHTTP`, skip the CSRF check only when the session came from `WithMCPSession`.
- [ ] **Step 4:** `go test ./internal/api ./internal/mcp` → all existing API tests plus new → PASS.
- [ ] **Step 5:** Commit `refactor(api): route grants through request context`.

### Task 3: Registry, schemas, parity test

**Files:** Create `backend/internal/mcp/registry.go`, `catalog_read.go`, `catalog_write.go`, `schema.go`, `registry_test.go`; embed OpenAPI via a copy generated by `go:generate` into `backend/internal/mcp/openapi.yaml` (source of truth stays `docs/openapi.yaml`; a test fails if the copy differs).

**Interfaces — Produces:**
```go
type Tool struct {
    Name, Domain, Description string
    Method, Path string          // path template with {cluster} etc.
    Scope string                 // read|messages|write|destructive
    ReadOnly, Destructive, Idempotent bool
    Paginated bool               // adds limit/cursor handling in dispatch
    Preview func(ctx context.Context, d *Dispatcher, c Call) (any, error) // optional, writes only
}
type Call struct{ Cluster string; Args map[string]any }
func Catalog() []Tool
func (t Tool) InputSchema() map[string]any   // from OpenAPI params + body; adds limit/cursor/search where Paginated; writes add optional "confirmation"
func (t Tool) OutputSchema() map[string]any
var Excluded = map[string]string{ /* "METHOD /path": reason */ }
```

- [ ] **Step 1:** Tests: `TestCatalogParity` (every OpenAPI `method path` is in `Catalog()` or `Excluded`, and every catalog entry matches a real operation); `TestCatalogNamesUnique` (regex `^kaflux_[a-z0-9_]+$`, ≤64 chars); `TestWritesHaveConfirmationParam`; `TestReadToolsAreReadOnlyAnnotated` (every `Scope==read|messages` tool has `ReadOnly && !Destructive`, every `destructive` scope tool has `Destructive`); `TestSchemasValid` (each schema compiles with the SDK's schema validator); `TestToolDescriptionsShort` (≤ 400 chars, no newline run).
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Fill the catalog from the Tool Catalog section above, exact names. Descriptions state what, key args with an example value, and what the result contains. Mark `Paginated` on every list-style read.
- [ ] **Step 4:** Run → PASS. If the parity test finds REST operations missing from the catalog, add them (or add to `Excluded` with a reason), never loosen the test.
- [ ] **Step 5:** Commit `feat(mcp): add tool registry and schema derivation`.

### Task 4: In-process dispatcher

**Files:** Create `backend/internal/mcp/dispatch.go`, `dispatch_test.go`, `limits.go`, `limits_test.go`.

**Interfaces — Consumes:** `api.WithMCPSession`, `Tool`. **Produces:**
```go
type Dispatcher struct{ /* handler http.Handler, limits, clock */ }
func NewDispatcher(h http.Handler, l Limits) *Dispatcher
type Result struct {
    Status int; Body json.RawMessage
    Truncated bool; Total, Returned int
}
type ToolError struct{ Code, Message, Hint string; Status int }
func (d *Dispatcher) Do(ctx context.Context, p Principal, t Tool, c Call) (Result, *ToolError)
type Principal struct{ Session auth.Session; Grants []auth.Grant; TokenID, TokenName string }
type Limits struct{ MaxBytes int; Timeout time.Duration; PerMinute, Concurrency int }
```

- [ ] **Step 1:** Tests: `TestDoFillsPathAndQuery` (cluster/topic path params URL-escaped; query args encoded), `TestDoMapsErrors` (403 → `forbidden` with hint naming the action; 404 → hint pointing at the list tool; 503 → retryable hint), `TestDoTruncatesByBytes` (fake handler returning 1 MiB → `Truncated`, body ≤ cap, valid JSON), `TestDoPaginatesLists` (limit default 50, max 200 clamp; `Total` from array length), `TestDoTimeout` (slow handler → timeout error within `Timeout`+slack), `TestRateLimit429` (PerMinute exceeded → `rate_limited` with retry-after hint), `TestConcurrencyCap`, `TestDoSetsAuditIdentity` (handler observes session user provider `mcp`, id `<owner>/<token>`).
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement with `httptest.NewRecorder`-style in-memory writer; request has `RemoteAddr` set from the original MCP request so audit source IP is real; body unknown-field rejection comes from the REST `decode`. Truncation: cut arrays (`items`/top-level array) to fit, else cut and flag. Rate limit: per-token token bucket in memory.
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat(mcp): dispatch tool calls through the REST handler`.

### Task 5: Two-phase write confirmation

**Files:** Create `backend/internal/mcp/confirm.go`, `confirm_test.go`; extend `dispatch.go` with `Prepare`/`Commit`.

**Interfaces — Produces:**
```go
type Confirmer struct{ /* hmac key (random per process unless config secret), ttl, replay cache */ }
func NewConfirmer(key []byte, ttl time.Duration, now func() time.Time) *Confirmer
func (c *Confirmer) Mint(p Principal, t Tool, call Call) (token string, expires time.Time)
func (c *Confirmer) Verify(token string, p Principal, t Tool, call Call) (prior *Result, err error)
func (c *Confirmer) Record(token string, r Result) // enables idempotent re-commit
type PrepareOutput struct{ Plan PlanSummary; Preview any; Confirmation string; ExpiresAt time.Time; Destructive bool }
type PlanSummary struct{ Tool, Method, Path string; Arguments map[string]any; ScopeRequired string }
func (d *Dispatcher) Prepare(ctx context.Context, p Principal, t Tool, c Call) (PrepareOutput, *ToolError)
func (d *Dispatcher) Commit(ctx context.Context, p Principal, t Tool, c Call, confirmation string) (Result, *ToolError)
```
Key persistence: if `mcp.confirmationSecretEnv` is set use it, else per-process random key (confirmations die on restart; acceptable and documented). With several replicas the secret is required; document in Task 11.

- [ ] **Step 1:** Tests: `TestConfirmRoundTrip`; `TestConfirmExpired` (clock +5m01s); `TestConfirmArgsTampered` (one arg changed); `TestConfirmOtherPrincipal`; `TestConfirmOtherTool`; `TestConfirmReplayReturnsPriorResult` (second commit returns the recorded result, REST handler called once); `TestConfirmBitFlipRejected`; `TestPrepareDoesNotMutate` (handler spy sees only GETs during prepare); `TestPrepareWritesAuditRow`; `TestBulkCap` (51 targets → error); `TestPrepareRunsPreviewHook`.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Token = base64url(payload ‖ HMAC-SHA256); payload carries principal id, tool, cluster, args hash (SHA-256 of canonical JSON), expiry, nonce. Prepare audit row action `mcp.prepare:<tool>`. Implement `Preview` hooks for: `topic_delete` (topic details + partition count + size), `topic_bulk_delete` (resolved target list), `group_delete`, `group_reset_offsets` (current vs target offsets via REST preview/offset reads), `rebalance_execute`/`rebalance_rollback` (`rebalance_get` summary), `topic_truncate`, `topic_recreate`, `acl_delete` (matching ACLs). Others return the argument echo only.
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat(mcp): add two-phase confirmation for write tools`.

### Task 6: MCP server, auth, tool exposure

**Files:** Create `backend/internal/mcp/server.go`, `server_test.go`, `metrics.go`. Modify `backend/internal/api/api.go` to mount `/mcp` (before the session check; `/mcp` does its own auth) — a small `Options.MCP http.Handler` field.

**Interfaces — Consumes:** Tasks 1–5. **Produces:**
```go
type Options struct {
    Store *store.Store; Handler http.Handler; Grants func() []auth.Grant
    Writes bool; AllowedOrigins []string; Limits Limits; ConfirmKey []byte
    Demo bool
}
func NewHandler(o Options) http.Handler // serves POST /mcp; 405 otherwise
```
Behavior: `Authorization: Bearer` → `MCPTokenByHash` → build `Principal` with `Narrow(ownerGrants, scopes, clusters)`; owner grants resolved each call from `Grants()` and the owner's current roles (token stores owner id/provider; resolve roles via current config grants for local/OIDC/LDAP identity as the session would have, store roles snapshot refreshed from the owner's last login if live lookup is impossible — builder documents which in `docs/mcp.md`). Origin check; `?toolsets=` filter; `tools/list` filtered by scope and `Writes`; every `tools/call` re-checks the same predicate. Write tools: call without `confirmation` → `Prepare`; with → `Commit`. Result returns `structuredContent` plus a compact text rendering. Metrics `kaflux_mcp_calls_total{tool,result}`, `kaflux_mcp_call_seconds`, `kaflux_mcp_denied_total{reason}` registered through the existing `/metrics` route (extend the handler's output).

- [ ] **Step 1:** Tests using the SDK client over `httptest.NewServer` (streamable HTTP, stateless): `TestNoTokenIs401`; `TestRevokedTokenDeniedNextCall`; `TestExpiredToken`; `TestListToolsReadOnlyToken` (no write/destructive names present); `TestHiddenWriteToolCallDenied`; `TestToolsetsFilter`; `TestWritesDisabledHidesWriteTools` (even for `write`+`destructive` token when `Writes=false`); `TestPrepareThenCommitCreatesTopicInDemo`; `TestOriginRejected`; `TestCookieIgnoredOnMCP`; `TestOwnerGrantRemovedDeniesImmediately`; `TestAuthMatrix` (table: every catalog tool × {viewer-only grants, operator, admin, token-narrowed, other-cluster allowlist} expects the same allow/deny as calling the REST route with a session for that user — failures list tool and persona).
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement the handler with the SDK's per-request server factory, registering low-level tools from `Catalog()` filtered per principal. Output both content and structured content; set annotations from `Tool`. Use the adapter pattern: only `server.go` imports the SDK so version changes stay local.
- [ ] **Step 4:** `go test ./internal/mcp ./internal/api -race` → PASS. Check `TestAuthMatrix` failures individually; fix the session adapter, never special-case a route.
- [ ] **Step 5:** Commit `feat(mcp): serve tools over streamable HTTP with scoped tokens`.

### Task 7: Resources and prompts

**Files:** Create `backend/internal/mcp/resources.go`, `prompts.go`, tests in `server_test.go` additions.

- [ ] **Step 1:** Tests: `TestResourcesListed` (`kaflux://clusters`, template `kaflux://clusters/{id}/overview`, `kaflux://clusters/{id}/topics/{topic}`); `TestResourceAuthorized` (denied cluster → error, same as tool); `TestPromptsListed` (`diagnose-consumer-lag`, `topic-health-review`, `plan-safe-rebalance`, `audit-acl-changes`); `TestPromptMentionsConfirmationProtocol` (write-capable prompts name the prepare/commit steps and only reference tools that exist in `Catalog()`).
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement; resources dispatch via `Dispatcher` read tools, prompts are static templates with arguments (`cluster`, `group`, `topic`). **Step 4:** PASS. **Step 5:** Commit `feat(mcp): add resources and prompts`.

### Task 8: Token management API

**Files:** Create `backend/internal/api/mcptokens.go`, `mcptokens_test.go`. Modify `api.go` route dispatch, `auth.Actions` (add `manage-mcp`), `docs/openapi.yaml` (`GET/POST /admin/mcp-tokens`, `DELETE /admin/mcp-tokens/{id}`), `openapi_test.go` expectations if any.

- [ ] **Step 1:** Tests: `TestCreateTokenReturnsSecretOnce` (POST returns `secret` once; GET never does); `TestCreateRequiresExpiry` and `TestExpiryMax90Days`; `TestScopesValidated` (unknown scope 400; `write`/`destructive` rejected when `mcp.writes` disabled? — allowed but inert, response includes `writesEnabled`); `TestUserSeesOwnTokensAdminSeesAll`; `TestRevokeOwnOrAdmin`; `TestCannotCreateTokenWithoutPermission` (`manage-mcp`); `TestTokenCannotCreateToken` (a bearer on `/api/v1/admin/mcp-tokens` → 401); `TestCreateAuditsAndCSRF`.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement; reject scopes the owner holds no grant for (a viewer cannot mint a `destructive` token). **Step 4:** PASS, plus `go test ./internal/api -run OpenAPI`. **Step 5:** Commit `feat(api): add MCP token management endpoints`.

### Task 9: Config and wiring

**Files:** Modify `backend/internal/config/config.go` (+ `config_test.go`), `backend/cmd/kaflux/main.go:178`, `config.example.yaml`, `deploy/helm/kaflux` values and templates, compose files.

Config block (YAML under `runtime.mcp`, each with env override `KAFLUX_MCP_*`): `enabled` false, `writes` `disabled|enabled` (default `disabled`), `allowedOrigins` [], `maxResponseBytes` 262144, `callTimeoutSeconds` 30, `rateLimitPerMinute` 120, `concurrencyPerToken` 4, `confirmationSecretEnv` "".

- [ ] **Step 1:** Tests: defaults; invalid `writes` value fails startup; `enabled` with multiple replicas hint is documented, not enforced; `TestMCPDisabledNotMounted` (404 on `/mcp`). **Step 2:** FAIL. **Step 3:** Implement and mount `mcp.NewHandler` only when enabled; Helm values and `values.schema.json` if present. **Step 4:** `go test ./...` and `helm lint deploy/helm/kaflux` → pass. **Step 5:** Commit `feat(config): add MCP settings`.

### Task 10: Token UI

**Files:** Create `frontend/src/McpAccess.tsx`, `McpAccess.test.tsx`, `McpAccess.css`. Modify `frontend/src/api.ts` (types), the Administration host component (where `ClusterSettings`/session admin sections are mounted), `frontend/src/test-permissions.ts`.

- [ ] **Step 1:** Vitest tests: `renders disabled state when MCP is off`; `create form validates name, scopes, expiry (NumberStepper, 1–90)`; `shows secret once with copy and connection snippets (Claude Desktop JSON, claude mcp add command, generic)`; `secret not shown after reload`; `lists tokens with last used and status`; `revoke asks confirmation and refreshes`; `write scopes show a warning when writes are disabled`; `failure state shows retry`; `hidden without manage-mcp`.
- [ ] **Step 2:** `cd frontend && npx vitest run src/McpAccess.test.tsx` → FAIL. **Step 3:** Implement following existing Administration components and `Feedback`/`LoadingPanel` patterns. **Step 4:** Vitest → PASS; `npm run lint && npm run build`. **Step 5:** Verify in a real browser (agent-browser preferred): create a token, copy snippet, revoke; test narrow viewport; capture screenshots for the PR. Commit `feat(ui): add MCP access management`.

### Task 11: Documentation

**Files:** Create `docs/mcp.md` (setup, scopes, write protocol, replica secret requirement, client snippets, tool catalog table generated by `go run ./cmd/mcpdoc` or a test-checked section), edit `docs/security.md`, `docs/authentication.md`, `README.md`, `docs/status.md` (state what is and is not verified).

- [ ] **Step 1:** Test `TestDocsListEveryTool` in `backend/internal/mcp` reads `docs/mcp.md` and fails if any catalog tool name is missing. **Step 2:** FAIL. **Step 3:** Write docs; generate the catalog table from `Catalog()`. **Step 4:** PASS. **Step 5:** Commit `docs(mcp): document setup, scopes and write protocol`.

### Task 12: Integration and evaluations

**Files:** Create `backend/internal/mcp/integration_test.go` (existing integration tag), `docs/mcp/evaluation.xml`.

- [ ] **Step 1:** Integration tests with the SDK client against the Testcontainers Kafka used by `internal/kafka` integration tests: list topics; prepare+commit create topic then delete (destructive token); unavailable broker returns retryable tool error; stale rebalance plan commit returns the REST conflict, not success; audit shows prepare and commit rows with provider `mcp`.
- [ ] **Step 2:** Run with the project's integration command (see `docs/integration-testing.md`) → PASS.
- [ ] **Step 3:** Following the mcp-builder evaluation guide, start demo mode, connect MCP Inspector (`npx @modelcontextprotocol/inspector`), explore with read-only tools, write 10 independent, multi-tool, stable, verifiable questions over demo data, solve each yourself, save `docs/mcp/evaluation.xml`. Run the guide's evaluation script if its dependencies are available; otherwise state that it was not run.
- [ ] **Step 4:** Commit `test(mcp): add integration tests and evaluation set`.

### Task 13: Final verification and handoff

- [ ] **Step 1:** `cd backend && go vet ./... && go test ./... -race`; frontend `npm run lint && npx vitest run && npm run build`; `graphify update .`; report exact outputs, including any failures.
- [ ] **Step 2:** Manual end-to-end in demo mode with a real client (Claude Code: `claude mcp add --transport http kaflux http://localhost:8080/mcp --header "Authorization: Bearer <token>"`): list topics, prepare a topic create, confirm, verify audit row in the UI, revoke token and confirm the next call fails.
- [ ] **Step 3:** `bd close` finished tasks; `git status`; propose commit/push/PR commands (title `feat(mcp): add MCP server with scoped tokens and confirmed write tools`, no AI attribution) and wait for approval. Do not merge the Release PR.
