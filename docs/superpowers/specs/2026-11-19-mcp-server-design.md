# MCP server for Kaflux — design

Status: draft for approval. Date: 2026-11-19.

## Intent

Let AI agents (Claude Desktop, Claude Code, Cursor, any MCP client) inspect and operate Kafka
clusters through Kaflux. The user asked for a production-ready server with read and write tools,
fast and precise, with broad coverage of what the REST API and UI already do.

The point is not a second API. It is the same governed API, exposed in a shape agents use well:
few round trips, small focused responses, errors that say what to do next.

## Goals

- Full coverage of the cluster-scoped REST surface (about 96 paths): topics, partitions, consumer
  groups, offsets, messages, ACLs, quotas, SCRAM users and tokens, brokers and configs, cluster
  info, transactions, schemas, connectors, probes, diagnostics, rebalances, audit.
- Read and write tools. Writes are safe by construction (see Write safety).
- No way around existing RBAC, cluster scoping, audit, or the rebalance plan/execute split.
- Remote, multi-user deployment: streamable HTTP on the existing server, stateless JSON.
- Token management UI (create, scope, expire, revoke, last used).
- Fast: in-process dispatch, no extra network hop, bounded per-call latency.
- Precise: typed input and output schemas, tool annotations, pagination, truncation flags.
- Proof it works: unit, authorization-matrix, parity and integration tests, plus 10 evaluation
  questions run against demo mode.

## Non-goals

- A stdio binary. Remote clients can bridge with `mcp-remote`; revisit if asked.
- MCP-native OAuth (authorization server). v1 uses scoped API tokens.
- Replacing or changing any REST endpoint behavior.
- Auth, login, OIDC and session-admin endpoints as tools. Agents never manage identity.
- Binary file download endpoints (`events.txt`, `report.csv`, `backup.json`) as separate tools;
  rebalance events and report are returned as structured data.

## Anti-goals (failure even if it technically works)

- An agent doing something its token's user could not do in the UI.
- A write that executes without a recorded intent and audit row.
- Message payloads reaching a third-party LLM without the token owner having opted in.
- Tool sprawl that burns the agent's context: huge descriptions, unbounded lists.

## Constraints from the codebase

- Go 1.26 backend. `internal/api` is one `ServeHTTP` with cookie session + CSRF, then per-route
  authorization via `auth.Authorizer.Allowed(user, cluster, action, resource)` and
  `a.audit(...)` with persisted intent before Kafka mutations (`docs/security.md`).
- No bearer-token auth exists today. `session()` only reads the `kaflux_session` cookie.
- `docs/openapi.yaml` plus `openapi_test.go` already enforce spec parity for REST.
- Mutations need backend authorization and audited approval; demo data stays labeled.
- Repo rules: numeric inputs use `NumberStepper`; every capability needs a UI surface; Conventional
  Commits; no AI attribution in commits or PRs.

## Approaches considered

1. **Embedded `/mcp` endpoint, in-process dispatch through the existing API handler (chosen).**
   Each tool maps to a REST route. The tool builds an internal request, attaches the token's
   principal as the session user, and runs it through `API.ServeHTTP`. Authorization, CSRF
   bypass-by-construction, audit, validation and error codes are exactly the REST behavior, so
   they cannot drift. Fast (no network), covers all 96 paths cheaply, and a parity test keeps the
   tool registry honest against `openapi.yaml`.
2. Embedded endpoint calling `kafka.Provider` directly. Slightly leaner per call, but duplicates
   authorization, audit and validation per tool, with high risk of drift and bypass. Rejected.
3. Separate sidecar (TypeScript, the mcp-builder default) calling REST. Adds a hop and a second
   artifact, and must re-map every response shape. Rejected for latency and deploy cost.

SDK: official `github.com/modelcontextprotocol/go-sdk` (tracks the spec, typed tools, streamable
HTTP, tool annotations). `mark3labs/mcp-go` was the alternative. This is my call.

## Design

### Transport and endpoint

- `POST /mcp` (streamable HTTP, stateless, JSON responses). `GET /mcp` returns 405 (no server
  push needed). Origin header validated against configured allowed origins to block DNS rebinding.
- Off by default. Config: `mcp.enabled`, `mcp.allowedOrigins`, `mcp.writes` (`disabled` default |
  `enabled`), `mcp.maxResponseBytes`, `mcp.callTimeout`, `mcp.rateLimitPerMinute`.
- Not served in demo mode with real credentials; demo mode serves demo data, labeled.

### Authentication: scoped API tokens

- New table/store entity `mcp_tokens`: id, name, owner user id + provider, hashed secret
  (SHA-256 of 256-bit random, shown once, prefix `kfx_`), scopes, cluster allowlist,
  expiry (required, max 90 days, default 30), created, last used, revoked.
- Token acts as its owner: effective grants = owner's grants ∩ token scopes ∩ cluster allowlist.
  A token can only narrow, never widen.
- Scopes: `read` (default), `messages` (consume payloads), `write`, `destructive`
  (deletes, truncate, recreate, reset offsets, delete-records, rollback). `write` and
  `destructive` also require `mcp.writes: enabled`.
- Owner disabled, grants removed, token revoked or expired: next call fails 401/403 immediately
  (checked per call, no cache beyond the request).
- `Authorization: Bearer kfx_...` is accepted only on `/mcp`. The MCP handler authenticates the
  token, builds a session for the owner and passes it to the REST handler through an unexported
  request-context key, so REST routes themselves never accept bearer tokens and outside requests
  cannot forge the session. Audit rows carry provider `mcp` and actor `<owner>/<token name>`.
- Management endpoints (not tools): `GET/POST /api/v1/admin/mcp-tokens`,
  `DELETE /api/v1/admin/mcp-tokens/{id}`, gated by a new `admin` action, documented in OpenAPI.

### Tool surface

- `tools/list` returns only tools the token's scopes allow (a read-only token sees about half),
  and an optional `?toolsets=topic,group` query narrows further, so agents are not flooded.
- Names `kaflux_<domain>_<verb>`, e.g. `kaflux_topic_list`, `kaflux_group_reset_offsets`.
  About 100 tools, one per REST operation, across: cluster, topic, partition/offset, group, message, acl, quota, scram/user,
  token (Kafka delegation tokens), broker/config, transaction, schema, connector, probe,
  diagnostics, rebalance, audit.
- One registry table in Go: name, domain, REST method + path template, input schema, annotations
  (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint=false`), required scope,
  RBAC action. A test fails if an `openapi.yaml` operation is neither mapped nor explicitly
  excluded, so new REST features cannot silently miss MCP.
- Input schemas derive from the OpenAPI request/parameter schemas, with hand-written
  descriptions and examples in field text. Output schemas declared; both `structuredContent` and
  a compact text rendering returned.
- Every list tool: `limit` (default 50, max 200), `cursor` or `offset`, optional `filter`/`search`.
  Responses add `total`, `returned`, `truncated`. Hard cap `mcp.maxResponseBytes`
  (default 256 KiB) with an explicit "narrow your query" hint.
- Messages tool: byte and count caps, payload bodies only with `messages` scope and the
  existing `consume` permission; headers and keys follow the same rule. Never logged.
- Errors: map REST error codes to tool errors (`isError: true`) with a `hint` field
  (e.g. `forbidden` → names the missing action and cluster; `not_found` → suggests the matching
  list tool). Never leak internals; reuse sanitized REST messages.

### Write safety

Every write tool is two-phase, so an agent cannot mutate on a single call:

1. **Prepare**: call without `confirmation`. Server validates input, runs REST dry-run/preview
   where one exists (rebalance dry-run, offset reset preview, topic config diff), computes impact
   (what will change, affected partitions/groups, destructive flag), and returns a
   `confirmation` token: single-use, 5-minute TTL, HMAC-bound to principal, tool, cluster and a
   hash of the exact arguments. Nothing is mutated; a `prepare` audit row is recorded.
2. **Commit**: call again with the same arguments plus `confirmation`. Any argument change,
   replay, expiry or different principal fails with a clear hint. The commit goes through the
   REST handler, so persisted-intent audit and RBAC apply as for UI users.

Additional rules:
- Rebalances keep their lifecycle: tools exist for generate-plan, dry-run, execute, pause, resume,
  cancel, rollback. `execute` and `rollback` need `destructive` scope and the existing distinct
  RBAC permissions. The prepare step returns the plan summary so the agent can show a human.
- Bulk operations (bulk-delete, ACL batches) cap item counts (default 50) and echo every target
  in the prepare result.
- Uncertain outcomes are reported as uncertain, never as success (matches `docs/security.md`).
- Idempotent retries: commit with a reused confirmation returns the original result instead of
  re-running, when the first commit succeeded.

### Resources and prompts

- Resources (read-only, small): `kaflux://clusters`, `kaflux://clusters/{id}/overview`,
  `kaflux://clusters/{id}/topics/{topic}`. Same authorization path as tools.
- Prompts: `diagnose-consumer-lag`, `topic-health-review`, `plan-safe-rebalance`,
  `audit-acl-changes`. Each states which tools to call in order and the confirmation protocol.

### Performance and robustness

- In-process dispatch; no marshalling beyond JSON body. Per-call timeout from config.
- Per-token rate limiting (token bucket) and a per-token concurrency cap; 429 with retry hint.
- Request body size limit mirrors REST (1 MiB). Unknown fields rejected, as in REST `decode`.
- Context cancellation propagates to Kafka calls.
- Metrics: `kaflux_mcp_calls_total{tool,result}`, latency histogram, denied-call counter.
  Structured logs with request ID and token id; never token secrets, never payloads.

### UI surface

Administration → "MCP access" section: create token (name, scopes, cluster allowlist, expiry via
`NumberStepper`), one-time secret reveal with copy and connection snippets (Claude Desktop,
Claude Code `claude mcp add`, generic JSON), list with last used and status, revoke. Honest empty
and disabled states ("MCP disabled by administrator"), loading and failure states. Screenshot
verification in a real browser per `AGENTS.md`.

### Documentation

`docs/mcp.md` (setup, scopes, write protocol, tool catalog generated from the registry),
updates to `docs/security.md`, `docs/authentication.md`, `docs/openapi.yaml` (token endpoints),
`config.example.yaml`, Helm values, README feature list.

## Testing and verification

- Unit: registry, schema derivation, confirmation token (replay, expiry, arg mismatch,
  cross-principal, tamper), scope narrowing, truncation, error hints.
- Authorization matrix: for every tool × {viewer, operator, admin, narrowed token, expired,
  revoked, wrong cluster} assert allow/deny identical to the REST route.
- Parity test: `openapi.yaml` operations vs registry (mapped or excluded with reason).
- Integration: official SDK client over streamable HTTP against demo mode and the Testcontainers
  Kafka suite for representative read and write flows, including unavailable broker and stale
  rebalance plan.
- Audit: every write produces prepare + commit rows with actor `<owner>/<token>`, provider `mcp`.
- Evaluations: 10 read-only, multi-tool, stable questions over demo-mode data in
  `docs/mcp/evaluation.xml`, answers verified by hand, per the mcp-builder evaluation guide.
- MCP Inspector smoke test documented. Frontend: Vitest plus browser check of the token flow.
- Gates before PR: `go vet`, `go test ./...`, frontend lint/test/build, `graphify update .`.

## Delivery

- One PR from a new branch off `main`: `feat(mcp): add MCP server with scoped tokens and
  confirmed write tools`. Minor release via release-please; no manual version edits.
- Current branch `feat/cluster-version` has unrelated uncommitted work; it is not touched.
- Work is tracked in beads (epic plus child tasks) before code starts.

## Decisions I made (yours to override)

- Official Go SDK, embedded endpoint, in-process dispatch.
- Writes off by default; two-phase confirmation on every write tool, including non-destructive.
- Token scopes `read`/`messages`/`write`/`destructive`; mandatory expiry, max 90 days.
- Message payloads need an explicit `messages` scope.
- No stdio, no MCP-native OAuth in v1.
- Page size 50 / max 200, response cap 256 KiB, confirmation TTL 5 minutes.

## Left to the builder

- Exact tool list and naming per route (registry must satisfy the parity test).
- Whether input schemas are generated at build time or loaded from the embedded OpenAPI at start.
- Store migration details for SQLite and PostgreSQL token tables.
- Rate-limit defaults, metric label names, connection snippet wording.
- Which REST endpoints lack a dry-run and need a computed preview instead.

## Open risks

- Go SDK API churn: pin the version and wrap it behind a thin internal adapter.
- Dispatching through `ServeHTTP` couples MCP to REST error shapes; covered by the matrix test.
- Some REST routes may rely on cookie-only assumptions (CSRF, session ID in audit); builder
  verifies each during the matrix test and fixes in the session adapter, not per route.
