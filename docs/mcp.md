# MCP server

Kaflux can expose its API to AI agents through the Model Context Protocol (MCP). An MCP client such
as Claude Code connects to `POST /mcp`, lists the tools it is allowed to use, and calls them. Every
tool call is dispatched through the same REST handlers the console uses, so the existing
authorization, audit and validation apply unchanged. MCP is off by default.

Enable it when you want an agent to inspect clusters, topics, consumer groups and lag, or to prepare
changes that a person reviews. Leave it off if nobody needs it: a disabled server answers `/mcp`
with 404 and mounts no MCP code path.

## Enable MCP

Settings live under `runtime.mcp` in the configuration file. Each has an environment override, which
takes precedence over the file. All values are validated at startup even while MCP is disabled, so a
typo fails the start instead of surfacing later.

| `runtime.mcp` key     | Environment variable                | Default    | Valid values                 |
| --------------------- | ----------------------------------- | ---------- | ---------------------------- |
| `enabled`             | `KAFLUX_MCP_ENABLED`                | `false`    | boolean                      |
| `writes`              | `KAFLUX_MCP_WRITES`                 | `disabled` | `disabled`, `enabled`        |
| `allowedOrigins`      | `KAFLUX_MCP_ALLOWED_ORIGINS`        | empty      | comma-separated `scheme://host[:port]` origins |
| `maxResponseBytes`    | `KAFLUX_MCP_MAX_RESPONSE_BYTES`     | `262144`   | 4096 to 16777216             |
| `callTimeoutSeconds`  | `KAFLUX_MCP_CALL_TIMEOUT_SECONDS`   | `30`       | 1 to 300                     |
| `rateLimitPerMinute`  | `KAFLUX_MCP_RATE_LIMIT_PER_MINUTE`  | `120`      | 1 to 6000, per token         |
| `concurrencyPerToken` | `KAFLUX_MCP_CONCURRENCY_PER_TOKEN`  | `4`        | 1 to 64, per token           |

```yaml
runtime:
  mcp:
    enabled: true
    writes: disabled
```

An explicit `0` for a numeric limit is rejected rather than replaced by the default. Origins must be
`http` or `https` with a host and no path, query, fragment or credentials.

With `writes: disabled`, only read-only tools exist: write and destructive tools are not listed and a
direct call to one is refused. Start there and enable writes only after you have reviewed
[Write protocol](#write-protocol).

### Helm

The chart passes the same settings as environment variables from the `mcp` block of `values.yaml`:

```yaml
mcp:
  enabled: true
  writes: disabled
  allowedOrigins: []
  maxResponseBytes: 262144
  callTimeoutSeconds: 30
  rateLimitPerMinute: 120
  concurrencyPerToken: 4
```

Because they are rendered as environment variables, these values override `runtime.mcp` in the
mounted configuration file. With more than one replica, also configure sticky routing for `/mcp`
(see [Multiple replicas](#multiple-replicas)); `values.yaml` has an ingress-nginx example.

### Docker Compose

`docker-compose.yaml` and `deploy/docker-compose.sqlite.yaml` contain commented pass-through lines
for `KAFLUX_MCP_ENABLED`, `KAFLUX_MCP_WRITES` and `KAFLUX_MCP_ALLOWED_ORIGINS`. Uncomment them and set
the values in `.env`, or set the same variables on the service yourself. The numeric limits are not
listed in the Compose files; add the variables from the table if you need them.

### Demo mode

With `KAFLUX_DEMO=true` the MCP server still requires `enabled`, and tool results are simulated data.
The server instructions it sends to clients start with `DEMO MODE`.

## Create a token

MCP clients authenticate with a bearer token, never with a console session cookie.

1. Sign in to the console as a user who holds the global `manage-mcp` grant (the built-in
   administrator role does).
2. Open Administration, then MCP access.
3. Enter a name (1 to 64 characters, no `/`), choose scopes, optionally restrict clusters, and set an
   expiry of 1 to 90 days. Expiry is required.
4. Copy the secret. It starts with `kfx_`, is shown once, and only its SHA-256 hash is stored. The page
   offers a ready-made `claude mcp add` command containing the secret; neither the page nor the URL
   keeps it after you dismiss the panel.

A user can hold at most 25 active tokens and cannot have two active tokens with the same name. Users
without `manage-mcp` cannot create tokens but can list and revoke their own. A user with `manage-mcp`
can list and revoke every user's tokens. Revoked and expired tokens are rejected on the next request. Revoked tokens disappear from the list; expired ones stay listed until they are revoked.

The creation call is audited as `mcp.token.create` and revocation as `mcp.token.revoke`, under the
console user. Creation is refused if the audit record cannot be written.

### Scopes

A token carries one or more scopes. What a call may do is the intersection of three things:

1. what the token owner is granted by role (configured `grants`, evaluated on every call);
2. the actions the token's scopes allow;
3. the token's cluster allowlist, if any. An empty allowlist means every cluster the owner holds.

A token therefore can never do more than its owner, and removing a grant from the configuration
takes effect on the next call. Token creation also refuses a scope for which the creator holds no
grant on the selected clusters.

| Scope         | Actions it allows                                                                                                    |
| ------------- | -------------------------------------------------------------------------------------------------------------------- |
| `read`        | `read`, `schema-read`, `connector-read`, `audit`                                                                     |
| `messages`    | `consume`                                                                                                            |
| `write`       | `produce`, `create`, `alter-config`, `rename`, `plan`, `manage-acls`, `schema-update`, `connector-update` |
| `destructive` | `delete`, `reset-offsets`, `execute`, `rollback`                                                                     |

`manage-sessions` and `manage-mcp` belong to no scope. Session and token administration are not
available over MCP at all.

Take care with `messages`: reading messages returns record keys, values and headers to the agent,
and therefore to the model provider behind it. Grant it only for clusters whose payloads you are
willing to send there. Responses are the REST API's responses; MCP adds no masking of message
content.

`write` and `destructive` do nothing unless `writes: enabled`. The console warns about this when you
select them. Rebalance pause, cancel, throttle and cancel-rollback need `destructive` because the
REST API authorizes them with `execute`.

## Sign in with OAuth

Clients that support the MCP authorization flow can sign in instead of being given a token. Point the
client at `https://HOST/mcp`. Its first call is refused with `401` and a `WWW-Authenticate` header that
names the discovery document, and the client does the rest:

1. It reads `/.well-known/oauth-protected-resource` and `/.well-known/oauth-authorization-server`.
2. It registers itself at `POST /oauth/register` (dynamic client registration, public clients only).
3. It opens `/oauth/authorize` in the browser with a PKCE `S256` challenge. Kaflux shows its usual
   sign-in screen if you have no session, then a consent screen naming the client.
4. You choose which of the requested scopes and clusters to allow, then allow or deny. Scopes your own
   grants do not cover are shown but cannot be granted.
5. The client exchanges the code at `POST /oauth/token` for an access token (1 hour) and a refresh token
   (30 days) that rotates on every use.

Only a user holding the global `manage-mcp` grant can approve, the same rule as creating a token. The
connection appears in MCP access marked **Signed in**, and revoking it ends its access tokens at once.
Approval, denial, issue and refresh are audited as `mcp.oauth.approve`, `mcp.oauth.deny`,
`mcp.oauth.token` and `mcp.oauth.refresh`.

Registrations, pending requests and authorization codes live in memory, so a restart makes a client
register again, which compliant clients do on their own. Run one replica or route `/oauth/*` and `/mcp`
with sticky sessions. Behind a proxy, forward the original `Host` and `X-Forwarded-Proto` so the
discovery document names the address clients use. Personal tokens remain the way to connect CI and other
headless clients.

## Connect a client

The endpoint is `https://HOST/mcp`. It speaks streamable HTTP in stateless mode with JSON responses
and accepts `POST` only. Use HTTPS outside local development, because the bearer token travels in
every request.

You can pass `?toolsets=topic,group` to limit a connection to those tool domains. The domains are
`acl`, `audit`, `balance`, `broker`, `cluster`, `connector`, `group`, `message`, `metrics`,
`rebalance`, `schema`, `topic` and `version`.

The client configurations below follow the clients' own documentation as understood when this page
was written; Kaflux's tests exercise the protocol with the Go SDK client only, not with these
products.

**Claude Code**

```bash
claude mcp add --transport http kaflux https://HOST/mcp \
  --header "Authorization: Bearer kfx_..."
```

**Claude Desktop and other clients without HTTP header support.** If the client only starts local
(stdio) servers, bridge it with `mcp-remote`, which forwards to the HTTP endpoint and adds the header:

```json
{
  "mcpServers": {
    "kaflux": {
      "command": "npx",
      "args": ["mcp-remote", "https://HOST/mcp", "--header", "Authorization: Bearer ${KAFLUX_MCP_TOKEN}"],
      "env": { "KAFLUX_MCP_TOKEN": "kfx_..." }
    }
  }
}
```

**Any other HTTP client.** Send JSON-RPC 2.0 messages as `POST /mcp` with
`Authorization: Bearer kfx_...`, `Content-Type: application/json` and
`Accept: application/json, text/event-stream`. The token is accepted only in the `Authorization`
header, never in the query string.

## Write protocol

Every write and destructive tool works in two steps, so a person (or the agent's own review step) sees
the plan before anything changes.

1. **Prepare.** Call the tool without `confirmation`. Nothing is changed. The response contains the
   `plan` (tool, REST method and path, the arguments), a `preview` where the tool can compute one
   (for example the topics a bulk delete would affect), a `destructive` flag, a `restConfirmation`
   text and a `confirmation` token with its `expiresAt`.
2. **Commit.** Repeat the identical call with the same arguments and add `"confirmation": "<token>"`.

The confirmation is valid for 5 minutes, is bound to the token, tool, cluster and arguments, and can
be used once. Any change to the arguments, or another MCP token, makes it fail with
`confirmation_mismatch`. Repeating a commit with the same confirmation does not write again; it
returns the first commit's recorded outcome.

Some REST operations also require their own confirmation value inside the request `body` (for
example a topic bulk delete or an offset reset apply). That value is separate from the MCP
`confirmation` and is not filled in for you. The prepare response's `restConfirmation` states exactly
what to put in `body`, and the commit must contain both.

A bulk write accepts at most 50 targets (for `kaflux_topic_bulk_delete`, entries of `body.topics`).
Longer lists are refused with `too_many_targets`; split them into batches and prepare each batch.

If a commit ends without a definite answer (timeout, cancellation, an internal error, or a 502, 503
or 504 from Kafka), Kaflux returns `outcome_uncertain` and does not retry. The change may have been
applied. Read the current state with the matching get or list tool before preparing again; replaying
the same confirmation only returns that same uncertain outcome.

A token never sees tools it cannot use. `tools/list` omits write tools when writes are disabled and
omits tools whose scope the token lacks. A direct call to a hidden tool is refused with `forbidden`
and a hint naming the missing scope or the disabled setting.

## Limits

| Limit                | Value                                                                                               |
| -------------------- | --------------------------------------------------------------------------------------------------- |
| List page size       | 50 by default, at most 200 (`limit`; `cursor` and `search` also accepted where the route supports them) |
| Response size        | `maxResponseBytes` (256 KiB by default). A list is cut to the items that fit and returns `truncated` and a `nextCursor`; another response becomes a `truncated` preview with a hint to narrow the query |
| Request body         | 1 MiB, as in the REST API                                                                           |
| Call rate            | `rateLimitPerMinute` per token (120 by default), as a token bucket                                  |
| Concurrency          | `concurrencyPerToken` calls in flight per token (4 by default)                                      |
| Call timeout         | `callTimeoutSeconds` (30 by default)                                                                |
| Failed authentication | 120 failures per minute per client IP, then 429 with `Retry-After: 60`                             |
| Write confirmation   | 5 minutes, single use                                                                               |
| Bulk targets         | 50 per call                                                                                         |
| Tokens               | 25 active per user, expiry at most 90 days                                                          |

Rate and concurrency refusals are returned as tool errors with code `rate_limited` and a retry hint.
A prepare costs one call of the rate budget, plus the calls its preview needs to read the target.

## Deployment notes

### Multiple replicas

Confirmation tokens are signed with a key drawn at process start, and the used-token memory is held
in that process. A confirmation therefore only works on the replica that issued it, and not after a
restart. This fails closed (the commit is refused, never repeated), but with several replicas a
prepare and its commit must reach the same one. Route `/mcp` by token: with ingress-nginx,
`nginx.ingress.kubernetes.io/upstream-hash-by: "$http_authorization"`. Cookie affinity does not work
because MCP clients send no cookies. Kaflux logs a warning at startup when MCP is enabled with the
PostgreSQL backend.

The rate and concurrency limits, the failed-authentication limiter and the 25-token creation limit
are also per process and best effort. With N replicas the effective rate budget can be up to N times
the configured value unless requests are routed by token.

### Origin and proxies

Browsers send an `Origin` header; CLI and desktop MCP clients do not. A request without `Origin` is
accepted. A request with `Origin` is accepted only if it matches an `allowedOrigins` entry by scheme,
host and port (default ports are implied), so leaving the list empty refuses every browser-originated
request. The `Host` header is never trusted for this decision.

The failed-authentication limiter keys on the direct peer address. Behind a reverse proxy that is the
proxy's address, so many clients share one failure budget. A valid token is never blocked by that
limiter: only failed attempts are counted.

`/metrics` is the existing unauthenticated route; the MCP series below are appended to it.

### Role snapshot

A token records its owner's roles when it is created. Each call evaluates the configured `grants`
against that snapshot. Grant changes in the configuration apply immediately, but a change of the
owner's roles at the identity provider does not reach existing tokens. When someone leaves or is
demoted, revoke their tokens in Administration, MCP access; otherwise they remain usable until they
expire (at most 90 days).

## Audit

- Token creation and revocation are recorded as `mcp.token.create` and `mcp.token.revoke` with the
  console user as actor.
- A prepare writes an audit row before it returns a confirmation, with action `mcp.prepare:<tool>`
  and result `prepared`. If the audit store is unavailable the prepare is refused with `audit_failed`.
- A commit goes through the normal REST handler, which writes its usual intent and result rows.
- Rows made on behalf of a token have provider `mcp`, the actor `<owner id>/<token name>` and the
  source IP of the MCP request. Request correlation IDs follow the REST API's.

Each call also logs `mcp call` with the token ID, tool, result and duration. Neither logs nor audit
rows contain the token secret or message payloads.

## Metrics

With MCP enabled, `/metrics` additionally exposes:

| Metric                       | Type      | Labels   | Meaning                                                                                |
| ---------------------------- | --------- | -------- | -------------------------------------------------------------------------------------- |
| `kaflux_mcp_calls_total`     | counter   | `tool`, `result` | Tool calls; `result` is `ok`, `prepared`, `error` or `denied`                  |
| `kaflux_mcp_call_seconds`    | histogram | none     | Tool call latency                                                                      |
| `kaflux_mcp_denied_total`    | counter   | `reason` | Denials: `unauthenticated`, `origin`, `scope`, `writes_disabled`, `toolset`, `permission`, `rate_limited`, `auth_rate_limited` |

Token IDs, names and secrets are never used as labels.

## Troubleshooting

| Symptom | Cause and fix |
| ------- | ------------- |
| HTTP 404 on `/mcp` | MCP is disabled. Set `runtime.mcp.enabled` or `KAFLUX_MCP_ENABLED=true` and restart. |
| HTTP 401 `unauthenticated` | Missing, malformed, unknown, revoked or expired token, or the secret was sent outside the `Authorization: Bearer` header. Create a new token. |
| HTTP 403 `origin_forbidden` | The request carried an `Origin` that is not in `allowedOrigins`. Add the exact origin, or use a client that sends none. |
| HTTP 405 | Only `POST` is accepted. |
| HTTP 429 `rate_limited` | Too many failed authentications from one address. Fix the token and wait for `Retry-After`. |
| Tool error `rate_limited` | The token exceeded its per-minute budget or its concurrent calls. Wait the stated seconds or lower the client's parallelism. |
| Tool error `forbidden` | The token's scope, the writes setting or the connection's `toolsets` hide the tool, or the owner has no grant for this action on this cluster. The hint names which. |
| `confirmation_invalid`, `confirmation_expired`, `confirmation_mismatch` | The confirmation is malformed, older than 5 minutes, issued by another process or before a restart, or the arguments changed. Prepare again and commit with the new token unchanged. |
| `confirmation_busy` | Too many unused confirmations are outstanding in the process. Retry after a few minutes. |
| `outcome_uncertain` | The write may have been applied. Read the current state before preparing again. |
| `audit_failed` | The audit store is unavailable; nothing was prepared. |
| `too_many_targets` | A bulk write listed more than 50 targets. Split it. |
| `response_too_large`, `truncated` | Narrow the query or page with `cursor`. |
| A tool does not appear in the client | Writes are disabled, the token lacks the scope, the connection uses `?toolsets=`, or the client cached the list: reconnect. |
| Token cannot be created | The creator lacks `manage-mcp`, lacks a grant for the scope on the chosen clusters, has 25 active tokens, or the audit store is unavailable. |

## Resources and prompts

Besides tools the server offers read-only resources, shown to a token only when it can use the
matching read tool:

- `kaflux://clusters`, the clusters the token may see;
- `kaflux://clusters/{id}/overview`, a cluster overview;
- `kaflux://clusters/{id}/topics/{topic}`, one topic's details.

It also offers four prompts: `diagnose-consumer-lag`, `topic-health-review`, `plan-safe-rebalance`
and `audit-acl-changes`. Prompts only describe a workflow in terms of the tools above; they grant
nothing.

## Tool catalog

The table is generated from the tool registry and checked by a test, so it cannot drift from the
code. To refresh it after changing the catalog run
`cd backend && go test ./internal/mcp -run TestDocsCatalogTable -update-docs`. REST paths are relative
to `/api/v1`. "Read-only" tools never change state; "changes state" and "destructive" tools use the
[write protocol](#write-protocol) and exist only with `writes: enabled`.

<!-- BEGIN GENERATED TOOL CATALOG (go test ./internal/mcp -run TestDocsCatalogTable -update-docs) -->

| Tool | REST operation | Scope | Access |
|---|---|---|---|
| `kaflux_version_get` | `GET /version` | read | read-only |
| `kaflux_cluster_list` | `GET /clusters` | read | read-only |
| `kaflux_audit_global` | `GET /audit` | read | read-only |
| `kaflux_metrics_catalog_global` | `GET /metrics/catalog` | read | read-only |
| `kaflux_cluster_overview` | `GET /clusters/{cluster}/overview` | read | read-only |
| `kaflux_cluster_capabilities` | `GET /clusters/{cluster}/capabilities` | read | read-only |
| `kaflux_cluster_configuration` | `GET /clusters/{cluster}/configuration` | read | read-only |
| `kaflux_balance_get` | `GET /clusters/{cluster}/balance` | read | read-only |
| `kaflux_topic_list` | `GET /clusters/{cluster}/topics` | read | read-only |
| `kaflux_topic_get` | `GET /clusters/{cluster}/topics/{topic}` | read | read-only |
| `kaflux_topic_config_get` | `GET /clusters/{cluster}/topics/{topic}/config` | read | read-only |
| `kaflux_topic_balance` | `GET /clusters/{cluster}/topics/{topic}/balance` | read | read-only |
| `kaflux_topic_unused` | `GET /clusters/{cluster}/unused-topics` | read | read-only |
| `kaflux_broker_list` | `GET /clusters/{cluster}/brokers` | read | read-only |
| `kaflux_group_list` | `GET /clusters/{cluster}/consumer-groups` | read | read-only |
| `kaflux_group_get` | `GET /clusters/{cluster}/consumer-groups/{group}` | read | read-only |
| `kaflux_acl_list` | `GET /clusters/{cluster}/acls` | read | read-only |
| `kaflux_schema_integration_list` | `GET /clusters/{cluster}/schemas` | read | read-only |
| `kaflux_schema_subject_list` | `GET /clusters/{cluster}/schemas/{integration}` | read | read-only |
| `kaflux_schema_get` | `GET /clusters/{cluster}/schemas/{integration}/{subject}` | read | read-only |
| `kaflux_schema_versions` | `GET /clusters/{cluster}/schemas/{integration}/{subject}/versions` | read | read-only |
| `kaflux_schema_version_get` | `GET /clusters/{cluster}/schemas/{integration}/{subject}/versions/{version}` | read | read-only |
| `kaflux_schema_config_get` | `GET /clusters/{cluster}/schemas/{integration}/{subject}/configuration` | read | read-only |
| `kaflux_schema_compatibility_check` | `POST /clusters/{cluster}/schemas/{integration}/{subject}/compatibility` | read | read-only |
| `kaflux_connector_integration_list` | `GET /clusters/{cluster}/connectors` | read | read-only |
| `kaflux_connector_list` | `GET /clusters/{cluster}/connectors/{integration}` | read | read-only |
| `kaflux_connector_get` | `GET /clusters/{cluster}/connectors/{integration}/{connector}` | read | read-only |
| `kaflux_connector_config_get` | `GET /clusters/{cluster}/connectors/{integration}/{connector}/config` | read | read-only |
| `kaflux_connector_status` | `GET /clusters/{cluster}/connectors/{integration}/{connector}/status` | read | read-only |
| `kaflux_audit_cluster` | `GET /clusters/{cluster}/audit` | read | read-only |
| `kaflux_metrics_catalog` | `GET /clusters/{cluster}/metrics/catalog` | read | read-only |
| `kaflux_metrics_query` | `GET /clusters/{cluster}/metrics/query` | read | read-only |
| `kaflux_rebalance_list` | `GET /clusters/{cluster}/rebalances` | read | read-only |
| `kaflux_rebalance_get` | `GET /clusters/{cluster}/rebalances/{job}` | read | read-only |
| `kaflux_rebalance_events` | `GET /clusters/{cluster}/rebalances/{job}/events` | read | read-only |
| `kaflux_message_consume` | `GET /clusters/{cluster}/messages` | messages | read-only |
| `kaflux_message_search` | `POST /clusters/{cluster}/messages/search` | messages | read-only |
| `kaflux_topic_create` | `POST /clusters/{cluster}/topics` | write | changes state |
| `kaflux_topic_config_update` | `POST /clusters/{cluster}/topics/{topic}/config` | write | changes state |
| `kaflux_topic_partitions_add` | `POST /clusters/{cluster}/topics/{topic}/partitions` | write | changes state |
| `kaflux_topic_copy` | `POST /clusters/{cluster}/topics/{topic}/copy` | write | changes state |
| `kaflux_topic_delete` | `DELETE /clusters/{cluster}/topics/{topic}` | destructive | destructive |
| `kaflux_topic_truncate` | `POST /clusters/{cluster}/topics/{topic}/truncate` | destructive | destructive |
| `kaflux_topic_recreate` | `POST /clusters/{cluster}/topics/{topic}/recreate` | destructive | destructive |
| `kaflux_topic_bulk_delete` | `POST /clusters/{cluster}/topics/bulk-delete` | destructive | destructive |
| `kaflux_group_reset_offsets` | `POST /clusters/{cluster}/consumer-groups/{group}/reset-offsets` | destructive | destructive |
| `kaflux_acl_create` | `POST /clusters/{cluster}/acls` | write | changes state |
| `kaflux_acl_delete` | `DELETE /clusters/{cluster}/acls` | destructive | destructive |
| `kaflux_schema_register` | `POST /clusters/{cluster}/schemas/{integration}/{subject}` | write | changes state |
| `kaflux_schema_config_update` | `PUT /clusters/{cluster}/schemas/{integration}/{subject}/configuration` | write | changes state |
| `kaflux_schema_subject_delete` | `DELETE /clusters/{cluster}/schemas/{integration}/{subject}` | destructive | destructive |
| `kaflux_schema_version_delete` | `DELETE /clusters/{cluster}/schemas/{integration}/{subject}/versions/{version}` | destructive | destructive |
| `kaflux_connector_upsert` | `PUT /clusters/{cluster}/connectors/{integration}/{connector}` | write | changes state |
| `kaflux_connector_config_update` | `PUT /clusters/{cluster}/connectors/{integration}/{connector}/config` | write | changes state |
| `kaflux_connector_pause` | `POST /clusters/{cluster}/connectors/{integration}/{connector}/pause` | write | changes state |
| `kaflux_connector_resume` | `POST /clusters/{cluster}/connectors/{integration}/{connector}/resume` | write | changes state |
| `kaflux_connector_restart` | `POST /clusters/{cluster}/connectors/{integration}/{connector}/restart` | write | changes state |
| `kaflux_connector_task_restart` | `POST /clusters/{cluster}/connectors/{integration}/{connector}/tasks/{task}/restart` | write | changes state |
| `kaflux_connector_delete` | `DELETE /clusters/{cluster}/connectors/{integration}/{connector}` | destructive | destructive |
| `kaflux_cluster_rename` | `PUT /clusters/{cluster}/name` | write | changes state |
| `kaflux_message_produce` | `POST /clusters/{cluster}/messages` | write | changes state |
| `kaflux_rebalance_plan` | `POST /clusters/{cluster}/rebalances` | write | changes state |
| `kaflux_rebalance_dry_run` | `POST /clusters/{cluster}/rebalances/{job}/dry-run` | write | changes state |
| `kaflux_rebalance_pause` | `POST /clusters/{cluster}/rebalances/{job}/pause` | destructive | destructive |
| `kaflux_rebalance_resume` | `POST /clusters/{cluster}/rebalances/{job}/resume` | destructive | destructive |
| `kaflux_rebalance_cancel` | `POST /clusters/{cluster}/rebalances/{job}/cancel` | destructive | destructive |
| `kaflux_rebalance_cancel_rollback` | `POST /clusters/{cluster}/rebalances/{job}/cancel-rollback` | destructive | destructive |
| `kaflux_rebalance_throttle` | `POST /clusters/{cluster}/rebalances/{job}/throttle` | destructive | destructive |
| `kaflux_rebalance_execute` | `POST /clusters/{cluster}/rebalances/{job}/execute` | destructive | destructive |
| `kaflux_rebalance_rollback` | `POST /clusters/{cluster}/rebalances/{job}/rollback` | destructive | destructive |

<!-- END GENERATED TOOL CATALOG -->

## Limits of this release

- The catalog covers the REST operations present in this release's OpenAPI document. Operations
  that are not in it are not available as tools. A parity test fails when the document and catalog
  diverge, so new REST routes must be added or explicitly excluded.
- Some operations are deliberately not tools: session and token administration, sign-in and identity
  flows, and a few others. The registry lists each exclusion with its reason.
- Transport is streamable HTTP only. There is no stdio server.
- Authentication is by Kaflux-issued bearer tokens, either created in the console or issued through the
  OAuth sign-in flow. OAuth supports public clients with PKCE only; there are no client secrets, and
  role changes at the identity provider reach a connection only after it is revoked or its refresh
  token expires.
- There is no per-tool allowlist beyond scopes, clusters and `toolsets`.

See [Security](security.md), [Authentication](authentication.md) and
[Implementation status](status.md) for the surrounding controls and what has been verified.
