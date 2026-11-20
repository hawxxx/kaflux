# Security

## Trust boundaries

All Kafka mutations require backend authorization. Credentials are resolved from environment
variables or mounted files and never returned by configuration APIs. Use HTTPS at the ingress;
cookies are HttpOnly, SameSite=Lax, and Secure outside local development. CSRF tokens protect
cookie-authenticated mutations.

## Policy

Grants bind role, cluster, action, and resource pattern. Default deny. Distinguish glob patterns
from explicit regex; compile regex safely and reject invalid rules. Viewer reads metadata; message
consumption has its own permission. Execute and rollback are distinct from generate-plan.
Identity-provider group mappings are configured by administrators, not accepted from arbitrary
client headers.

## Mutations and audit

Audit actor, provider, request ID, source IP, resource, intent, result, and sanitized changes.
Persist intent before Kafka mutation. Record uncertainty and reconcile rather than reporting
success. Never log message bodies, authorization headers, secrets, or complete connection strings.

## Deployment

Non-root containers, read-only root filesystem, dropped capabilities, explicit resource limits,
network policies, secret mounts, and workload identity are the defaults. Reverse-proxy
authentication is disabled unless trusted proxy networks and identity headers are configured. Block
outbound destinations through network policy; integration URLs are administrator configuration, not
user-supplied fetch targets.

## MCP access

The optional [MCP server](mcp.md) lets AI agents call the API at `/mcp`. It is off by default.

- **Trust boundary.** An agent holds a bearer token, and everything it reads is sent to the model
  provider behind the agent. The `messages` scope returns record payloads, so grant it deliberately.
- **Token model.** Tokens are `kfx_` secrets shown once; only a SHA-256 hash is stored. They expire
  within 90 days, and an owner may hold 25 active tokens. A call is authorized as the intersection of
  the owner's role grants, the token's scopes and its cluster allowlist, evaluated per call, so a
  token never exceeds its owner. The owner's roles are snapshotted at creation: identity-provider role
  changes do not reach existing tokens, so revoke tokens when a person leaves or is demoted.
- **Write confirmation.** With `writes: enabled`, each write is prepared (nothing changes), then
  committed with a single-use, five-minute confirmation bound to the token and arguments. Confirmations
  are per process; use sticky routing for `/mcp` with several replicas.
- **Browser requests.** Requests with an `Origin` header are refused unless the origin is allowlisted.
  Cookies are never read on `/mcp`.
- **Audit.** Prepares and commits are audited with provider `mcp` and actor `<owner>/<token name>`;
  a prepare is refused when its audit row cannot be written. Token creation and revocation are audited.
- **What MCP cannot do.** It cannot manage sessions or tokens, use sign-in flows, or exceed its
  owner's grants. Limits and failed-authentication throttling are per process.

## Release checks

Verify CSRF, expiry, brute-force throttling, regex restrictions, authorization on every mutation,
provider outage handling, secret redaction, and database backups. Do not claim compliance
certifications. See [Release status](status.md) for unverified paths.

## Related documentation

- [Documentation index](README.md)
- [Architecture](architecture.md)
- [MCP server](mcp.md)
- [Rebalancing](rebalancing.md)
- [Message decoding](message-decoding.md)
