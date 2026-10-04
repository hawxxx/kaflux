# Session administration

## Contract and safeguards

Session inventory and administrator revocation are organization-wide operations. They require an
explicit global `manage-sessions` grant; a Kafka cluster or topic grant must not confer session
administration rights.

Inventory must be paginated and omit bearer session IDs, CSRF tokens, credentials and message data.
Public session handles identify a revocation target without granting authentication. Show the user,
authentication provider, roles, absolute expiration and whether the entry is the administrator's
current session.

Revocation requires an explicit confirmation and the ordinary authenticated CSRF check. Persist the
audit record and session deletion atomically when PostgreSQL is used. Return a visible, retryable
error when persistence fails rather than reporting a successful revocation. Revocation of the
current session must expire its cookie and clear private frontend state before returning to login.

Simulator access is automatic and is not a revocable personal session. Explain this distinction in
the UI and reject simulator revocation requests.

## API contract

| Operation        | Request                                                                  |
| ---------------- | ------------------------------------------------------------------------ |
| List sessions    | `GET /api/v1/admin/sessions?limit=25&offset=0`                           |
| Revoke a session | `POST /api/v1/admin/sessions/{handle}/revoke` with `{ "confirm": true }` |

The maximum limit is 100 and the maximum offset is 10,000. Inventory responses include
`meta.hasMore`; they do not expose an unbounded total.

The session endpoint supplies `canManageSessions` for capability-based navigation. A scoped
revocation grant can restrict target users; complete inventory requires permission on `*`.

## Acceptance checks

- Unauthorized callers cannot list sessions or revoke one.
- Cluster-specific grants cannot administer global sessions.
- Inventory responses and audit events contain no bearer IDs or CSRF tokens.
- Pagination, expiry filtering and public handles behave consistently in memory and PostgreSQL.
- Revocation invalidates authentication on subsequent requests, including on another API instance
  sharing the database.
- Missing confirmation, invalid handles and missing CSRF are rejected.
- Persistence failures leave the session valid and produce no false success.
- Browser flows cover confirmation, errors, retry, self-revocation and narrow-screen usability.

Implementation and verification evidence is recorded in [status.md](status.md); this acceptance
contract alone is not release evidence.

## Related documentation

- [Authentication](authentication.md).
- [Storage](storage.md).
- [Documentation index](README.md).
