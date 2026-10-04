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

## Release checks

Verify CSRF, expiry, brute-force throttling, regex restrictions, authorization on every mutation,
provider outage handling, secret redaction, and database backups. Do not claim compliance
certifications. See [Release status](status.md) for unverified paths.

## Related documentation

- [Documentation index](README.md)
- [Architecture](architecture.md)
- [Rebalancing](rebalancing.md)
- [Message decoding](message-decoding.md)
