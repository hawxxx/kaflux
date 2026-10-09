# Authentication

User authentication and Kafka connection authentication are separate systems.

Real mode requires configured local bcrypt credentials, OIDC, or LDAP regardless of the storage
backend. SQLite persists identity sessions, revocation, OIDC flows and login throttles across
restarts without PostgreSQL. See [storage.md](storage.md) for configuration and single-instance
limits.

## User authentication

### Local and OIDC providers

Local credentials use password hashes, rate-limited login, server-side session expiry, and CSRF.
OIDC providers use discovery, issuer/audience verification, authorization-code flow, PKCE, state,
and nonce. Authentik, Keycloak, Okta, Entra ID, and Google use configured OIDC providers. Native
GitHub OAuth2 login is not implemented. Arbitrary OAuth2 endpoints do not establish identity
automatically; a future GitHub adapter must validate its identity contract.

### LDAP and provider boundaries

LDAP supports secure bind/search with escaped filters and LDAPS or StartTLS; insecure binds are
rejected. Set `caFile` when the directory certificate is issued by a private CA. Verified provider
groups map to configured roles on login; LDAP group DNs match case-insensitively and ignore spacing
after commas.

Native MFA-claim enforcement, JIT provisioning policies, and trusted-header login are not
implemented. Upstream OIDC policies require separate tenant qualification; a future trusted-header
provider must enforce trusted proxies. Multiple configured providers may coexist. The sign-in page
shows the password form only when a local account or LDAP directory is configured, and one button
per OIDC provider.

Startup fails on invalid identity configuration: unknown grant actions, duplicate provider IDs, an
OIDC `redirectURL` whose path is not `/api/v1/auth/oidc/<id>/callback`, insecure LDAP URLs, a
`userFilter` without exactly one `{username}`, or an unset bind password reference. A role that a
provider assigns but no grant mentions is logged as a warning, because those users can sign in and
see nothing.

### Roles and grants

Providers assign roles; `grants` in `config.yaml` give roles actions. The local admin account always
has the `administrator` role. With no grants configured, only `administrator` has access (to
everything). Roles are captured at sign-in: group changes apply at the next sign-in, and
administrators can revoke sessions to force it.

Each grant has `role`, `action`, optional `cluster` (cluster ID, default `*`) and optional `pattern`
(default `*`). `pattern` is a glob, or a regular expression with `regex: true`, matched against the
topic, consumer group, subject or connector name.

| Action | Allows |
| --- | --- |
| `read` | Clusters, brokers, topics, groups, configuration, metrics, ACL list, reassignment plans |
| `consume` | Reading message payloads |
| `produce` | Producing messages |
| `create` | Creating and copying topics |
| `delete` | Deleting, clearing and recreating topics |
| `alter-config` | Topic configuration and partition count |
| `reset-offsets` | Consumer group offset resets |
| `rename` | Cluster display name |
| `plan` | Creating, validating and deleting reassignment plans |
| `execute` | Running, throttling and cancelling reassignments |
| `rollback` | Requesting a reassignment rollback |
| `manage-acls` | Creating and deleting Kafka ACLs |
| `schema-read`, `schema-update` | Schema Registry |
| `connector-read`, `connector-update` | Kafka Connect |
| `audit` | The audit log (global grant: `cluster: "*"`) |
| `manage-sessions` | Session administration (global grant; `pattern` matches the user ID) |
| `*` | Everything |

A read-only role:

```yaml
grants:
  - {role: viewer, action: read}
  - {role: viewer, action: schema-read}
  - {role: viewer, action: connector-read}
  # Optional: let viewers read payloads of non-sensitive topics only.
  - {role: viewer, action: consume, pattern: "public.*"}
```

The backend authorizes every request. The session also reports each cluster's permitted actions so
the UI disables controls the user cannot use.

### Authentik

OIDC: create an OAuth2/OpenID provider (confidential client, signing key set) and an application
with slug `kaflux`. Set the redirect URI to `https://<kaflux>/api/v1/auth/oidc/authentik/callback`.
Authentik's default `profile` scope includes the `groups` claim.

```yaml
oidc:
  - id: authentik
    name: Authentik
    issuer: https://authentik.example.com/application/o/kaflux/   # keep the trailing slash
    clientId: <client ID>
    clientSecretEnv: KAFLUX_OIDC_SECRET
    redirectURL: https://kaflux.example.com/api/v1/auth/oidc/authentik/callback
    groupRoles:
      kafka-admins: [administrator]
      kafka-operators: [operator]
      kafka-viewers: [viewer]
```

LDAP: deploy an LDAP outpost, bind as a service account that may search the directory, and use
LDAPS (outpost port 6636) with `caFile` if its certificate is self-signed.

```yaml
ldap:
  - id: authentik
    url: ldaps://authentik-ldap.example.com:6636
    caFile: /etc/kaflux/authentik-ldap-ca.pem
    bindDN: cn=kaflux-ldap,ou=users,dc=ldap,dc=goauthentik,dc=io
    bindPasswordEnv: KAFLUX_LDAP_BIND_PASSWORD
    baseDN: dc=ldap,dc=goauthentik,dc=io
    userFilter: (cn={username})
    groupAttribute: memberOf
    groupRoles:
      cn=kafka-viewers,ou=groups,dc=ldap,dc=goauthentik,dc=io: [viewer]
```

Keycloak needs a "Group Membership" mapper (full group path off) on the client's dedicated scope.
Okta needs `scopes: [groups]` and a groups claim on the authorization server. Entra ID emits group
object IDs; map those IDs. Pass secrets as environment variables (`secrets.extraEnv` in Helm;
`KAFLUX_OIDC_SECRET` and `KAFLUX_LDAP_BIND_PASSWORD` are forwarded by `docker-compose.yaml`).

### Troubleshooting sign-in

Failed OIDC sign-ins return to `/login` with a message; the server log names the cause.

- `sso_rejected` with "identity has no assigned roles": none of the user's groups is in
  `groupRoles` and no `defaultRoles` are set. Check the claim name (`groupClaim`) and group spelling.
- `sso_state`: the login took longer than five minutes, or cookies were blocked or the browser
  changed. Kaflux must be served over HTTPS for its cookies outside demo mode.
- Signed in but everything is denied: the role has no grants (startup logs a warning).
- LDAP `identity lookup failed`: the filter matched zero or several entries, or the service account
  cannot search `baseDN`.

### Logout and session lifetime

Authenticated users can sign out from the sidebar. Successful logout clears the frontend query cache
and CSRF token and returns to login; failures remain visible and retryable. Demo access is automatic
and has no personal session to sign out of.

Server sessions default to eight hours. Set `runtime.sessionLifetime` in `config.yaml` (for example,
`sessionLifetime: 30m`), or override it with `KAFLUX_SESSION_LIFETIME=30m`.

- Values use Go duration syntax and must be between `5m` and `24h`, inclusive; invalid values fail
  startup.
- Local, LDAP, and OIDC login share the same absolute session lifetime and cookie expiration.
- Sessions expire at the boundary with no sliding renewal.
- Changes apply to newly issued sessions; existing stored sessions retain their original expiration.
- Demo access remains automatic.

## Kafka connection authentication

franz-go clients support plaintext only when explicitly configured, TLS, mTLS, PLAIN, SCRAM-256/512,
OAuth bearer, and MSK IAM. AWS clients use the default credential chain and optional role
assumption, supporting container credentials and workload identity. Kerberos requires a separately
verified plugin and is not silently approximated.

## Connection wizard and secret handling

The connection wizard sends configuration to the API and displays sanitized DNS/TLS/SASL/metadata
diagnostics. Passwords and private keys are write-only inputs. Config APIs return secret references
and authentication type, never secret values.

## Evidence

Local and development authentication must be tested automatically. External providers require real
test tenants or realistic protocol fixtures and explicit verification records. See
[implementation status](status.md); do not mistake configuration examples for completed
integrations.

## Related documentation

- [Kafka OAuth client credentials](kafka-oauth.md).
- [Session administration](session-administration.md).
- [Login screenshot evidence](login-screenshot-evidence.md).
- [Documentation index](README.md).
