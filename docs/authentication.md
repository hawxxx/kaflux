# Authentication

User authentication and Kafka connection authentication are separate systems.

Real mode requires configured local bcrypt credentials, OIDC, or LDAP regardless of the storage backend. SQLite persists identity sessions, revocation, OIDC flows and login throttles across restarts without PostgreSQL. See [storage.md](storage.md) for configuration and single-instance limits.

## Users

Local credentials use password hashes, rate-limited login, server-side session expiry, and CSRF. OIDC providers use discovery, issuer/audience verification, authorization-code flow, PKCE, state, and nonce. Authentik, Keycloak, Okta, Entra ID, and Google use configured OIDC providers. Native GitHub OAuth2 login is not implemented. Arbitrary OAuth2 endpoints do not establish identity automatically; a future GitHub adapter must validate its identity contract.

LDAP supports secure bind/search with escaped filters and LDAPS or StartTLS; deny insecure production binds. Verified provider groups map to configured roles on login. Native MFA-claim enforcement, JIT provisioning policies, and trusted-header login are not implemented. Upstream OIDC policies require separate tenant qualification; a future trusted-header provider must enforce trusted proxies. Multiple configured providers may coexist.

Authenticated users can sign out from the sidebar. Successful logout clears the frontend query cache and CSRF token and returns to login; failures remain visible and retryable. Demo access is automatic and has no personal session to sign out of. Server sessions default to eight hours. Set `runtime.sessionLifetime` in `config.yaml` (for example, `sessionLifetime: 30m`), or override it with `KAFLUX_SESSION_LIFETIME=30m`. Values use Go duration syntax and must be between `5m` and `24h`, inclusive; invalid values fail startup. Local, LDAP, and OIDC login share the same absolute session lifetime and cookie expiration. Sessions expire at the boundary with no sliding renewal. Changes apply to newly issued sessions; existing stored sessions retain their original expiration. Demo access remains automatic.

## Kafka

franz-go clients support plaintext only when explicitly configured, TLS, mTLS, PLAIN, SCRAM-256/512, OAuth bearer, and MSK IAM. AWS clients use the default credential chain and optional role assumption, supporting container credentials and workload identity. Kerberos requires a separately verified plugin and is not silently approximated.

## UI

The connection wizard sends configuration to the API and displays sanitized DNS/TLS/SASL/metadata diagnostics. Passwords and private keys are write-only inputs. Config APIs return secret references and authentication type, never secret values.

## Evidence

Local and development authentication must be tested automatically. External providers require real test tenants or realistic protocol fixtures and explicit verification records. See `status.md`; do not mistake configuration examples for completed integrations.
