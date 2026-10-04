# Kafka OAuth client credentials

Kafka connection authentication is independent of Kaflux user login. `sasl: oauthbearer` supports either a static bearer token environment reference (`oauthTokenEnv`) or OAuth2 client credentials. Both modes require Kafka TLS; client credentials also require a fixed HTTPS token endpoint.

```yaml
clusters:
  - id: secured
    name: Secured Kafka
    seeds: [broker.example:9093]
    tls: true
    sasl: oauthbearer
    oauthTokenEndpoint: https://identity.example/oauth/token
    oauthClientId: kaflux-operations
    oauthClientSecretEnv: KAFLUX_KAFKA_OAUTH_SECRET
    oauthScopes: [kafka.read, kafka.admin]
    # oauthCAFile: /run/secrets/token-endpoint-ca.pem
```

Supply the secret through the named server environment variable or deployment Secret. Do not configure `oauthTokenEnv` alongside client credentials. `oauthCAFile` optionally adds a CA for the token endpoint; broker trust remains configured independently through `caFile`. Certificate verification remains enabled, with TLS 1.2 or newer.

Kaflux posts `grant_type=client_credentials` with HTTP Basic client authentication, following [RFC 6749](https://www.rfc-editor.org/rfc/rfc6749#section-4.4). Providers requiring client-secret-post, private-key JWT or mTLS client authentication are not supported by this adapter. Redirects, URL credentials, query strings and fragments are rejected. Successful responses must provide a Bearer token and `expires_in` between 1 second and 24 hours. Responses are limited to 64 KiB and tokens to 16 KiB of printable ASCII.

Each Kafka connection provider maintains an in-process token cache. Concurrent acquisition requests coalesce behind one cancellable gate and a five-second deadline. Refresh begins on the next Kafka authentication callback within the final 10% of token lifetime, capped at 30 seconds before expiry. This is demand-driven refresh for connection authentication/reauthentication, not a background process that forces existing broker sessions to reconnect.

Endpoint failures back off exponentially with a base delay from one to 32 seconds and up to 20% jitter. Refresh timing is also jittered within the expiry margin. A previously obtained token may be reused only until its actual expiration; an expired token never succeeds through the cache. Token endpoint response bodies, credentials and token values are excluded from errors and logs. The client secret environment reference is read again during refresh, allowing secret rotation.

TLS protocol fixtures verify client authentication, scopes, request coalescing, expiry refresh, cancellation, backoff, invalid/oversized responses and redirect rejection. They do not certify any live identity provider or Kafka OAuth deployment. Runtime YAML and JSON cluster files accept the same fields, and container/Helm configuration uses the existing secret environment mechanism.
