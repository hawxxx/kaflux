# Security review and remediation

Reviewed and remediated on 2026-10-04. Scope: Go HTTP API, React frontend, authentication/session controls, authorization, configured integrations, dependency advisories, and build/CI configuration. This is a source review with regression tests and local browser verification, not a penetration test or a certification of production infrastructure.

## Findings addressed

| Priority | Finding | Evidence and remediation |
| --- | --- | --- |
| P1 | Known vulnerable backend dependencies | The initial Go scan found eight advisories with call traces in identity parsing, PostgreSQL, gRPC, text handling, and telemetry. Upgraded affected direct and transitive dependencies, including go-jose, pgx, gRPC, OpenTelemetry, x/crypto, x/net, x/text, OAuth2, compression, and NTLM support. Go 1.26.8 is now the minimum in `backend/go.mod`, README, CI, and Docker. A subsequent package-level gRPC advisory was also patched with 1.83.2. |
| P2 | Login lacked browser CSRF protection | Login ran before authenticated CSRF checks and accepted JSON bodies labeled as simple browser form types. Regression tests demonstrated successful logins from foreign/null origins and `text/plain` before remediation. Login now requires `application/json`, rejects cross-site Fetch Metadata and mismatched/malformed origins, and continues to accept JSON requests from non-browser clients without Origin metadata. Rejected requests do not establish cookies. |
| P2 | Login throttle had unbounded per-IP growth and discarded active limits | Blocked requests were appended to each IP's timestamp list, and exceeding the client-map threshold reset the entire map. Tests reproduced 100 retained attempts for one IP and loss of an existing limit at capacity. Each IP now retains at most five admitted attempts per minute; expired clients are evicted periodically, and new clients are rejected at the 10,000-client bound without resetting active limits. Malformed JSON is throttled before parsing. Rejections include Retry-After. |
| P2 | Browser responses lacked CSP and clickjacking protections | Added a self-only script policy, frame-ancestors none, X-Frame-Options DENY, no-referrer, disabled unused camera/microphone/geolocation permissions, object-src none, base-uri none, and form-action self. Existing no-store and nosniff remain. Policy permits inline styles required by React/uPlot and the existing Google Fonts stylesheet/font origins. Verified the compiled application under these headers. |
| P3 | Logout inherited topic search state into the login URL | The production browser suite exposed a login URL carrying topic query/default parameters. Added an explicit login route without topic-search validation and clear search state in both sign-out callbacks. The regression starts with a private topic query and requires the clean login URL and login screen. |

Initial called-symbol advisory IDs: GO-2026-6505, GO-2026-4394, GO-2026-6348, GO-2026-4945, GO-2026-5970, GO-2026-6061, GO-2026-4985, and GO-2026-5004. The additional patched gRPC package advisory was GO-2026-6443. A call trace establishes use of affected code, not proof that every advisory was exploitable in Kaflux's deployment.

## Existing controls checked

- Backend authorization filters topic/group resources and gates administrative operations; mutation approval and audit coverage have regression tests.
- Sessions use random identifiers, HttpOnly/Secure/SameSite=Lax cookies in production, configured absolute expiry, backend revocation, and CSRF tokens for authenticated unsafe requests.
- OIDC verifies state, nonce, issuer/audience, and PKCE; pending state is bounded and expires in both memory and persistent storage. LDAP uses escaped filters and requires LDAPS or StartTLS with certificate verification.
- Integration clients constrain concurrency, response sizes, deadlines, and redirects; credentials use environment references and configuration responses are redacted. Private-network service endpoints are an intended administrator-configured feature.
- HTTP server timeouts/header limits and JSON request-body bounds already exist. React message rendering does not use raw HTML/eval sinks.
- A targeted current-source scan of 92 non-test backend/frontend/deployment files found no private-key, AWS access-key, or GitHub-token patterns. This does not cover Git history, ignored configuration, every credential format, or runtime secrets.

## Verification

- Security regression tests were observed failing before remediation and passing afterward.
- Final `go test -race ./...` and `go vet ./...` passed. Environment-gated live Kafka/PostgreSQL/OIDC/LDAP/MSK tests were not provisioned.
- Final `go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 -show verbose ./...` reports zero called-symbol and zero imported-package vulnerabilities. One required-module advisory remains: GO-2026-5932 for unmaintained `golang.org/x/crypto/openpgp`, with no fix available. Kaflux does not import that package; x/crypto is needed for maintained packages including bcrypt.
- Final frontend npm audit reports zero vulnerabilities. Frontend unit tests pass (40 tests), TypeScript and the final production build pass, and all 25 Playwright workflows pass against the final compiled bundle under the backend's CSP. Details and screenshots are recorded in `responsiveness-review.md`.
- CI now runs the pinned Go vulnerability scanner, npm audit, and responsive/session/topic-URL browser regressions alongside existing checks.

## Deployment implications and limits

API login clients must send Content-Type application/json. Reverse proxies must preserve the public Host for browser Origin validation; arbitrary forwarded headers are not trusted. Production HTTPS termination/HSTS and per-client rate limiting at the trusted edge still require deployment verification, especially because the application limiter is per instance and uses the direct peer address. Container-image vulnerabilities, real identity providers, broker credentials, and public network exposure were not tested locally. Docker's Go builder version was updated; a Docker image build was not performed.

Guidance: [OWASP CSRF prevention](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html), [OWASP HTTP security headers](https://cheatsheetseries.owasp.org/cheatsheets/HTTP_Headers_Cheat_Sheet.html), [Go vulnerability database](https://pkg.go.dev/vuln/), and [OpenPGP advisory](https://pkg.go.dev/vuln/GO-2026-5932).

## Follow-up review: security and observability

A second source review with local verification was carried out on 2026-10-04.

| Area | Finding | Change |
| --- | --- | --- |
| Security | Local login skipped bcrypt when the username did not match, so response time revealed the administrator username. | bcrypt now always runs and the username comparison is constant-time. |
| Security | HSTS and cross-origin isolation headers were missing. | Added `Strict-Transport-Security: max-age=31536000` (ignored by browsers over plain HTTP), `Cross-Origin-Opener-Policy: same-origin` and `Cross-Origin-Resource-Policy: same-origin`. |
| Security | Audit records stored a client-supplied `X-Request-ID` of any length. | Audit records use the trace ID when one exists. Otherwise they accept a client ID only if it is at most 128 characters from `[A-Za-z0-9._:-]`. |
| Observability | The `/metrics` output was out of spec: lines for each metric were not grouped together, some families had no `# TYPE`, and the order changed between scrapes. | Each family is now one sorted, contiguous group with HELP/TYPE lines. `kaflux_http_errors_total` was removed because it repeated the `status` label; use `kaflux_http_requests_total{status=~"5.."}` instead. |
| Observability | Handler panics bypassed the JSON log and aborted the connection. | Panics are recovered, logged with the stack trace and trace ID, counted as 500 and returned as `internal_error`. |
| Observability | About 28 handlers returned 5xx responses without recording the underlying error. | `failCause` logs the cause with the trace ID. Clients still receive only the sanitized message. |
| Observability | Failed logins, rate-limited logins and LDAP outages were not logged. LDAP outages looked like wrong passwords. | Each is now logged as a warning. Usernames are cut to 128 characters; passwords are never logged. Integration transport errors are also logged. |

Fonts are now bundled with the frontend: `@fontsource-variable/dm-sans` and `@fontsource/ibm-plex-mono` are pinned exactly. The CSP allows styles and fonts only from Kaflux's own origin (`'self'`), so browsers no longer send operators' IP addresses to Google, and fonts load without internet access. A regression test rejects any third-party font origin in the CSP.

Verification: `go vet ./...` and `go test -race ./...` passed for all 15 packages, including new tests for metric grouping, panic recovery, `failCause` and request-ID bounds. The patched demo server returned the new headers, the grouped metrics output, and a JSON `login failed` warning for an invalid login.
