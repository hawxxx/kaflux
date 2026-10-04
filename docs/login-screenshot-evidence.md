# Login screenshot evidence

## Captured artifacts

| Viewport | Artifact                                                               | Dimensions  |
| -------- | ---------------------------------------------------------------------- | ----------- |
| Desktop  | [login-auth-desktop.png](../frontend/artifacts/login-auth-desktop.png) | 1440 × 1000 |
| Mobile   | [login-auth-mobile.png](../frontend/artifacts/login-auth-mobile.png)   | 390 × 844   |

## Capture environment

Captured with `agent-browser` against the actual frontend production build served by a temporary,
non-demo Kaflux API at `http://127.0.0.1:8081/`. The API used local PostgreSQL and Kafka fixtures,
with a temporary bcrypt local account. The unauthenticated session endpoint returned HTTP 401, and
the rendered page contained the username/password login form. No API response interception or static
mockup was used. Input fields were left empty; screenshots contain no credentials.

## Inspection and evidence boundary

Both images were visually inspected after the page settled. Browser commands and screenshot capture
used the same native execution environment; context-mode and native commands may otherwise connect
to different browser sessions despite sharing a session name.

These screenshots demonstrate the local login UI, not external LDAP/OIDC provider interoperability.

## Related documentation

- [Authentication](authentication.md).
- [Implementation status](status.md).
- [Documentation index](README.md).
