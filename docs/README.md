# Kaflux documentation

Use [implementation status](status.md) to distinguish verified behavior from planned features and
external compatibility that still needs qualification.

## Install and operate

| Guide                                               | Purpose                                                |
| --------------------------------------------------- | ------------------------------------------------------ |
| [Deployment](deployment.md)                         | Docker, Compose, Kubernetes, and operational checks    |
| [Storage](storage.md)                               | SQLite, PostgreSQL, persistence, and deployment limits |
| [Authentication](authentication.md)                 | Login providers, session lifetime, and access control  |
| [Session administration](session-administration.md) | Session inventory, permissions, and audited revocation |
| [Kafka OAuth](kafka-oauth.md)                       | Broker-side OAuth credentials and token refresh        |

## Observe and troubleshoot

| Guide                                     | Purpose                                                     |
| ----------------------------------------- | ----------------------------------------------------------- |
| [Metrics](metrics.md)                     | Native Kafka metrics and monitoring failure isolation       |
| [Message decoding](message-decoding.md)   | Supported formats, schema references, and resource limits   |
| [Rebalancing](rebalancing.md)             | Planning, validation, execution, cancellation, and rollback |
| [Live throttle changes](live-throttle.md) | Asynchronous rate changes and restoration safeguards        |

## Develop and verify

| Guide                                                     | Purpose                                               |
| --------------------------------------------------------- | ----------------------------------------------------- |
| [Architecture](architecture.md)                           | Current system boundaries and provider interfaces     |
| [Security](security.md)                                   | Security controls and operational requirements        |
| [Integration testing](integration-testing.md)             | Local fixtures and reproducible verification          |
| [Implementation status](status.md)                        | Verification evidence, limitations, and release gates |
| [Completion roadmap](completion-roadmap.md)               | Remaining implementation and qualification milestones |
| [Login screenshot evidence](login-screenshot-evidence.md) | Authentication screenshots and capture conditions     |

## Design records

These documents preserve design intent; they are not evidence that every described feature is
implemented.

- [Original product proposal](kaflux-design.md)
- [Initial implementation plan](implementation-plan.md)
- [Embedded storage design](sqlite-design.md)
- [OpenAPI contract](openapi.yaml)
