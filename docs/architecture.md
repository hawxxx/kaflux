# Architecture

Kaflux uses Go, franz-go, PostgreSQL, and a React/TypeScript/Vite client. This supersedes the
earlier Java proposal. The operator's API is `/api/v1`; the browser never connects to Kafka,
Prometheus, LDAP, or AWS directly.

## Boundaries

| Module                   | Responsibility                                                     |
| ------------------------ | ------------------------------------------------------------------ |
| `backend/internal/kafka` | Broker clients and a provider interface                            |
| `model`                  | Transport models                                                   |
| `balance`                | Deterministic pure planning                                        |
| `store`                  | Audit and job persistence                                          |
| `auth`                   | Sessions and authorization                                         |
| `metrics`                | Datasource gateways                                                |
| `api`                    | Service composition                                                |
| `frontend/src`           | TanStack Query, Router, Table, and Virtual with server-side paging |

Each configured cluster has its own bounded client and cached metadata. Metadata refresh uses a
timeout; failures retain timestamped observations. Broker RPCs and metrics queries use separate
concurrency budgets. Missing optional metrics never prevent topic navigation. Unknown data is
represented as nullable values, never invented zeros.

## Runtime modes

Production requires durable storage and configured authentication.

- Embedded SQLite supports one instance without a separate database service.
- PostgreSQL supports shared state for multiple replicas.
- Development simulation is explicitly enabled and displayed in the UI and can use memory storage.

The simulator is a backend provider implementing the same contracts, not frontend fallback data.
Kafka-compatible providers can be added behind the interface.

## Operations

Reassignments are durable jobs, never long-running HTTP requests. Approval is distinct from plan
generation. PostgreSQL leases coordinate workers; broker state is authoritative after ambiguous
submission or restart. API servers may run concurrently only with shared PostgreSQL persistence.

## Scale

Bound messages, request bodies, cached entries, result pages, query series, stream subscribers, and
parallel work. Benchmark deterministic 15/6,000/30,000 and 100/10,000/100,000 metadata scenarios.
Measure rather than assert sub-200-ms latency. SSE publishes bounded snapshots and disconnects slow
consumers.

## Release status

Feature support and verification are documented in [Release status](status.md). A configured adapter
is not proof of production interoperability; real Kafka and identity-provider tests are release
gates.

## Related documentation

- [Documentation index](README.md)
- [Native metrics](metrics.md)
- [Rebalancing](rebalancing.md)
- [Security](security.md)
