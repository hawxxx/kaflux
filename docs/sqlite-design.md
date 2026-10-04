# Embedded production storage

## Goal

Allow a single Kaflux instance to operate real Kafka clusters without provisioning a separate
database service. Keep durable operational state and preserve the PostgreSQL deployment option.

## Modes

- SQLite: embedded, durable state in a local database directory mounted into the container. Use WAL,
  bounded lock waits, restrictive permissions, transactional mutations and an exclusive Kaflux
  process lock. One instance owns the directory.
- PostgreSQL: existing shared persistence and worker fencing for multiple API replicas.
- Memory: explicitly ephemeral demo/development behavior. It is not a production durability
  substitute.

Existing PostgreSQL configuration remains valid. A real deployment without a PostgreSQL URL should
select SQLite, while simulation retains an in-memory default. Configuration must reject conflicting
backend settings and still require authentication for real Kafka operation.

## Persistence and failure semantics

SQLite must persist sessions, audit history, rebalance jobs and backups, lease/revision fencing,
pending cancellation and throttle requests, original throttle ownership, and one-use OIDC flow
state. Session revocation and audit writes must be atomic. A storage failure must return an error
rather than silently switching to memory.

The container remains non-root. A named Docker volume or persistent Kubernetes volume mounts the
entire SQLite directory, including WAL and lock files. Read-only root filesystems remain supported
when the data volume is writable. SQLite deployments cannot use multiple replicas or HPA; PostgreSQL
retains that deployment path.

## Verification

Verify storage parity, restart/reopen persistence, transaction rollback on failure, one-use
authentication flow replay protection, revision fencing and second-process rejection. Run existing
Kafka/PostgreSQL regressions as well. Build the final container and exercise real-mode authenticated
startup with SQLite and a writable volume, restart it, and confirm durable state. Record exact
evidence in status documentation.

SQLite support does not establish backup restoration, power-loss recovery or production load
qualification without those tests. PostgreSQL-to-SQLite state migration is outside this change.
