# Storage

Real deployments use SQLite by default when no database URL is supplied. SQLite needs no separate database server. It stores sessions, revoked sessions, audit records, jobs, worker leases, login throttles and OIDC flows in one durable file. Authentication remains required.

| Setting | Purpose |
| --- | --- |
| `runtime.storageBackend` / `KAFLUX_STORAGE_BACKEND` | `sqlite`, `postgres`, or demo-only `memory` |
| `runtime.sqlitePath` / `KAFLUX_SQLITE_PATH` | SQLite filesystem path; default `/var/lib/kaflux/kaflux.db` |
| `runtime.databaseURLEnv` / `KAFLUX_DATABASE_URL` | PostgreSQL secret reference / connection URL |

Environment variables override YAML. Without an explicit backend, a database URL selects PostgreSQL for compatibility; otherwise demo selects memory and real mode selects SQLite. PostgreSQL requires a URL. Explicit SQLite or memory rejects a PostgreSQL URL. Memory is forbidden outside demo mode. SQLite URI and in-memory names are rejected; use a normal filesystem path.

## Single instance

SQLite uses WAL, bounded busy waits and transactions. One Kaflux process owns the database directory; a second process fails startup. Use a local persistent disk with filesystem locking, not a shared network filesystem. Mount the entire directory, including the database, WAL, shared-memory and lock files. Keep the directory writable by UID/GID 65532 in containers. The image prepares `/var/lib/kaflux` with that ownership; a host bind mount must provide equivalent permissions.

Do not scale SQLite horizontally. Kubernetes uses one replica, a `Recreate` strategy and a persistent volume; the chart rejects autoscaling or multiple replicas. PostgreSQL remains the chart default and supports shared persistence for multiple replicas.

## Backup and migration

For the simplest consistent backup, gracefully stop Kaflux, copy the complete data directory, then restart. Restore the directory while Kaflux is stopped, preserving ownership and permissions. Do not copy only the database file while a process is running: committed data can still reside in WAL. A qualified SQLite online backup utility can produce a consistent live snapshot; test restore separately before relying on it.

SQLite and PostgreSQL use distinct schemas. Changing `storageBackend` creates or uses that backend's database; it does not migrate existing sessions, audit records or jobs. A SQLite-to-PostgreSQL data migration tool is not included. Preserve backups and finish or reconcile jobs before switching. Automatic schema setup runs on startup; retain a backup before upgrading.
