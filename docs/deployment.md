# Deployment

## Local development

Use the root Makefile for unit tests, frontend builds, and development. Copy `config.example.yaml` to a private `config.yaml`; resolve credentials from environment variables. Never commit that private configuration. Development simulation must be explicitly enabled. For a local real deployment set `KAFLUX_SQLITE_PATH=./data/kaflux.db` and configure authentication and cluster endpoints before starting the binary.

## Docker and Compose

The multi-stage Dockerfile builds the UI and Go binary into a non-root image. Configure a bcrypt password hash (or an OIDC/LDAP provider). Containers expose health, readiness, and Prometheus metrics.

For a single real instance without PostgreSQL:

```sh
cp config.example.yaml config.yaml
# Edit private config.yaml: supply reachable Kafka seeds and credentials.
# Set KAFLUX_ADMIN_PASSWORD_HASH to your bcrypt password hash.
export KAFLUX_ADMIN_USER=admin
docker compose -f deploy/docker-compose.sqlite.yaml up --build -d
```

The SQLite Compose example requires a password hash and mounts a named volume at `/var/lib/kaflux` while keeping the root filesystem read-only. Mount the directory rather than just `kaflux.db`: SQLite also writes WAL and lock files. No demo flag is enabled and no PostgreSQL process starts. See [storage.md](storage.md) for backup, permissions and migration limits.

The root `docker-compose.yaml` retains the PostgreSQL setup and optional Kafka/Prometheus integration profile. Set `KAFLUX_DB_PASSWORD` and `KAFLUX_ADMIN_PASSWORD_HASH`, then run `docker compose up --build -d`. It explicitly selects PostgreSQL even when the example YAML selects SQLite.

## Kubernetes

The Helm chart supplies Deployment, Service, optional Ingress, ConfigMap, Secret references, ServiceAccount annotations, HPA, PDB, NetworkPolicy, affinity, and topology spread options. PostgreSQL is the default. Use an external PostgreSQL service, TLS termination, resource requests, and workload identity. Mount secret references rather than putting credentials in Helm values.

For SQLite use `--set storage.backend=sqlite --set replicaCount=1 --set autoscaling.enabled=false`. The chart creates a ReadWriteOnce PVC, mounts `/var/lib/kaflux`, sets `fsGroup: 65532`, and uses `Recreate` so rollout pods never overlap. Set `storage.sqlite.existingClaim` to reuse a provisioned claim, or configure `storage.sqlite.size` and `storage.sqlite.storageClass`. The runtime Secret still needs admin credentials but does not need `database-url` in SQLite mode. The volume must support local filesystem locks; SQLite is a single-instance deployment.

Multiple API replicas require PostgreSQL sessions/audit/jobs and coordinated worker ownership. Run database migrations before rolling out an incompatible binary. Graceful shutdown stops claiming work and releases leases after reconciliation.

## Production release gate

Validate image startup, Compose integration, Helm lint/rendering, Kafka reassignment recovery, authentication providers, authorization, and load budgets. Test backup/restore and monitor application errors, lease contention, queue age, Kafka RPC latency, and stale telemetry. `status.md` records actual evidence and gaps; examples alone do not establish production readiness.
