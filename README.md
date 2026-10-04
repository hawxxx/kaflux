# Kaflux

Kaflux is a self-hosted Kafka operations console built with Go and React. It combines cluster inventory, message exploration, monitoring, balance planning, and audited reassignment workflows behind a backend API. Simulation is an explicitly enabled backend provider and is labeled in the UI.

## Develop

Prerequisites: Go 1.25 (or Go toolchain auto-download), Node.js 22, npm, and Docker for integration checks.

Run `make test` for backend race tests/vet and frontend tests/build. Run `make dev` to start the explicit simulator backend on localhost:8080; in another terminal run `cd frontend && npm ci && npm run dev`. Vite proxies `/api` to the backend. The development simulator's sign-in behavior is described by the login screen; production requires a bcrypt password hash.

## Container

Build with `make docker`. The image serves the compiled frontend and API on port 8080 as a non-root user. It supports a read-only filesystem.

Compose requires `KAFLUX_DB_PASSWORD` and a private `config.yaml` copied from [config.example.yaml](config.example.yaml). Set `KAFLUX_ADMIN_USER` and `KAFLUX_ADMIN_PASSWORD_HASH` for real mode. YAML accepts secret environment references; environment variables override configured settings. Compose generates `KAFLUX_DATABASE_URL` by default; for a password containing URI-reserved characters, set an explicit database URL with a URL-encoded password. Protect private files and use a TLS ingress when exposing the service beyond localhost.

For local simulation, copy the example to `config.yaml`, remove optional secret references that you have not set, set `KAFLUX_DB_PASSWORD` to a local password, then run `KAFLUX_DEMO=true docker compose up --build`. Open http://localhost:8080. Demo mode must be explicitly enabled; its data and operations are simulated.

For a real local Kafka integration, use the example's cluster entry with `seeds: ["kafka:9092"]`, `tls: false`, and `allowPlaintext: true`. Run `docker compose --profile integration up --build` with configured admin credentials. The profile includes single-node KRaft Kafka and Prometheus. It is a local integration fixture with plaintext Kafka, not a hardened broker deployment. Prometheus scrapes application metrics; broker monitoring requires Kafka/JMX exporters configured separately.

The optional `deploy/docker-compose.integration.yaml` override adds two brokers for reassignment tests. Start the fixture with `docker compose -f docker-compose.yaml -f deploy/docker-compose.integration.yaml --profile integration up -d --wait postgres kafka kafka2 kafka3`. Host test clients use `KAFLUX_TEST_KAFKA_SEED=127.0.0.1:19092` and `KAFLUX_TEST_DATABASE_URL=postgres://kaflux:YOUR_LOCAL_PASSWORD@127.0.0.1:15432/kaflux?sslmode=disable`; use a URL-encoded database password. Run `cd backend && go test -race -p 1 ./...`; serialize test packages because they change broker throttle settings on the shared fixture. Kafka security and controller redundancy are intentionally simplified in this fixture.

Kafka fixture storage belongs to each container. If the single controller is recreated, recreate all three Kafka containers together with `docker compose -f docker-compose.yaml -f deploy/docker-compose.integration.yaml --profile integration up -d --force-recreate --wait kafka kafka2 kafka3` to avoid stale broker metadata from the prior controller. This resets local Kafka test data and preserves the PostgreSQL volume.

## Kubernetes

The [Helm chart](deploy/helm/kaflux) provides Deployment, Service, optional ingress/TLS, HPA, PDB, topology spread/affinity, secret references, probes, and a NetworkPolicy. Supply an existing Secret with `database-url`, `admin-user`, and `admin-password-hash`. The chart never generates credentials. Connection configuration is a ConfigMap; cluster passwords resolve through environment variables added with `secrets.extraEnv`. Use `extraVolumes` and read-only `extraVolumeMounts` for CA certificates or Kafka mTLS Secrets; configure cluster `caFile`, `certFile`, and `keyFile` paths accordingly.

Configure OIDC providers (including authentik) and LDAP under `configuration.oidc` and `configuration.ldap`; map verified groups to roles and configure least-privilege `configuration.grants`. Refer to the commented examples in the YAML configuration. Provider secrets resolve through `secrets.extraEnv` Secret references. OIDC uses discovery, signature/issuer/audience validation, authorization-code PKCE, state and nonce; LDAP requires LDAPS or StartTLS.

Kafka SASL OAuth bearer uses a token environment reference in cluster `oauthTokenEnv` with TLS enabled. Kaflux does not acquire or automatically refresh client-credentials tokens; manage their lifetime externally.

Use one replica until restart and multi-worker recovery gates pass. Additional replicas require shared PostgreSQL persistence for sessions, OIDC login flows, audit, jobs, and worker coordination. Pending OIDC state is bounded, expires after five minutes, and is consumed atomically from PostgreSQL. Configure workload identity via `serviceAccount.annotations` (for example the EKS role annotation). IAM configuration alone does not prove MSK interoperability. The default egress rule permits outbound traffic; replace it with database, broker, metrics, identity-provider, and DNS rules for your network. `make helm` validates and renders locally; it does not install resources.

## API and operations

The browser accesses `/api/v1`; Kafka and datasource credentials remain on the backend. The [OpenAPI document](docs/openapi.yaml) describes the HTTP contract. Cookie-authenticated changes require CSRF protection and backend role checks. Plans require review and approval before execution, and successful execution must be established from broker state.

Use `/health` for process liveness and `/ready` for database readiness. Broker and metrics connectivity are reported through their cluster API responses and UI status; `/ready` does not certify those dependencies. PostgreSQL schema setup runs at startup; review migration and backup policies before upgrades. See [security](docs/security.md), [authentication](docs/authentication.md), [metrics](docs/metrics.md), and [rebalancing](docs/rebalancing.md) for requirements and verification boundaries.

Stop every consumer in a group before previewing or applying an offset reset, and keep them stopped until the operation finishes. Kaflux checks that the group is inactive and requires a matching fresh preview, but Kafka does not provide an atomic lock against a consumer starting between the inactivity check and offset commit.

Schema Registry and Kafka Connect use configured backend integrations with `id`, `clusterID`, `kind` (`schemas` or `connect`), `url`, and optional `username`, `passwordEnv` and `tokenEnv`. Set the top-level `integrations` array in runtime YAML, or `configuration.integrations` in Helm values. Supply credentials through secret environment variables. Resource grants use `schema-read`/`schema-update` and `connector-read`/`connector-update`; configured URLs stay on the backend. See the status document for current external-service evidence.

Running reassignments support audited, asynchronous throttle changes with typed job confirmation. The UI distinguishes the requested rate from the last verified rate; zero is not a pause operation. See [live throttle controls](docs/live-throttle.md) for ownership, recovery and safety guarantees, and [implementation status](docs/status.md) for remaining production qualification.

The message explorer supports opt-in Confluent-wire Avro and Protobuf value decoding through configured registries, with subject authorization, bounded previews, paged JSON trees and per-record errors. See [message decoding](docs/message-decoding.md) for supported schema types and limits.

Licensed under [MIT](LICENSE).
