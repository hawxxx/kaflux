# Kaflux

Kaflux is a Kafka operations console built with Go and React. Explore clusters and messages, monitor activity, and manage rebalancing from one interface.

## Features

- **Cluster management:** brokers, topics, partitions, consumer groups, and access controls.
- **Message exploration:** filtering, JSON previews, and Avro/Protobuf decoding through Schema Registry.
- **Monitoring:** metrics, consumer lag, partition balance, and per-topic broker distribution.
- **Rebalancing:** plan review, approval, audited execution, and live throttle controls.
- **Integrations:** Kafka Connect, Schema Registry, OIDC, and LDAP.

## Installation

### Docker Compose

Requires Git, Docker with Compose, and access to a Kafka cluster. Go and Node.js are not needed for container installation.

**1. Clone the repository and copy the configuration.**

```sh
git clone https://github.com/hawxxx/kaflux.git
cd kaflux
cp config.example.yaml config.yaml
```

**2. Configure your cluster and sign-in credentials.**

Edit `config.yaml` with Kafka broker addresses reachable from the container and the appropriate TLS/SASL settings. Set referenced secret environment variables, or remove unused references such as `passwordEnv` for a cluster without SASL.

Set your administrator username and a bcrypt hash of your chosen login password. Replace the placeholder below with the hash; use single quotes to preserve its `$` characters.

```sh
export KAFLUX_ADMIN_USER=admin
export KAFLUX_ADMIN_PASSWORD_HASH='YOUR_BCRYPT_PASSWORD_HASH'
```

**3. Start Kaflux.**

```sh
docker compose -f deploy/docker-compose.sqlite.yaml up --build -d
```

Open **http://localhost:8080** and sign in with your administrator username and original password. This setup uses SQLite with a persistent Docker volume.

To stop it:

```sh
docker compose -f deploy/docker-compose.sqlite.yaml down
```

### Try the demo

After cloning the repository, create a minimal configuration and start the simulator with PostgreSQL. If you already have a `config.yaml`, use a separate checkout for the demo.

```sh
printf 'runtime:\n  demo: true\n' > config.yaml
export KAFLUX_DB_PASSWORD=local-demo-password
KAFLUX_DEMO=true docker compose up --build -d
```

Open **http://localhost:8080**. Demo access is automatic; cluster data and operations are simulated. Stop it with `docker compose down` before starting another setup on the same port.

### PostgreSQL and Kubernetes

For PostgreSQL, use the root `docker-compose.yaml` and set `KAFLUX_DB_PASSWORD` in addition to your administrator credentials. Start it with `docker compose up --build -d`. If the database password contains URI-reserved characters, provide `KAFLUX_DATABASE_URL` with a URL-encoded password.

For Kubernetes, use the [Helm chart](deploy/helm/kaflux). See the [deployment guide](docs/deployment.md) for storage, secrets, ingress, and replica configuration.

## Configuration

[config.example.yaml](config.example.yaml) documents cluster connections, permissions, and identity providers. Environment variables override YAML settings. Keep credentials in environment variables or Kubernetes Secrets, and keep `config.yaml` private.

| Setting | Purpose |
| --- | --- |
| `KAFLUX_ADMIN_USER` | Local administrator username |
| `KAFLUX_ADMIN_PASSWORD_HASH` | Bcrypt hash for local sign-in |
| `KAFLUX_STORAGE_BACKEND` | `sqlite` or `postgres`; supplied by the Compose files |
| `KAFLUX_DATABASE_URL` | PostgreSQL connection string |
| `KAFLUX_PROMETHEUS_URL` | Prometheus endpoint for metrics |
| `KAFLUX_DEMO` | Enables simulated data and operations |

See [authentication](docs/authentication.md), [storage](docs/storage.md), and [metrics](docs/metrics.md) for details. Use TLS when exposing Kaflux beyond localhost.

## Development

Requires Go 1.26.8 or newer, Node.js 22, npm, and Make.

Start the simulator backend:

```sh
make dev
```

In another terminal, start the frontend and open the URL printed by Vite:

```sh
cd frontend
npm ci
npm run dev
```

| Command | Purpose |
| --- | --- |
| `make test` | Backend race tests and vet, frontend tests and build |
| `make build` | Build the backend and frontend |
| `make docker` | Build the Docker image |
| `make helm` | Validate and render the Helm chart locally |

For local Kafka fixtures and reassignment tests, see [integration testing](docs/integration-testing.md).

## Documentation

| Guide | Topics |
| --- | --- |
| [Deployment](docs/deployment.md) | Docker, Kubernetes, and storage setup |
| [Authentication](docs/authentication.md) | Local sign-in, OIDC, LDAP, and Kafka authentication |
| [Security](docs/security.md) | Authorization, credentials, and audit controls |
| [Rebalancing](docs/rebalancing.md) | Planning, approval, execution, and recovery |
| [Live throttle controls](docs/live-throttle.md) | Rate changes during reassignment |
| [Message decoding](docs/message-decoding.md) | Avro and Protobuf support |
| [API reference](docs/openapi.yaml) | HTTP endpoints under `/api/v1` |
| [Architecture](docs/architecture.md) | Backend and frontend structure |

## License

[MIT](LICENSE).
