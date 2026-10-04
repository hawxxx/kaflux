# Local integration testing

Run these commands from the repository root. Requires Docker Compose and Go 1.25. The fixture uses
plaintext Kafka for local testing.

## Prepare configuration

1. Copy `config.example.yaml` to private `config.yaml`.
2. Set `KAFLUX_DB_PASSWORD`.
3. Configure administrator credentials if starting Kaflux.
4. Remove unused secret references or supply their environment variables.

The example cluster uses `seeds: ["kafka:9092"]`, `tls: false`, and `allowPlaintext: true`.

## Single broker

```sh
docker compose --profile integration up --build
```

This starts Kaflux, PostgreSQL, single-node KRaft Kafka, and Prometheus. Prometheus scrapes
application metrics; broker metrics require Kafka/JMX exporters configured separately.

## Three brokers

The integration override adds two brokers for reassignment tests:

```sh
docker compose -f docker-compose.yaml -f deploy/docker-compose.integration.yaml --profile integration up -d --wait postgres kafka kafka2 kafka3
export KAFLUX_TEST_KAFKA_SEED=127.0.0.1:19092
export KAFLUX_TEST_DATABASE_URL='postgres://kaflux:YOUR_LOCAL_PASSWORD@127.0.0.1:15432/kaflux?sslmode=disable'
cd backend
go test -race -p 1 ./...
```

Replace `YOUR_LOCAL_PASSWORD` with your database password, URL-encoding any URI-reserved characters.
Serialize test packages because they change broker throttle settings on the shared fixture.

## Recreating the controller

Kafka fixture storage belongs to each container. If the single controller is recreated, recreate all
three Kafka containers together to avoid stale broker metadata. From the repository root:

```sh
docker compose -f docker-compose.yaml -f deploy/docker-compose.integration.yaml --profile integration up -d --force-recreate --wait kafka kafka2 kafka3
```

This resets local Kafka test data and preserves the PostgreSQL volume.

## Related documentation

- [Documentation index](README.md)
- [Rebalancing](rebalancing.md)
- [Live reassignment throttles](live-throttle.md)
- [Message decoding](message-decoding.md)
