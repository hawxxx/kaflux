# Kaflux Implementation Plan

**Goal:** Deliver an operational Go/React Kafka console with native monitoring and safe balancing.

**Architecture:** Go adapters implement a common provider interface; PostgreSQL persists audit and operations. React consumes a versioned API with cached snapshots and server-side pages. Explicit simulator mode supports reproducible development and scale benchmarks.

**Stack:** Go, franz-go, pgx, React, TypeScript, Vite, TanStack libraries, lightweight charts, Docker, Helm.

## 1. Contracts and core correctness

- [ ] Define backend transport models and Kafka provider interface in `backend/internal/model` and `backend/internal/kafka`.
- [ ] Write failing planner, skew, RBAC, and stale-plan tests; run `go test ./...` to observe failure.
- [ ] Implement deterministic planner and runtime safety validation; verify tests and benchmarks.

## 2. API and persistence

- [ ] Define `/api/v1` routes in `docs/openapi.yaml`.
- [ ] Implement real franz-go metadata, groups, messages, and reassignment adapters.
- [ ] Implement PostgreSQL migrations, sessions, audit, durable jobs, leases, and broker reconciliation.
- [ ] Verify API authorization, failure responses, expiry, CSRF, approval, and restart behavior.

## 3. Metrics

- [ ] Add sanitized native dashboard catalog, import command, metrics gateway, bounded cache, and Prometheus adapter.
- [ ] Test request coalescing, datasource failure isolation, timeout, limits, and cancellation.
- [ ] Implement AWS adapters only with truthful compatibility status and tests.

## 4. Frontend

- [ ] Build original SVG mark, responsive shell, dark/light themes, command palette, and deep links.
- [ ] Implement overview, topics/details, brokers, groups, messages, metrics, balance, reassignments, audit, and configuration using typed API contracts.
- [ ] Add component and Playwright workflows for loading/errors, paging, auth, plan preview, execution, and rollback.
- [ ] Verify large tables and narrow layouts in a real browser.

## 5. Packaging and evidence

- [ ] Create Dockerfile, Compose, Helm, config examples, CI, benchmark simulator, README, and license.
- [ ] Run Go race tests/vet, frontend tests/typecheck/build, Kafka integration, Docker build/startup, Compose, and Helm validation.
- [ ] Review security/correctness and fix findings before final reporting.
- [ ] Write `docs/status.md` with exact support, verification, and remaining release gates.

Changes remain in the requested workspace. No deployment, external messages, or publication is authorized. Keep `AGENTS.md` unchanged per the original request.
