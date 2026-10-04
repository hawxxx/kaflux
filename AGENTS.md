# Repository Guidelines

## Project Structure & Module Organization

Kaflux is a new Kafka operations console. The repository currently contains design documentation; application scaffolding is pending design approval. Read `docs/kaflux-design.md` before implementing features.

The proposed layout separates `backend/` for the Java API and workers, `frontend/` for the React application, `deploy/` for containers and Kubernetes resources, and `docs/` for architecture and operational guidance. Keep tests beside their owning modules and static UI assets under `frontend/public/` once these directories exist.

## Build, Test, and Development Commands

No build scripts or executable application exist yet. Do not report successful builds or tests until tooling is implemented. Planned commands are `./mvnw verify` in `backend/`, `npm ci` and `npm run dev` in `frontend/`, and `docker compose up --build` at the repository root. Add exact commands and prerequisites here when scaffolding lands.

## Coding Style & Naming Conventions

Use four-space indentation for Java and two spaces for TypeScript, YAML, and JSON. Use PascalCase for Java classes and React components, camelCase for functions, and kebab-case for configuration filenames. Organize backend packages by feature. Enforce formatting and linting through build tooling when introduced; no formatter is currently configured.

## Testing Guidelines

Plan JUnit and Testcontainers coverage for Kafka administration and authentication, Vitest for frontend logic, and Playwright for user workflows. Name Java tests `*Test.java` and frontend tests `*.test.ts(x)`. Verify authorization failures, unavailable brokers, stale rebalance plans, restart recovery, and partial metrics responses. Coverage thresholds are not established.

### Browser Testing

Prefer `agent-browser` when available for interactive browser verification to reduce token use. Request focused snapshots and return concise findings through context-mode instead of dumping full page output. Use Playwright when `agent-browser` is unavailable or a workflow requires its capabilities, and retain automated Playwright regression tests for critical operations. Verify the actual rendered workflow, including loading, failure and responsive states; passing unit tests alone does not establish browser behavior.

Scroll offscreen targets into view before clicking, and assert the resulting state; a successful CLI command alone does not prove that the UI action occurred.

## Commit & Pull Request Guidelines

There is no Git history establishing a commit convention. Prefer focused Conventional Commits, such as `feat(metrics): cache broker snapshots`. PRs should explain behavior, link relevant issues, list actual verification, and include screenshots for UI changes. Document configuration and migration impacts.

## Security & Configuration

Never commit credentials, private dashboard identifiers, message payloads, or environment-specific broker addresses. Use sanitized fixtures and secret references. Require backend authorization and audited plan approval for mutations; keep demo data explicitly labeled.
