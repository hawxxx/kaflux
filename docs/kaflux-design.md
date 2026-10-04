# Kaflux: Proposed Product and Architecture

**Document status:** historical design proposal. It records the original options and desired scope,
including alternatives that were not selected. For current architecture and verification, read
[architecture](architecture.md) and [implementation status](status.md).

## Product Goal

Build a responsive, self-hosted Kafka operations console combining message exploration, cluster
management, monitoring, and governance workflows inspired by Redpanda Console, Conduktor, and
Kafbat. Implement an original interface and architecture rather than assuming compatibility with
every proprietary feature.

Target deployments include self-managed Kafka with ZooKeeper or KRaft, Amazon MSK provisioned
clusters, and capability-limited MSK Serverless clusters. Treat compatibility as a tested matrix,
not a universal promise. Cluster capabilities determine which actions are available.

## Architecture Options

1. **Recommended: Java Kafka backend and React frontend.** Use Spring Boot, the official Kafka
   AdminClient, Spring Security, React, and TypeScript. This provides a direct route to Kafka
   administration, MSK IAM, LDAP, and OIDC. The tradeoff is JVM resource usage and two build
   toolchains.
2. **Go backend and React frontend.** Attractive container footprint and concurrency, but Kafka
   administration and authentication integration require a separate compatibility assessment.
3. **Extend an existing console.** Faster initial breadth, but customization, dependency upgrades,
   licensing review, and upstream architecture constrain the result.

Use PostgreSQL for cluster configuration, operation records, audit events, and durable job leases.
Store credential references rather than plaintext credentials. Keep bounded in-memory caches for
monitoring snapshots. Introduce shared cache infrastructure only when measured scale requires it.

## Application Layout

- `backend/`: API, authentication, Kafka adapters, metrics adapters, analysis, operation workers,
  and persistence migrations.
- `frontend/`: accessible React interface, route modules, typed API client, tables, charts, and
  workflow tests.
- `deploy/`: Docker, Compose, Helm templates, and configuration examples.
- `docs/`: design, compatibility matrix, deployment instructions, runbooks, and security model.

Keep domain planning separate from Kafka/network calls. Publish an OpenAPI contract and use typed
request/response schemas.

## UI and Operator Workflows

Provide cluster overview, topics, topic details, brokers, consumer groups, messages, monitoring,
skew analysis, rebalance operations, and administration. Later modules add Schema Registry, Kafka
Connect, ACL management, quotas, and saved investigations.

Use a restrained operations-console design: dark and light themes, teal accents, readable numerals,
compact desktop tables, and accessible mobile detail views. Provide keyboard navigation, visible
focus, reduced-motion support, empty/error states, and explicit observation timestamps. Preserve
filters and selected cluster in navigable URLs. Use server-side pagination and virtualized tables
for large inventories.

Topic details show partition count, replication factor, retention, cleanup policy, leaders,
replicas, ISR, offsets, and available sizes. Distinguish logical leader bytes from physical
replicated bytes. Mark unavailable measurements as unknown rather than zero.

Message exploration uses bounded consumers, byte/record/time limits, cancellation, binary-safe
rendering, and optional payload redaction. Support partition/offset/timestamp navigation and common
serialization formats. Producing messages, resetting offsets, changing configurations, and deleting
resources require action-specific permissions and explicit previews.

## Authentication and Authorization

Separate UI user authentication from Kafka connection authentication.

User authentication supports OIDC authorization-code flow, including authentik; LDAP/Active
Directory with TLS; and Basic Auth backed by hashed credentials. Implement provider-specific OAuth2
adapters only where identity validation is defined: OAuth2 alone does not supply an identity
contract. Support secure server-side sessions, CSRF protection, login throttling, issuer/audience
validation, OIDC state and nonce checks, and PKCE where supported.

Map provider groups to roles scoped by cluster and topic patterns. Start with viewer, operator, and
administrator roles, plus explicit permissions for message inspection and mutations. Enforce all
permissions in the backend. Record actor, resource, action, outcome, and request identifier in audit
events without logging secrets or message bodies.

Kafka connections support TLS/mTLS, SASL PLAIN, SCRAM, OAuth bearer, and AWS MSK IAM through
appropriate client plugins. Assess Kerberos separately. Prefer AWS workload identity and the SDK
credential provider chain to static keys. User login to Kaflux does not automatically confer broker
credentials.

## Kafka and MSK Compatibility

Use broker Admin APIs for inventory and supported administration on both ZooKeeper-backed and
KRaft-backed clusters. Avoid a mandatory ZooKeeper connection. Expose mode-specific health only when
detection and metrics support it; label uncertain mode as unknown.

MSK integration adds configured or discovered endpoints, IAM/SCRAM/TLS authentication, regional AWS
configuration, CloudWatch metrics, and Amazon Managed Service for Prometheus query signing. Gate
reassignment and other administrative operations according to broker capabilities and AWS service
restrictions. Preserve native Kafka authorization errors as actionable, sanitized responses.

## Native Metrics Integration

The supplied `Kafka Broker Overview-1791004114520.json` contains 133 metric panels, 168 PromQL
expressions, and 164 unique PromQL expressions. Four panels also use CloudWatch. Prometheus
variables include `$instance` and `$__range`. Coverage includes topic and broker size, replication
health, consumer lag, requests, throughput, controller/KRaft health, JVM, CPU, disk, network,
quotas, and exporter health.

Import the dashboard into a normalized metric catalog: panel title, datasource kind, expressions,
units, legends, variables, thresholds, and supported layout metadata. Render native charts without
requiring a Grafana iframe. Report unsupported transformations explicitly. Treat imported JSON as
untrusted configuration; never evaluate arbitrary scripts or permit uploaded datasource URLs to
bypass administrator configuration.

Map Grafana datasource identifiers to explicitly configured Prometheus/AMP and CloudWatch adapters.
Substitute variables using typed, escaped rules, with server-controlled time windows and steps.
Replace environment-specific CloudWatch cluster dimensions with configured bindings. Do not commit
the original private dashboard; generate sanitized catalog fixtures when implementation begins.

A monitoring scheduler performs bounded queries off the HTTP request path. Deduplicate requests by
datasource, scope, expression, range, and step. Batch CloudWatch retrieval, cap parallelism, apply
deadlines/backoff, and cache timestamped snapshots. Fetch overview metrics first; load expensive
details only when opened. Bound chart series and point counts; pause unused subscriptions and cancel
abandoned requests. Show stale or missing metrics instead of blocking navigation.

These controls limit monitoring impact; they do not justify a guarantee of zero performance cost.
Establish measurable budgets through load tests, including 10,000 topics, high-cardinality metrics,
slow datasources, concurrent operators, and mobile browser rendering.

## Skew and Capacity Analysis

Calculate partition-count, leader-count, replica-byte, logical-byte, and traffic distribution
independently. Report coefficient of variation, max-to-mean ratio, share imbalance, and statistical
skewness where sample size supports it. Explain each result and its sampling window. Handle zeros,
missing brokers, insufficient samples, and heterogeneous broker capacity explicitly.

Support topic, partition, broker, and rack/AZ drilldowns. Normalize disk and traffic utilization by
known capacity when possible. Do not sum leaders and replicas indiscriminately or infer partition
traffic from topic totals. Highlight hot partitions and uneven leader placement; identify missing
telemetry before suggesting changes.

## Rebalance Planning and Execution

The referenced MSK shell script provides selected-topic input, broker selection, dry-run,
reassignment, throttling, verification, saved assignments for rollback, and preferred-leader
election. Preserve those workflows using native Admin APIs rather than executing user-supplied shell
commands.

Planning must preserve replication factor, avoid duplicate replicas, respect rack/AZ constraints,
consider available capacity, and detect existing reassignments. Support selected topics and broker
pools. Present old/new assignments, estimated transferred bytes with confidence, placement
violations, and expected balance changes. Store the original assignment, approved plan hash, and
metadata fingerprint.

Before execution, revalidate permissions, topology, ISR health, capacity, and the approved
fingerprint. Require a distinct operator confirmation. Scope job locking to affected partitions and
coordinate leases across application replicas. Persist state transitions and submission attempts;
reconcile with broker state after worker restart rather than blindly resubmitting.

Run asynchronously with bounded concurrency, throttling, deadlines, audit events, and streamed
progress. Reassignment cancellation and rollback are separate operations: rollback is another
validated reassignment and may fail if topology has changed. Preferred-leader election has its own
preview and permission.

Track preexisting throttle settings and restore only settings owned by the operation. Never clear
unrelated throttles. Treat uncertain Kafka submission results as reconciliation cases. Do not infer
successful completion from CLI exit status alone.

## Deployment and Operations

Build a multi-stage, non-root container serving the compiled UI and backend. Provide a local Compose
profile with PostgreSQL, KRaft Kafka, and monitoring; add opt-in authentication and integration
profiles. Provide Helm deployment with Secret references, resource limits, probes, network-policy
examples, ingress/TLS options, disruption budgets, and workload identity documentation.

Keep readiness distinct from liveness. A remote Kafka or metrics outage should produce a visible
degraded state, not endless process restarts. Support graceful worker shutdown, rolling upgrades,
database migrations, backups, structured logs, application metrics, and OpenTelemetry tracing.
Explain how to run multiple replicas with shared persistence and leases.

## Delivery Sequence and Acceptance Gates

1. **Foundation:** build tooling, real Kafka connectivity, explicit demo mode, cluster/topic/broker
   inventory, secure authentication, authorization, audit storage, and Compose deployment.
2. **Monitoring:** sanitized dashboard catalog, Prometheus/AMP and CloudWatch adapters, native
   charts, size accounting, skew analysis, and load budgets.
3. **Operations:** durable selected-topic rebalance planning, approval, execution, reconciliation,
   throttle management, cancellation, and rollback.
4. **Broader tooling:** messages, consumer offset operations, Schema Registry, Connect, ACLs,
   quotas, and operational guidance.
5. **Release hardening:** authentication-provider matrix, Kafka/MSK compatibility evidence, Helm
   verification, browser accessibility, failure injection, load tests, and documented recovery
   procedures.

Use JUnit and Testcontainers for backend integration, Vitest for UI logic, and Playwright for
complete workflows. Include plan-staleness, role denial, broker failure, partial metrics, session
expiry, rolling restart, throttle restoration, and migration tests. Run credentialed MSK/OIDC/LDAP
tests only against configured test environments. Clearly distinguish local evidence from unverified
external integrations.

Call a release production-ready only after these gates pass and operational limitations are
documented.

## References Inspected

- Rebalance source: https://github.com/hawxxx/kafka/blob/main/kafka-rebalance-msk.sh
- AWS MSK IAM: https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html
- Authentik OAuth2/OIDC: https://docs.goauthentik.io/add-secure-apps/providers/oauth2/
- User-supplied dashboard: inspected locally; retain outside version control until sanitized.
