# Completion roadmap

Kaflux has a locally tested operational core. It has not met the complete product specification or production release criteria. Passing local tests does not establish safety against a production Kafka estate.

## Milestone 1: Complete the operator workflows

Close the documented feature gaps in authentication, authorization administration, message formats, schema references, alerts, and table workflows. Each feature needs a working backend, backend authorization, bounded resource use, appropriate audit events, usable error states, and meaningful tests. Preserve the explicit distinction between simulator data and live cluster data.

The source audit confirmed missing user/role administration. Frontend logout, administrator session inventory/revocation, backend revocation-failure handling, configurable session lifetime and persistent topic filter URLs have since been implemented. Existing topic tables already use virtualization and server pagination; their remaining usability and scale requirements need separate qualification. Native GitHub/header authentication, MFA/JIT policies, alert delivery and Glue decoding remain outside the implemented scope.

Bounded Protobuf schema-reference resolution is now implemented, including authorization for each referenced subject and limits on dependency depth, count, total schema bytes, and request duration. Unsupported formats return per-record errors while preserving original bytes. Remaining schema/format support must follow the same constraints.

## Milestone 2: Qualify reliability and security

Exercise multiple API/worker instances, worker termination during reassignment, lease expiry, database outages, controller changes, cancellation, rollback, and restoration of owned throttles. Verify backup restoration and session/provider failure behavior. Review destructive operations, secret redaction, authorization boundaries, and configuration updates.

Establish measurable browser memory, API latency, concurrency, metrics-cardinality, and accessibility budgets using the large-cluster simulator. Synthetic serialization benchmarks alone do not qualify these budgets.

## Milestone 3: Qualify supported deployments

Run credentialed acceptance tests for the declared Kafka security matrix, MSK variants and intelligent-rebalancing capability checks, OIDC/LDAP providers, registry/Connect integrations, and monitoring backends. Validate the Helm deployment on a real Kubernetes environment, including replicas, ingress, secrets, probes, network policy, and failure recovery.

Embedded SQLite now permits single-instance real Kafka operation without a separate database service. Local container authentication and restart persistence are verified. PostgreSQL remains the shared-state option for multiple replicas; SQLite backup restoration and runtime Kubernetes qualification remain release gates.

These checks require suitable authorized environments. Unavailable services remain explicitly unverified; fixture tests cannot substitute for interoperability evidence.

## Release decision

A release is complete only when the promised scope has passed its acceptance gates, deployment artifacts have been rebuilt and checked, and operational documentation matches observed behavior. Record results and remaining exclusions in [status.md](status.md).

No reliable completion date is established. A calendar estimate requires the remaining scope to be audited and the external test environments to be available. Report completed milestones and concrete remaining gates rather than an unsupported percentage or date.
