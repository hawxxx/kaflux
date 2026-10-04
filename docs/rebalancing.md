# Rebalancing

## Contract

A plan selects topics and eligible brokers, preserving every replication factor and replica
uniqueness. Excluded brokers cannot be targets. Rack-aware planning requires known rack labels and
sufficient rack diversity; otherwise fail with an explanation. Never touch unrelated topics.

Store original assignments, proposed assignments, metadata fingerprint, plan hash, actor, policy
decisions, and approval. Show before/after partition and leadership distributions and moved
replicas. Estimated bytes must disclose unavailable sizes and count replica copies correctly.

## Workflow

1. Analyze.
2. Generate.
3. Review/dry-run.
4. Explicitly approve.
5. Execute asynchronously.
6. Reconcile.
7. Verify.

Preview does not mutate Kafka. Require production throttling where configured. A second approver
must be a different authorized identity.

## Safety

Reject unreachable clusters, offline partitions, insufficient ISR, invalid broker IDs, changed
assignments, conflicting reassignments, capacity violations where reliable capacity is configured,
and policy limits. Unknown capacity must be exposed; a policy may require capacity evidence. Capture
and restore only operation-owned throttle configuration.

## Recovery

Use durable PostgreSQL jobs and cluster/partition conflict locks. A lease prevents concurrent
application workers; fencing and broker reconciliation protect uncertain outcomes. Stop means
Kafka-supported reassignment cancellation, not guaranteed instant interruption. Rollback creates and
validates a new assignment job using saved originals. Broker changes can make rollback impossible;
surface that condition.

## Algorithm evidence

Deterministic tests cover replication factor, duplicate avoidance, rack diversity, exclusions,
unrelated topics, stale-plan rejection, rollback, and dry-run immutability. Native Admin API
behavior follows the public MSK reference script's workflows without invoking shell code.

## Related documentation

- [Documentation index](README.md)
- [Live reassignment throttles](live-throttle.md)
- [Native metrics](metrics.md)
- [Local integration testing](integration-testing.md)
