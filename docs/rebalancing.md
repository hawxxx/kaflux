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

Preview does not mutate Kafka. The replication throttle is optional (default 10 MiB/s in the plan
form); set `requireThrottle: true` on a cluster to reject execution without one. A second approver
must be a different authorized identity.

## Execution

Execution follows the production-tested `kafka-rebalance-msk.sh` workflow:

- Topics run one at a time, in the order chosen in the plan form. Each topic's partitions are sent to
  Kafka in one reassignment, with no per-broker cap; the throttle limits load.
- Before a topic starts, the cluster must have no offline or under-replicated partitions and no
  reassignment owned by someone else. The job waits, and pauses after 10 minutes.
- The throttle, when set, covers only the moving topic and is removed when the topic finishes.
- After each topic a preferred leader election runs, as `kafka-leader-election.sh --election-type
  PREFERRED` does. After three failed attempts it becomes a warning; the job still completes.
- Progress is reported in topics, partitions and bytes. The ETA uses remaining bytes over the
  measured rate, or the average time per topic when partition sizes are unavailable.
- Every step is written to the job's activity log (newest 2000 lines, deleted with the job).

## Pause, cancel and rollback

- **Pause** takes effect at the next topic boundary. A paused job keeps the cluster lock and makes no
  Kafka calls. **Resume** retries a failed topic; **Skip topic** leaves it as it is and continues.
- A failed topic, a cluster that stays unhealthy, or Kafka blocking manual reassignment after some
  topics moved pauses the job with the reason.
- **Cancel** asks Kafka to cancel the moves in flight; moved topics stay moved.
- **Cancel & roll back** cancels, then queues a rollback job of the partitions that moved, in reverse
  topic order, approved by the person who asked. Both are stored in one transaction.
- **Download backup** returns the original assignments in `kafka-reassign-partitions.sh` format for
  recovery without Kaflux. **Report CSV** lists every partition with its before and after brokers.

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
