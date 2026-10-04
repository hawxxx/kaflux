# Live reassignment throttles

## Design

Operators can request a positive replication rate for a running reassignment. The API checks every
affected topic's execution permission, confirmation, plan hash, cluster capability and job state,
then stores one pending request. Kafka configuration changes happen in the lease-owning background
worker rather than the HTTP handler.

## Worker transition and recovery

1. The worker durably records the transition from the currently owned rate to the requested rate
   before contacting Kafka.
2. Both values are recognized during crash recovery; unrelated external values are preserved.
3. After configuration propagation is verified, the worker clears the transition and acknowledges
   the request using its lease and request revision.

The original pre-operation settings are never replaced.

## Request limits

Cancellation and terminal cleanup take precedence over throttle requests. Zero does not mean pause:
Kafka does not expose a general safe reassignment pause operation. Rates must be between 1 and
1,000,000,000,000 bytes per second. Only one request may be pending for a job.

## Verification

Test interrupted updates, retries, external changes, stale workers, cancellation races, unchanged
original settings, authorization failures and explicit MSK denial. Verify actual broker
configuration application and restoration using the local Kafka fixture before claiming live Kafka
support. Browser tests must distinguish the current verified rate from a pending request.

## Related documentation

- [Documentation index](README.md)
- [Rebalancing](rebalancing.md)
- [Local integration testing](integration-testing.md)
