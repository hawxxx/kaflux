# Native Metrics

The supplied Grafana dashboard is a specification, not an iframe. Its 133 data panels include 168
PromQL expressions (164 unique) and four CloudWatch panels. Import titles, expressions, units,
legends, variable definitions, and datasource kinds into a sanitized native catalog.
Environment-specific names must become bindings.

## Gateway

Only the backend queries metrics. Per-datasource schedulers coalesce identical queries, cache
bounded results with TTL, enforce deadlines and concurrency, and isolate failures from Kafka
metadata. Bound response bytes, series, samples, query range, and step. Back off with jitter and
open a circuit after repeated failures. Cancel unused work without allowing one subscriber to cancel
work shared by others.

| Datasource                                 | Gateway                           |
| ------------------------------------------ | --------------------------------- |
| Prometheus, Thanos, Mimir, VictoriaMetrics | Configured PromQL-compatible APIs |
| AMP                                        | SigV4 through AWS credentials     |
| CloudWatch                                 | Batched GetMetricData calls       |

Adapters must disclose compatibility limits.

## Presentation

Fetch overview aggregates first. Historical charts query when visible using IntersectionObserver;
hidden tabs stop polling. Topic lists never start a metrics query per row. Return snapshot timestamp
and source status. Display “Metrics temporarily unavailable” while preserving Kafka navigation.

## Correctness

Escape `$instance` values and interpret `$__range` using bounded server-side durations. Preserve
metric units. Replica bytes are physical storage; leader bytes approximate logical topic size.
Missing telemetry is unknown. Fetch waiting time alone is not failure; correlate queues, local time,
errors, ISR, and follower lag.

## Balance

Expose mean, min/max, population standard deviation, CV, max-to-mean ratio, and percentage
deviations for each measured dimension. Use transparent formulas and no unexplained composite health
score. All-zero distributions have zero imbalance; insufficient or partial samples are disclosed.

### Topic distribution

The topic **Distribution** tab (`GET /clusters/{cluster}/topics/{topic}/balance`) applies the same
statistics to one topic. Every broker in the cluster is included, so brokers holding none of the
topic count as zero. Because replica and leader counts are integers, a spread of at most one is the
most even placement possible and is reported as balanced even when the CV crosses a threshold.
Broker bytes are estimated as the sum of the sizes of the partitions each broker hosts and are
unavailable unless every partition size is known.

The view also reports:

- the preferred-leader ratio (partitions led by their first assigned replica);
- under-replicated and offline partitions, and out-of-sync replicas per broker;
- partitions whose replicas share a single rack, when brokers span more than one rack;
- a partition-by-broker placement matrix marking leaders, in-sync followers and out-of-sync replicas.

Findings are ordered critical, warning, then info. **Analyze** re-reads cluster metadata.

## Related documentation

- [Documentation index](README.md)
- [Architecture](architecture.md)
- [Rebalancing](rebalancing.md)
