# Broker capacity

The Balance page compares each broker's load with how much a broker is meant to hold. Kaflux does
not estimate capacity. A cluster's capacity comes from one of two sources, and any value that
neither provides stays unknown.

## Where capacity comes from

Kaflux uses the first source that provides a number.

| Order | Source                   | Applies to                                     | How to set it                     |
| ----- | ------------------------ | ----------------------------------------------- | --------------------------------- |
| 1     | Your cluster declaration | Any Kafka: EC2, Kubernetes, bare metal, or MSK | `capacity` block in `config.yaml` |
| 2     | Limits AWS publishes     | Amazon MSK clusters with a listed broker type  | `mskClusterArn`, configured or discovered |

A cluster with neither source shows **Unknown**, the busiest broker's replica count, and the
median across all brokers. Kafka's admin API does not report instance types or limits, so
self-managed clusters need a declaration.

## Declare capacity for a self-managed cluster

Add a `capacity` block to the cluster. Use numbers from your own sizing and load testing.

```yaml
clusters:
  - id: orders
    seeds: [kafka-1.example.internal:9093]
    capacity:
      label: r6i.4xlarge, 3 AZ # shown on the Balance page
      partitionsPerBroker:
        recommended: 4000 # replicas per broker you plan for
        maximum: 6000 # replicas per broker you treat as a hard ceiling
      diskBytesPerBroker: 4398046511104 # usable log storage, 4 TiB
```

| Field                             | Meaning                                                         |
| ---------------------------------- | ---------------------------------------------------------------- |
| `label`                             | Text shown as the tile headline, for example the instance type   |
| `partitionsPerBroker.recommended`   | Replicas a broker should hold. Leaders and followers both count  |
| `partitionsPerBroker.maximum`       | Replicas above which a broker is reported as over its maximum    |
| `diskBytesPerBroker`                | Usable log storage of one broker. Used with measured broker sizes |

Set at least `partitionsPerBroker` or `diskBytesPerBroker`. Kaflux refuses to start when the `capacity` block in `config.yaml` has neither, has negative values,
sets `recommended` above `maximum`, or contains an unknown key, so a typo cannot silently disable the
check. A clusters file in JSON is checked for values but not for unknown keys. A declaration always takes precedence over the
limits AWS publishes, so you can also use it to apply a limit you have tested.

## Amazon MSK

For an MSK cluster, Kaflux reads the broker type with `kafka:DescribeClusterV2` and looks up the
limits AWS documents for it. There are two ways to identify which cluster to describe:

### Configured ARN

Set `mskClusterArn` and `region` on the cluster. This is the most direct route and the only one
available when seeds are not MSK bootstrap broker hostnames, for example behind a custom DNS name
or a load balancer.

### Automatic discovery

When `mskClusterArn` is left empty and the cluster's seeds are MSK bootstrap broker hostnames
(`b-<n>.<cluster-name-without-dashes>.<id>.c<generation>.kafka.<region>.amazonaws.com`), Kaflux
discovers the cluster itself:

1. It calls `kafka:ListClustersV2` and compares every returned cluster's name, with
   non-alphanumeric characters removed, against the name encoded in the bootstrap hostname.
2. Among name matches, it keeps only the cluster whose ARN ends in the same generation suffix
   (`-<n>`) as the `c<n>` label in the hostname. MSK's bootstrap hostnames and cluster ARNs share
   this generation number, so this disambiguates clusters that were recreated under the same name.
3. Discovery succeeds only when exactly one cluster matches both. Zero or multiple matches leave
   capacity unknown with a reason naming the ambiguity; set `mskClusterArn` to resolve it directly.

Discovery is lazy: it runs on the first request that needs it, not at startup, and its result is
cached. A slow or misconfigured AWS account cannot block Kaflux from starting. A successfully
discovered ARN also resolves the cluster settings page's rebalancing status, which otherwise shows
"Configure mskClusterArn" for a cluster that has not set one.

### Documented limits by broker family

| Broker family | Documented limits used                                                                  |
| ------------- | --------------------------------------------------------------------------------------- |
| Express       | `express.m7g.large` through `express.m7g.16xlarge`, recommended and maximum per broker   |
| Standard      | `kafka.t3.small`, `kafka.m5.*` and `kafka.m7g.*`, recommended and maximum per broker     |

The counts include leader and follower replicas. The recommended count is guidance for clusters
that send traffic to every partition. For Express brokers MSK does not allow more than the maximum.
For Standard brokers, above the maximum MSK blocks updates such as configuration changes and moving
to a smaller broker size. A broker type that is not in the table, such as a newer family, stays
unknown until the table is updated or you declare capacity yourself.

The values were read from the AWS documentation on 2026-10-06. AWS can change them, so confirm them
in the [Express broker partition quota][express] and the [Standard broker best practices][standard]
before relying on them for a decision.

### Required IAM permissions

| Action                     | Needed for                                                               |
| --------------------------- | ------------------------------------------------------------------------- |
| `kafka:DescribeClusterV2`   | Reading the broker type and rebalancing status of a known cluster ARN     |
| `kafka:ListClustersV2`      | Discovering the cluster ARN when `mskClusterArn` is not configured        |

Both calls use the AWS credentials Kaflux already resolves for the cluster (the role from
`roleArn`, or the runtime's default credentials). No other IAM action is required for capacity or
rebalancing status.

### When AWS denies the request

If AWS returns HTTP 403 for either call, the reason shown names the specific permission that is
missing, for example:

> AWS denied the request to describe the MSK cluster (HTTP 403 AccessDenied). Grant the
> kafka:DescribeClusterV2 permission to the role Kaflux runs as.

This replaces a generic "capacity unknown" message so the fix is unambiguous: grant the named
action to the role, rather than guessing which call failed.

## Reading the result

The tile headline is the label or broker type. Its second line is the worst status of any broker.

| Status                       | Meaning                                                   |
| ----------------------------- | ----------------------------------------------------------- |
| Within limits                 | No broker is above a limit that is set                      |
| Above the recommended count   | At least one broker is above `recommended`                  |
| Over the maximum              | At least one broker is above `maximum`                      |
| Unknown                       | No partition limits are set                                 |

The line below the tiles states the limits, how close the busiest broker is to the recommended and
maximum counts, the median replica count across every broker, and where the numbers come from
(configured, or the AWS-documented limits for the detected broker type). Limits are inclusive: a
broker exactly at a limit is not above it. The busiest broker and median are reported even when
capacity itself is unknown, since replica counts always come from cluster metadata.

The `GET /clusters/{cluster}/balance` response carries the same data in `capacity`, including every
broker, `busiest`, and `medianReplicas`. `capacityKnown` is `true` when a source provided limits.

[express]: https://docs.aws.amazon.com/msk/latest/developerguide/limits.html#msk-express-broker-partition-quota
[standard]: https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html
