package mcp

// readTools are the read-only tools (scopes read and messages). Paths follow
// docs/openapi.yaml; annotations are derived from Scope in Catalog.
var readTools = []Tool{
	// Global.
	{Name: "kaflux_version_get", Method: "GET", Path: "/version", Scope: "read",
		Description: "Get the running Kaflux build version. No arguments. Returns the version string."},
	{Name: "kaflux_cluster_list", Method: "GET", Path: "/clusters", Scope: "read", Paginated: true,
		Description: "List the Kafka clusters Kaflux manages. Optional search, e.g. search=\"prod\". Returns each cluster's id (the cluster argument of other tools), name, environment, state, broker/topic/partition counts, Kafka version and metadata mode."},
	{Name: "kaflux_audit_global", Method: "GET", Path: "/audit", Scope: "read", Paginated: true,
		Description: "List audit events across all clusters, newest first. Optional limit, e.g. limit=20. Returns actor, action, target, outcome and time for each event."},
	{Name: "kaflux_metrics_catalog_global", Method: "GET", Path: "/metrics/catalog", Scope: "read",
		Description: "List the metric names Kaflux can query, independent of cluster. No arguments. Returns the normalized native metric panel catalog; pass a metric id to kaflux_metrics_query."},

	// Cluster.
	{Name: "kaflux_cluster_overview", Method: "GET", Path: "/clusters/{cluster}/overview", Scope: "read",
		Description: "Summarize one cluster. Args: cluster, e.g. \"local\". Returns the cluster, a broker summary, nullable totals (topics, partitions, under-replicated) and observedAt."},
	{Name: "kaflux_cluster_capabilities", Method: "GET", Path: "/clusters/{cluster}/capabilities", Scope: "read",
		Description: "Show observed cluster capabilities and manual reassignment restrictions, such as MSK intelligent rebalancing state that blocks execution. Args: cluster, e.g. \"local\". Returns capability states and evidence."},
	{Name: "kaflux_cluster_configuration", Method: "GET", Path: "/clusters/{cluster}/configuration", Scope: "read",
		Description: "Show a cluster's sanitized capability configuration. Args: cluster, e.g. \"local\". Returns configured capability status; broker credentials and mutable broker settings are never included."},
	{Name: "kaflux_balance_get", Method: "GET", Path: "/clusters/{cluster}/balance", Scope: "read",
		Description: "Assess replica and leader distribution across brokers. Args: cluster, e.g. \"local\". Returns per-broker replica, leader and byte figures plus a flag saying whether broker capacity is known."},

	// Topics.
	{Name: "kaflux_topic_list", Method: "GET", Path: "/clusters/{cluster}/topics", Scope: "read", Paginated: true,
		Description: "List topics with paging and filters. Args: cluster, e.g. \"local\"; optional q=\"orders\", internal=false, sort=\"urp\". Returns topic rows (name, partitions, replication factor, under-replicated count) and paging totals."},
	{Name: "kaflux_topic_get", Method: "GET", Path: "/clusters/{cluster}/topics/{topic}", Scope: "read",
		Description: "Describe one topic. Args: cluster, topic, e.g. topic=\"orders\". Returns partitions with leaders, replicas, ISR and offsets, plus key configuration."},
	{Name: "kaflux_topic_config_get", Method: "GET", Path: "/clusters/{cluster}/topics/{topic}/config", Scope: "read",
		Description: "Read a topic's configuration. Args: cluster, topic, e.g. topic=\"orders\". Returns every config entry with value, source, default flag and sensitivity."},
	{Name: "kaflux_topic_balance", Method: "GET", Path: "/clusters/{cluster}/topics/{topic}/balance", Scope: "read",
		Description: "Show how a topic's replicas and leaders spread across brokers. Args: cluster, topic, e.g. topic=\"orders\". Returns per-broker replica, leader and byte counts, per-partition leader/replicas/ISR, skew and findings."},
	{Name: "kaflux_topic_unused", Method: "GET", Path: "/clusters/{cluster}/unused-topics", Scope: "read", Paginated: true,
		Description: "Find topics with no recent writes or consumers. Args: cluster; optional minAgeDays=30, includeInternal=false. Returns candidate topics with evidence and the fingerprints kaflux_topic_bulk_delete needs, plus evaluatedAt and partial flags."},

	// Brokers and consumer groups.
	{Name: "kaflux_broker_list", Method: "GET", Path: "/clusters/{cluster}/brokers", Scope: "read", Paginated: true,
		Description: "List brokers. Args: cluster, e.g. \"local\". Returns each broker with host, rack, controller flag and its replica and leader counts."},
	{Name: "kaflux_group_list", Method: "GET", Path: "/clusters/{cluster}/consumer-groups", Scope: "read", Paginated: true,
		Description: "List consumer groups. Args: cluster; optional search=\"billing\". Returns group id, state, members, topics and nullable lag."},
	{Name: "kaflux_group_get", Method: "GET", Path: "/clusters/{cluster}/consumer-groups/{group}", Scope: "read",
		Description: "Describe a consumer group. Args: cluster, group, e.g. group=\"billing-service\". Returns state, members, and per-partition committed offset, log bounds and nullable lag."},

	// ACLs.
	{Name: "kaflux_acl_list", Method: "GET", Path: "/clusters/{cluster}/acls", Scope: "read", Paginated: true,
		Description: "List Kafka ACL bindings. Args: cluster; optional filters resourceType=\"TOPIC\", resourceName=\"orders\", principal=\"User:alice\", operation, permission. Returns matching bindings."},

	// Schema registry.
	{Name: "kaflux_schema_integration_list", Method: "GET", Path: "/clusters/{cluster}/schemas", Scope: "read", Paginated: true,
		Description: "List schema registry integrations configured for a cluster. Args: cluster, e.g. \"local\". Returns integration ids to pass as integration to other schema tools."},
	{Name: "kaflux_schema_subject_list", Method: "GET", Path: "/clusters/{cluster}/schemas/{integration}", Scope: "read", Paginated: true,
		Description: "List subjects in a schema registry. Args: cluster, integration, e.g. integration=\"registry\". Returns subject names."},
	{Name: "kaflux_schema_get", Method: "GET", Path: "/clusters/{cluster}/schemas/{integration}/{subject}", Scope: "read",
		Description: "Get the latest schema of a subject. Args: cluster, integration, subject, e.g. subject=\"orders-value\". Returns the registry's latest version record (id, version, schema)."},
	{Name: "kaflux_schema_versions", Method: "GET", Path: "/clusters/{cluster}/schemas/{integration}/{subject}/versions", Scope: "read", Paginated: true,
		Description: "List a subject's registered versions. Args: cluster, integration, subject, e.g. subject=\"orders-value\". Returns version numbers."},
	{Name: "kaflux_schema_version_get", Method: "GET", Path: "/clusters/{cluster}/schemas/{integration}/{subject}/versions/{version}", Scope: "read",
		Description: "Get one schema version. Args: cluster, integration, subject, version, e.g. version=\"3\" or \"latest\". Returns the version record (id, version, schema)."},
	{Name: "kaflux_schema_config_get", Method: "GET", Path: "/clusters/{cluster}/schemas/{integration}/{subject}/configuration", Scope: "read",
		Description: "Read a subject's compatibility setting. Args: cluster, integration, subject, e.g. subject=\"orders-value\". Returns the compatibility level, e.g. BACKWARD."},
	{Name: "kaflux_schema_compatibility_check", Method: "POST", Path: "/clusters/{cluster}/schemas/{integration}/{subject}/compatibility", Scope: "read",
		Description: "Check a candidate schema against the subject's latest version without registering it. Args: cluster, integration, subject, body={confirmation:true, payload:{schema:\"...\"}}. Returns is_compatible and any messages."},

	// Kafka Connect.
	{Name: "kaflux_connector_integration_list", Method: "GET", Path: "/clusters/{cluster}/connectors", Scope: "read", Paginated: true,
		Description: "List Kafka Connect integrations configured for a cluster. Args: cluster, e.g. \"local\". Returns integration ids to pass as integration to other connector tools."},
	{Name: "kaflux_connector_list", Method: "GET", Path: "/clusters/{cluster}/connectors/{integration}", Scope: "read", Paginated: true,
		Description: "List connectors in a Connect cluster. Args: cluster, integration, e.g. integration=\"connect\". Returns connector names the caller may read."},
	{Name: "kaflux_connector_get", Method: "GET", Path: "/clusters/{cluster}/connectors/{integration}/{connector}", Scope: "read",
		Description: "Describe a connector. Args: cluster, integration, connector, e.g. connector=\"orders-sink\". Returns the Connect connector record with secrets redacted."},
	{Name: "kaflux_connector_config_get", Method: "GET", Path: "/clusters/{cluster}/connectors/{integration}/{connector}/config", Scope: "read",
		Description: "Read a connector's configuration. Args: cluster, integration, connector, e.g. connector=\"orders-sink\". Returns config keys and values with secrets redacted."},
	{Name: "kaflux_connector_status", Method: "GET", Path: "/clusters/{cluster}/connectors/{integration}/{connector}/status", Scope: "read",
		Description: "Get a connector's runtime status. Args: cluster, integration, connector, e.g. connector=\"orders-sink\". Returns connector and per-task state as Kafka Connect reports it."},

	// Audit and metrics.
	{Name: "kaflux_audit_cluster", Method: "GET", Path: "/clusters/{cluster}/audit", Scope: "read", Paginated: true,
		Description: "List audit events for one cluster, newest first. Args: cluster, e.g. \"local\"; optional limit=20. Returns actor, action, target, outcome and time."},
	{Name: "kaflux_metrics_catalog", Method: "GET", Path: "/clusters/{cluster}/metrics/catalog", Scope: "read",
		Description: "List metrics available for a cluster. Args: cluster, e.g. \"local\". Returns the metric panels the caller may query; pass a metric id to kaflux_metrics_query."},
	{Name: "kaflux_metrics_query", Method: "GET", Path: "/clusters/{cluster}/metrics/query", Scope: "read",
		Description: "Query a metric time series. Args: cluster, metric, e.g. metric=\"bytes_in\", optional range=\"1h\" (default 15m). Returns bounded time series and a partial-status flag."},

	// Rebalances.
	{Name: "kaflux_rebalance_list", Method: "GET", Path: "/clusters/{cluster}/rebalances", Scope: "read", Paginated: true,
		Description: "List rebalance jobs. Args: cluster, e.g. \"local\". Returns persisted plans and execution jobs with state, planHash and progress."},
	{Name: "kaflux_rebalance_get", Method: "GET", Path: "/clusters/{cluster}/rebalances/{job}", Scope: "read",
		Description: "Get a rebalance job and its plan. Args: cluster, job, e.g. job=\"rb-20261119-01\". Returns state, planHash (required by execute, pause, throttle), steps, changes, progress and ETA."},
	{Name: "kaflux_rebalance_events", Method: "GET", Path: "/clusters/{cluster}/rebalances/{job}/events", Scope: "read", Paginated: true,
		Description: "Read a rebalance job's event log. Args: cluster, job; optional after=42 for events past that sequence number. Returns up to 500 events, oldest first, and the last sequence number."},

	// Messages.
	{Name: "kaflux_message_consume", Method: "GET", Path: "/clusters/{cluster}/messages", Scope: "messages",
		Description: "Read messages from a topic without committing offsets. Args: cluster, topic=\"orders\"; optional partition=0 with offset=100, offsets=\"0:150,1:200\", timestamp, limit=20. Returns messages with partition, offset, timestamp, key, value and headers."},
	{Name: "kaflux_message_search", Method: "POST", Path: "/clusters/{cluster}/messages/search", Scope: "messages",
		Description: "Scan a topic for messages matching a filter. Args: cluster, body={topic:\"orders\", match:{...}, maxMatches:50}, optional partitions and from/to bounds. Returns matches, scanned count, done, stoppedBy and a resume position."},
}
