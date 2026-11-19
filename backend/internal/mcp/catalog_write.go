package mcp

// writeTools are the mutating tools (scopes write and destructive). Every one
// takes an optional confirmation token: call once to prepare, again to commit.
var writeTools = []Tool{
	// Topics.
	{Name: "kaflux_topic_create", Method: "POST", Path: "/clusters/{cluster}/topics", Scope: "write",
		Description: "Create a topic. Args: cluster, body={name:\"orders\", partitions:6, replicationFactor:3, config:{\"retention.ms\":\"604800000\"}}. Returns the created topic."},
	{Name: "kaflux_topic_config_update", Method: "POST", Path: "/clusters/{cluster}/topics/{topic}/config", Scope: "write",
		Description: "Change topic configuration. Args: cluster, topic, body={config:{\"retention.ms\":\"86400000\"}, confirmation:true}. Returns the updated configuration."},
	{Name: "kaflux_topic_partitions_add", Method: "POST", Path: "/clusters/{cluster}/topics/{topic}/partitions", Scope: "write",
		Description: "Increase a topic's partition count (cannot decrease). Args: cluster, topic, body={count:12, confirmation:true}. Returns the new partition count."},
	{Name: "kaflux_topic_copy", Method: "POST", Path: "/clusters/{cluster}/topics/{topic}/copy", Scope: "write",
		Description: "Create a new topic with the source topic's configuration. Args: cluster, topic (source), body={name:\"orders-v2\", partitions:6, replicationFactor:3}. Returns the created topic."},
	{Name: "kaflux_topic_delete", Method: "DELETE", Path: "/clusters/{cluster}/topics/{topic}", Scope: "destructive",
		Description: "Delete a topic and all its data. Args: cluster, topic, body={confirmation:\"<exact topic name>\"}, e.g. topic=\"orders-old\". Returns the deletion result."},
	{Name: "kaflux_topic_truncate", Method: "POST", Path: "/clusters/{cluster}/topics/{topic}/truncate", Scope: "destructive",
		Description: "Delete all records from a topic, keeping the topic. Args: cluster, topic, body={confirmation:\"<exact topic name>\"}. Rejected for compact-only topics. Returns the truncation result."},
	{Name: "kaflux_topic_recreate", Method: "POST", Path: "/clusters/{cluster}/topics/{topic}/recreate", Scope: "destructive",
		Description: "Delete and recreate a topic with the same settings, discarding all data. Args: cluster, topic, body={confirmation:\"<exact topic name>\"}. Returns the recreated topic."},
	{Name: "kaflux_topic_bulk_delete", Method: "POST", Path: "/clusters/{cluster}/topics/bulk-delete", Scope: "destructive",
		Description: "Delete up to 50 unused topics. Args: cluster, body={topics:[\"a\",\"b\"], minAgeDays:30, fingerprints:{...} from kaflux_topic_unused, confirmation:\"delete 2 topics\"}. Returns per-topic results."},

	// Consumer groups.
	{Name: "kaflux_group_reset_offsets", Method: "POST", Path: "/clusters/{cluster}/consumer-groups/{group}/reset-offsets", Scope: "destructive",
		Description: "Reset an inactive consumer group's offsets. Args: cluster, group, body={mode:\"earliest\", topics:[\"orders\"], preview:true}; then apply with preview:false, confirmation:\"<group>\" and the returned previewHash. Returns current and target offsets."},

	// ACLs.
	{Name: "kaflux_acl_create", Method: "POST", Path: "/clusters/{cluster}/acls", Scope: "write",
		Description: "Create an ACL binding. Args: cluster, body={acl:{resourceType:\"TOPIC\", resourceName:\"orders\", patternType:\"LITERAL\", principal:\"User:alice\", host:\"*\", operation:\"READ\", permission:\"ALLOW\"}, confirmation:true}. Returns the binding."},
	{Name: "kaflux_acl_delete", Method: "DELETE", Path: "/clusters/{cluster}/acls", Scope: "destructive",
		Description: "Delete an exact ACL binding. Args: cluster, body={acl:{resourceType:\"TOPIC\", resourceName:\"orders\", patternType:\"LITERAL\", principal:\"User:alice\", host:\"*\", operation:\"READ\", permission:\"ALLOW\"}, confirmation:true}. Returns the deleted bindings."},

	// Schema registry.
	{Name: "kaflux_schema_register", Method: "POST", Path: "/clusters/{cluster}/schemas/{integration}/{subject}", Scope: "write",
		Description: "Register a new schema version under a subject. Args: cluster, integration, subject=\"orders-value\", body={confirmation:true, payload:{schemaType:\"AVRO\", schema:\"...\"}}. Returns the schema id."},
	{Name: "kaflux_schema_config_update", Method: "PUT", Path: "/clusters/{cluster}/schemas/{integration}/{subject}/configuration", Scope: "write",
		Description: "Set a subject's compatibility level. Args: cluster, integration, subject, body={confirmation:true, payload:{compatibility:\"BACKWARD\"}}. Returns the new level."},
	{Name: "kaflux_schema_subject_delete", Method: "DELETE", Path: "/clusters/{cluster}/schemas/{integration}/{subject}", Scope: "destructive",
		Description: "Delete a subject and all its versions. Args: cluster, integration, subject=\"orders-value\", body={confirmation:true}. Returns the deleted version numbers."},
	{Name: "kaflux_schema_version_delete", Method: "DELETE", Path: "/clusters/{cluster}/schemas/{integration}/{subject}/versions/{version}", Scope: "destructive",
		Description: "Delete one schema version. Args: cluster, integration, subject, version=\"3\", body={confirmation:true}. Returns the deleted version number."},

	// Kafka Connect.
	{Name: "kaflux_connector_upsert", Method: "PUT", Path: "/clusters/{cluster}/connectors/{integration}/{connector}", Scope: "write",
		Description: "Create or replace a connector. Args: cluster, integration, connector=\"orders-sink\", body={confirmation:true, payload:{\"connector.class\":\"...\", \"topics\":\"orders\"}}. Returns the connector."},
	{Name: "kaflux_connector_config_update", Method: "PUT", Path: "/clusters/{cluster}/connectors/{integration}/{connector}/config", Scope: "write",
		Description: "Replace a connector's configuration; [REDACTED] values keep their current secret. Args: cluster, integration, connector, body={confirmation:true, payload:{...}}. Returns the connector."},
	{Name: "kaflux_connector_pause", Method: "POST", Path: "/clusters/{cluster}/connectors/{integration}/{connector}/pause", Scope: "write",
		Description: "Pause a connector and its tasks. Args: cluster, integration, connector=\"orders-sink\", body={confirmation:true}. Returns the request outcome."},
	{Name: "kaflux_connector_resume", Method: "POST", Path: "/clusters/{cluster}/connectors/{integration}/{connector}/resume", Scope: "write",
		Description: "Resume a paused connector. Args: cluster, integration, connector=\"orders-sink\", body={confirmation:true}. Returns the request outcome."},
	{Name: "kaflux_connector_restart", Method: "POST", Path: "/clusters/{cluster}/connectors/{integration}/{connector}/restart", Scope: "write",
		Description: "Restart a connector. Args: cluster, integration, connector=\"orders-sink\", body={confirmation:true}. Returns the request outcome."},
	{Name: "kaflux_connector_task_restart", Method: "POST", Path: "/clusters/{cluster}/connectors/{integration}/{connector}/tasks/{task}/restart", Scope: "write",
		Description: "Restart one connector task. Args: cluster, integration, connector, task=\"0\", body={confirmation:true}. Returns the request outcome."},
	{Name: "kaflux_connector_delete", Method: "DELETE", Path: "/clusters/{cluster}/connectors/{integration}/{connector}", Scope: "destructive",
		Description: "Delete a connector. Args: cluster, integration, connector=\"orders-sink\", body={confirmation:true}. Returns the request outcome."},

	// Cluster.
	{Name: "kaflux_cluster_rename", Method: "PUT", Path: "/clusters/{cluster}/name", Scope: "write",
		Description: "Change a cluster's display name; its id stays the same. Args: cluster, body={name:\"Production EU\"}. Returns the updated cluster."},

	// Messages.
	{Name: "kaflux_message_produce", Method: "POST", Path: "/clusters/{cluster}/messages", Scope: "write",
		Description: "Produce one message. Args: cluster, body={topic:\"orders\", key:\"42\", value:\"{\\\"id\\\":42}\", optional partition, headers}. Returns the written partition and offset."},

	// Rebalances.
	{Name: "kaflux_rebalance_plan", Method: "POST", Path: "/clusters/{cluster}/rebalances", Scope: "write",
		Description: "Generate a deterministic rebalance plan without touching Kafka. Args: cluster, body={topics:[\"orders\"], brokers:[1,2,3], rackAware:true, throttleBytesPerSec:10485760}. Returns the job id, planHash and proposed moves."},
	{Name: "kaflux_rebalance_dry_run", Method: "POST", Path: "/clusters/{cluster}/rebalances/{job}/dry-run", Scope: "write",
		Description: "Validate a rebalance plan against the live cluster without executing it. Args: cluster, job, body={confirmation:true, planHash:\"<from kaflux_rebalance_get>\"}. Returns the plan with warnings; nothing is mutated."},
	{Name: "kaflux_rebalance_pause", Method: "POST", Path: "/clusters/{cluster}/rebalances/{job}/pause", Scope: "destructive",
		Description: "Pause a running rebalance after the topic it is moving. Args: cluster, job, body={confirmation:true, planHash:\"...\"}. Returns the job."},
	{Name: "kaflux_rebalance_resume", Method: "POST", Path: "/clusters/{cluster}/rebalances/{job}/resume", Scope: "destructive",
		Description: "Resume a paused rebalance. Args: cluster, job, body={confirmation:true, planHash:\"...\", skip:false} (skip true leaves the current topic). Returns the job."},
	{Name: "kaflux_rebalance_cancel", Method: "POST", Path: "/clusters/{cluster}/rebalances/{job}/cancel", Scope: "destructive",
		Description: "Request durable cancellation of a rebalance; moved partitions stay where they are. Args: cluster, job, body={confirmation:true, planHash:\"...\"}. Returns the job."},
	{Name: "kaflux_rebalance_cancel_rollback", Method: "POST", Path: "/clusters/{cluster}/rebalances/{job}/cancel-rollback", Scope: "destructive",
		Description: "Cancel a running or paused rebalance and queue a rollback of the partitions it moved. Args: cluster, job, body={confirmation:true, planHash:\"...\"}. Returns the job."},
	{Name: "kaflux_rebalance_throttle", Method: "POST", Path: "/clusters/{cluster}/rebalances/{job}/throttle", Scope: "destructive",
		Description: "Change the replication throttle of a rebalance. Args: cluster, job, body={confirmation:true, planHash:\"...\", bytesPerSec:20971520}. Returns the job with a pending throttle request."},
	{Name: "kaflux_rebalance_execute", Method: "POST", Path: "/clusters/{cluster}/rebalances/{job}/execute", Scope: "destructive",
		Description: "Approve a plan by its hash and enqueue execution; topics move one at a time. Args: cluster, job, body={confirmation:true, planHash:\"<from kaflux_rebalance_get>\"}. Returns the running job."},
	{Name: "kaflux_rebalance_rollback", Method: "POST", Path: "/clusters/{cluster}/rebalances/{job}/rollback", Scope: "destructive",
		Description: "Create a validated reverse plan from the replica assignments saved before a rebalance. Args: cluster, job, body={confirmation:true, planHash:\"...\"}. Returns the rollback plan."},
}
