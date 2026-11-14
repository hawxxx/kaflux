package model

import "time"

// UnusedTopic is a topic the unused finder reports as a deletion candidate.
type UnusedTopic struct {
	Name string `json:"name"`
	// Classification is "unused", "likely-unused" or "idle".
	Classification string   `json:"classification"`
	Reasons        []string `json:"reasons"`
	Partitions     int      `json:"partitions"`
	// EmptyPartitions lists partitions without records on an otherwise non-empty topic.
	EmptyPartitions []int32 `json:"emptyPartitions"`
	// Records is the retained record count, or -1 when the brokers did not report offsets.
	Records   int64  `json:"records"`
	SizeBytes *int64 `json:"sizeBytes"`
	// LastRecordAt is nil when the last write time is unknown.
	LastRecordAt    *time.Time `json:"lastRecordAt"`
	ConsumerGroups  []string   `json:"consumerGroups"`
	ActiveConsumers int        `json:"activeConsumers"`
	Internal        bool       `json:"internal"`
	// AgeKnown is false for topics without records: nothing shows how old they are.
	AgeKnown bool `json:"ageKnown"`
	// Fingerprint encodes "<partition>:<start>:<end>" from a fresh offsets read; bulk delete sends it back.
	Fingerprint string `json:"fingerprint"`
}

// TopicGroupUse is one consumer group that references a topic by committed
// offsets, an assignment or a subscription.
type TopicGroupUse struct {
	Group               string `json:"group"`
	State               string `json:"state"`
	Members             int    `json:"members"`
	HasCommittedOffsets bool   `json:"hasCommittedOffsets"`
	// AssignedMembers counts members assigned to or subscribed to the topic.
	AssignedMembers int `json:"assignedMembers"`
}
