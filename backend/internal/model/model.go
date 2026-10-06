package model

import (
	"encoding/json"
	"time"
)

type Cluster struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ConfiguredName string `json:"configuredName"`
	Environment    string `json:"environment"`
	Kind           string `json:"kind"`
	Mode           string `json:"mode"`
	State          string `json:"state"`
	BrokerCount    int    `json:"brokerCount"`
	TopicCount     int    `json:"topicCount"`
	PartitionCount int    `json:"partitionCount"`
}
type Broker struct {
	ID         int32  `json:"id"`
	Host       string `json:"host"`
	Port       int32  `json:"port"`
	Rack       string `json:"rack"`
	Partitions int    `json:"partitions"`
	Leaders    int    `json:"leaders"`
	SizeBytes  *int64 `json:"sizeBytes"`
}
type Partition struct {
	ID          int32   `json:"id"`
	Leader      int32   `json:"leader"`
	Replicas    []int32 `json:"replicas"`
	ISR         []int32 `json:"isr"`
	StartOffset *int64  `json:"startOffset"`
	EndOffset   *int64  `json:"endOffset"`
	SizeBytes   *int64  `json:"sizeBytes"`
}
type Topic struct {
	Name              string      `json:"name"`
	Partitions        []Partition `json:"partitionDetails"`
	ReplicationFactor int         `json:"replicationFactor"`
	SizeBytes         *int64      `json:"sizeBytes"`
	URP               int         `json:"urp"`
	CleanupPolicy     string      `json:"cleanupPolicy"`
	RetentionMs       *int64      `json:"retentionMs"`
	ObservedAt        time.Time   `json:"observedAt"`
}
type Snapshot struct {
	Brokers    []Broker  `json:"brokers"`
	Topics     []Topic   `json:"topics"`
	ObservedAt time.Time `json:"observedAt"`
	Controller *int32    `json:"controller"`
}
type Group struct {
	ID      string   `json:"id"`
	State   string   `json:"state"`
	Members int      `json:"members"`
	Topics  []string `json:"topics"`
	Lag     *int64   `json:"lag"`
}
type Header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type Message struct {
	Topic            string          `json:"topic"`
	Partition        int32           `json:"partition"`
	Offset           int64           `json:"offset"`
	Key              string          `json:"key"`
	Value            string          `json:"value"`
	ValueBase64      string          `json:"valueBase64"`
	DecodedValue     json.RawMessage `json:"decodedValue,omitempty"`
	DecodedFormat    string          `json:"decodedFormat,omitempty"`
	DecodedKey       json.RawMessage `json:"decodedKey,omitempty"`
	KeyDecodedFormat string          `json:"keyDecodedFormat,omitempty"`
	KeySchemaID      uint32          `json:"keySchemaId,omitempty"`
	KeyDecodeError   string          `json:"keyDecodeError,omitempty"`
	SchemaID         uint32          `json:"schemaId,omitempty"`
	DecodeError      string          `json:"decodeError,omitempty"`
	KeyBase64        string          `json:"keyBase64"`
	Truncated        bool            `json:"truncated"`
	Timestamp        time.Time       `json:"timestamp"`
	Headers          []Header        `json:"headers"`
}
type Change struct {
	Topic     string  `json:"topic"`
	Partition int32   `json:"partition"`
	Before    []int32 `json:"before"`
	After     []int32 `json:"after"`
}
type Distribution struct {
	Broker   int32 `json:"broker"`
	Replicas int   `json:"replicas"`
	Leaders  int   `json:"leaders"`
}
type Plan struct {
	ThrottleRequest       *ThrottleRequest `json:"throttleRequest,omitempty"`
	ThrottleError         string           `json:"throttleError,omitempty"`
	ID                    string           `json:"id"`
	ClusterID             string           `json:"clusterId"`
	State                 string           `json:"state"`
	PlanHash              string           `json:"planHash"`
	Fingerprint           string           `json:"fingerprint"`
	Topics                []string         `json:"topics"`
	Changes               []Change         `json:"changes"`
	Before                []Distribution   `json:"before"`
	After                 []Distribution   `json:"after"`
	CreatedAt             time.Time        `json:"createdAt"`
	Actor                 string           `json:"actor"`
	ApprovedBy            string           `json:"approvedBy,omitempty"`
	Error                 string           `json:"error,omitempty"`
	Progress              int              `json:"progress"`
	ThrottleBytesPerSec   int64            `json:"throttleBytesPerSec"`
	EstimatedBytes        *int64           `json:"estimatedBytes"`
	CapacityKnown         bool             `json:"capacityKnown"`
	StartedAt             time.Time        `json:"startedAt,omitempty"`
	CleanupPending        bool             `json:"cleanupPending"`
	TerminalState         string           `json:"terminalState,omitempty"`
	TerminalError         string           `json:"terminalError,omitempty"`
	CancellationRequested bool             `json:"cancellationRequested"`
	RackAware             bool             `json:"rackAware"`
}

type ThrottleRequest struct {
	Revision    string    `json:"revision"`
	BytesPerSec int64     `json:"bytesPerSec"`
	Actor       string    `json:"actor"`
	Provider    string    `json:"provider"`
	At          time.Time `json:"at"`
}
