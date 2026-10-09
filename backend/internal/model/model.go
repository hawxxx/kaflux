package model

import (
	"encoding/json"
	"strings"
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
	// RequireThrottle rejects rebalance execution without a replication throttle.
	RequireThrottle bool `json:"requireThrottle"`
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
	// Internal is set when the broker reports the topic as internal, such as
	// __consumer_offsets. IsInternal also covers underscore-prefixed names.
	Internal bool `json:"internal"`
}

// IsInternal reports topics owned by Kafka or its ecosystem rather than
// applications: broker-internal topics and, by convention, names starting with
// an underscore (_schemas, __transaction_state, _confluent-*).
func (t Topic) IsInternal() bool { return t.Internal || strings.HasPrefix(t.Name, "_") }

// MessageCount sums end minus start offsets over the partitions. It is nil
// unless every partition reports both offsets. Compaction and transaction
// markers make it an upper bound of the records actually retained.
func (t Topic) MessageCount() *int64 {
	if len(t.Partitions) == 0 {
		return nil
	}
	var total int64
	for _, p := range t.Partitions {
		if p.StartOffset == nil || p.EndOffset == nil {
			return nil
		}
		total += max(*p.EndOffset-*p.StartOffset, 0)
	}
	return &total
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
	// FinishedAt is set by the worker when the partition first reaches its target.
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
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

	// Steps run one topic at a time in the order the user chose; CurrentStep is the checkpoint.
	Steps               []TopicStep `json:"steps,omitempty"`
	CurrentStep         int         `json:"currentStep"`
	PauseRequested      bool        `json:"pauseRequested"`
	PauseRequestedBy    string      `json:"pauseRequestedBy,omitempty"`
	PauseReason         string      `json:"pauseReason,omitempty"`
	RollbackRequested   bool        `json:"rollbackRequested"`
	RollbackRequestedBy string      `json:"rollbackRequestedBy,omitempty"`
	RollbackOf          string      `json:"rollbackOf,omitempty"`
	RollbackJob         string      `json:"rollbackJob,omitempty"`
	HealthWaitSince     *time.Time  `json:"healthWaitSince,omitempty"`
	Warnings            []string    `json:"warnings,omitempty"`
	PartitionsDone      int         `json:"partitionsDone"`
	PartitionsTotal     int         `json:"partitionsTotal"`
	BytesTotal          *int64      `json:"bytesTotal,omitempty"`
	BytesDone           *int64      `json:"bytesDone,omitempty"`
	RateBytesPerSec     *int64      `json:"rateBytesPerSec,omitempty"`
	ETASeconds          *int64      `json:"etaSeconds,omitempty"`
	ETABasis            string      `json:"etaBasis,omitempty"`
	ProgressAt          *time.Time  `json:"progressAt,omitempty"`
	ProgressLoggedAt    *time.Time  `json:"progressLoggedAt,omitempty"`
	FinishedAt          *time.Time  `json:"finishedAt,omitempty"`
}

// Step states of a TopicStep.
const (
	StepPending  = "pending"
	StepMoving   = "moving"
	StepElecting = "electing"
	StepDone     = "done"
	StepSkipped  = "skipped"
	StepFailed   = "failed"
)

// TopicStep is one topic of a rebalance, moved in a single reassignment.
type TopicStep struct {
	Topic            string     `json:"topic"`
	State            string     `json:"state"`
	Partitions       int        `json:"partitions"`
	PartitionsDone   int        `json:"partitionsDone"`
	Bytes            *int64     `json:"bytes,omitempty"`
	BytesDone        *int64     `json:"bytesDone,omitempty"`
	StartedAt        *time.Time `json:"startedAt,omitempty"`
	FinishedAt       *time.Time `json:"finishedAt,omitempty"`
	Error            string     `json:"error,omitempty"`
	ElectionAttempts int        `json:"electionAttempts,omitempty"`
}

// ActiveJobStates hold the per-cluster lock: no other job may start and the job cannot be deleted.
// rollback-queued is a legacy state kept so older rows stay protected.
var ActiveJobStates = []string{"queued", "running", "paused", "rollback-queued"}

// WorkerJobStates are the states the worker claims and advances. A paused job makes no Kafka calls.
var WorkerJobStates = []string{"queued", "running"}

func IsActiveJobState(state string) bool {
	for _, s := range ActiveJobStates {
		if s == state {
			return true
		}
	}
	return false
}

// JobEvent is one line of a job's activity log.
type JobEvent struct {
	Seq     int64     `json:"seq"`
	At      time.Time `json:"at"`
	Level   string    `json:"level"`
	Message string    `json:"message"`
}

type ThrottleRequest struct {
	Revision    string    `json:"revision"`
	BytesPerSec int64     `json:"bytesPerSec"`
	Actor       string    `json:"actor"`
	Provider    string    `json:"provider"`
	At          time.Time `json:"at"`
}
