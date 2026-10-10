package kafka

import (
	"context"
	"errors"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"time"
)

type Provider interface {
	Snapshot(context.Context) (model.Snapshot, error)
	Groups(context.Context) ([]model.Group, error)
	Messages(context.Context, string, int32, int64, int) ([]model.Message, error)
	Produce(context.Context, model.Message) (model.Message, error)
	Reassign(context.Context, []model.Change) error
	Pending(context.Context) (map[string][]int32, error)
	Close()
}

// ErrUnknownPartition reports a partition the topic does not have.
var ErrUnknownPartition = errors.New("unknown partition")

// MultiReader reads several partitions of one topic through a single consumer,
// so a topic-wide view costs one connection instead of one per partition.
type MultiReader interface {
	// MessagesAt reads up to limit records from each partition, starting at its offset.
	MessagesAt(ctx context.Context, topic string, from map[int32]int64, limit int) ([]model.Message, error)
	// OffsetsAt returns, per partition, the first offset at or after the time.
	OffsetsAt(ctx context.Context, topic string, at time.Time) (map[int32]int64, error)
}

// MessageCounter is implemented by providers whose snapshots carry no
// partition offsets, so record counts are listed on demand per topic.
type MessageCounter interface {
	MessageCounts(context.Context, []string) map[string]int64
}
