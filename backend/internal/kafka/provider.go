package kafka

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/model"
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

// MessageCounter is implemented by providers whose snapshots carry no
// partition offsets, so record counts are listed on demand per topic.
type MessageCounter interface {
	MessageCounts(context.Context, []string) map[string]int64
}
