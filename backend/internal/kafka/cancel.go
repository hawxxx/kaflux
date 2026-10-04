package kafka

import (
	"context"

	"github.com/hawxxx/kaflux/backend/internal/model"
)

type CancellationProvider interface {
	CancelReassignment(context.Context, []model.Change) error
}

// Nil replica sets are Kafka's native cancellation operation, not a reverse plan.
func (n *Native) CancelReassignment(ctx context.Context, changes []model.Change) error {
	cancellations := make([]model.Change, len(changes))
	for i, change := range changes {
		cancellations[i] = model.Change{Topic: change.Topic, Partition: change.Partition, After: nil}
	}
	return n.Reassign(ctx, cancellations)
}

func (d *Demo) CancelReassignment(context.Context, []model.Change) error { return nil }
