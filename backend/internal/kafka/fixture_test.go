package kafka

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"testing"
	"time"
)

// Controller acknowledgements precede broker metadata propagation. Wait for
// elected leaders and their expected ISR before producing or reassigning.
func awaitNativeTopic(t *testing.T, ctx context.Context, n *Native, name string, count, replicas int) model.Snapshot {
	t.Helper()
	for {
		snapshot, err := n.FreshSnapshot(ctx)
		if err == nil {
			for _, topic := range snapshot.Topics {
				if topic.Name != name || len(topic.Partitions) != count {
					continue
				}
				ready := true
				for _, p := range topic.Partitions {
					if p.Leader < 0 || len(p.ISR) != replicas {
						ready = false
					}
				}
				if ready {
					return snapshot
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("topic metadata did not converge: %v", ctx.Err())
			return model.Snapshot{}
		case <-time.After(100 * time.Millisecond):
		}
	}
}
