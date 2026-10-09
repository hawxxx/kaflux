package balance

import (
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/model"
)

func TestStepsFollowRequestedOrderAndCountNewReplicaBytes(t *testing.T) {
	size := int64(100)
	snap := model.Snapshot{Topics: []model.Topic{
		{Name: "a", Partitions: []model.Partition{{ID: 0, SizeBytes: &size}, {ID: 1, SizeBytes: &size}}},
		{Name: "b", Partitions: []model.Partition{{ID: 0}}},
		{Name: "c", Partitions: []model.Partition{{ID: 0, SizeBytes: &size}}},
	}}
	changes := []model.Change{
		{Topic: "a", Partition: 0, Before: []int32{1, 2}, After: []int32{3, 4}},
		{Topic: "a", Partition: 1, Before: []int32{1, 2}, After: []int32{1, 3}},
		{Topic: "b", Partition: 0, Before: []int32{1}, After: []int32{2}},
	}
	steps := Steps([]string{"c", "b", "a"}, changes, snap)
	if len(steps) != 2 || steps[0].Topic != "b" || steps[1].Topic != "a" {
		t.Fatalf("steps = %+v, want b then a and no step for c", steps)
	}
	if steps[0].Bytes != nil {
		t.Fatalf("unknown size must stay nil, got %d", *steps[0].Bytes)
	}
	if steps[1].Partitions != 2 || steps[1].Bytes == nil || *steps[1].Bytes != 300 {
		t.Fatalf("a = %+v, want 2 partitions and 300 bytes", steps[1])
	}
	for _, s := range steps {
		if s.State != model.StepPending {
			t.Fatalf("state = %s", s.State)
		}
	}
}
