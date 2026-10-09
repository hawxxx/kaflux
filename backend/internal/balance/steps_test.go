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

func TestDistributionsCountZeroBrokersAndUnknownSizes(t *testing.T) {
	size := int64(10)
	snap := model.Snapshot{Topics: []model.Topic{
		{Name: "a", Partitions: []model.Partition{{ID: 0, Replicas: []int32{1, 2}, SizeBytes: &size}, {ID: 1, Replicas: []int32{2, 1}, SizeBytes: &size}}},
		{Name: "other", Partitions: []model.Partition{{ID: 0, Replicas: []int32{3}}}},
	}}
	d := Distributions(snap, []string{"a"}, []int32{1, 2, 3})
	if len(d) != 3 || d[2].Broker != 3 || d[2].Replicas != 0 || d[0].Leaders != 1 || *d[0].Bytes != 20 || *d[2].Bytes != 0 {
		t.Fatalf("distributions = %+v", d)
	}
	snap.Topics[0].Partitions[1].SizeBytes = nil
	if d = Distributions(snap, []string{"a"}, nil); d[0].Bytes != nil {
		t.Fatal("unknown size must leave bytes nil")
	}
}

func TestPlanCarriesBytesBeforeAndAfter(t *testing.T) {
	size := int64(100)
	snap := model.Snapshot{Brokers: []model.Broker{{ID: 1}, {ID: 2}, {ID: 3}}}
	topic := model.Topic{Name: "a"}
	for i := int32(0); i < 6; i++ {
		topic.Partitions = append(topic.Partitions, model.Partition{ID: i, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}, SizeBytes: &size})
	}
	snap.Topics = []model.Topic{topic}
	p, e := Generate(snap, Request{Topics: []string{"a"}})
	if e != nil {
		t.Fatal(e)
	}
	total := func(ds []model.Distribution) (n int64) {
		for _, d := range ds {
			if d.Bytes == nil {
				t.Fatalf("bytes missing on broker %d", d.Broker)
			}
			n += *d.Bytes
		}
		return n
	}
	if total(p.Before) != 1200 || total(p.After) != 1200 {
		t.Fatalf("bytes before %d after %d, want 1200 each", total(p.Before), total(p.After))
	}
}
