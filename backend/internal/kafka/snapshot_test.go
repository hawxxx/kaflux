package kafka

import (
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"testing"
	"time"
)

func TestSnapshotKeepsDegradedPartitionsAndSkipsFailedTopics(t *testing.T) {
	m := kadm.Metadata{
		Controller: 1,
		Brokers:    kadm.BrokerDetails{{NodeID: 1}, {NodeID: 2}, {NodeID: 3}},
		Topics: kadm.TopicDetails{
			"orders": {Topic: "orders", Partitions: kadm.PartitionDetails{
				0: {Partition: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
				1: {Partition: 1, Leader: 2, Replicas: []int32{2, 3, 1}, ISR: []int32{2, 1}, Err: kerr.ReplicaNotAvailable},
				2: {Partition: 2, Leader: -1, Replicas: []int32{3}, ISR: []int32{}, Err: kerr.LeaderNotAvailable},
			}},
			"creating": {Topic: "creating", Err: kerr.LeaderNotAvailable},
		},
	}
	s := snapshotFromMetadata(m, time.Unix(0, 0))
	if len(s.Topics) != 1 || s.Topics[0].Name != "orders" {
		t.Fatalf("topics = %+v", s.Topics)
	}
	orders := s.Topics[0]
	if len(orders.Partitions) != 3 || orders.URP != 2 || orders.ReplicationFactor != 3 {
		t.Fatalf("orders = %+v", orders)
	}
	if orders.Partitions[2].Leader != -1 {
		t.Fatalf("offline partition leader = %d", orders.Partitions[2].Leader)
	}
	leaders := map[int32]int{}
	replicas := map[int32]int{}
	for _, b := range s.Brokers {
		leaders[b.ID], replicas[b.ID] = b.Leaders, b.Partitions
	}
	if leaders[1] != 1 || leaders[2] != 1 || leaders[3] != 0 || replicas[3] != 3 {
		t.Fatalf("leaders = %v replicas = %v", leaders, replicas)
	}
}
