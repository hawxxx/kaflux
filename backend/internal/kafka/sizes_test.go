package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
)

func logDirIn(broker int32, dir string, err error, parts ...kadm.DescribedLogDirPartition) kadm.DescribedLogDir {
	d := kadm.DescribedLogDir{Broker: broker, Dir: dir, Err: err, Topics: kadm.DescribedLogDirTopics{}}
	for _, p := range parts {
		p.Broker, p.Dir = broker, dir
		if d.Topics[p.Topic] == nil {
			d.Topics[p.Topic] = map[int32]kadm.DescribedLogDirPartition{}
		}
		d.Topics[p.Topic][p.Partition] = p
	}
	return d
}

func logDir(broker int32, err error, parts ...kadm.DescribedLogDirPartition) kadm.DescribedLogDir {
	return logDirIn(broker, "/data", err, parts...)
}

func part(topic string, id int32, size int64) kadm.DescribedLogDirPartition {
	return kadm.DescribedLogDirPartition{Topic: topic, Partition: id, Size: size}
}

func snapshotFixture() model.Snapshot {
	return model.Snapshot{
		Brokers: []model.Broker{{ID: 1}, {ID: 2}, {ID: 3}},
		Topics: []model.Topic{{Name: "orders", Partitions: []model.Partition{
			{ID: 0, Leader: 1, Replicas: []int32{1, 2}},
			{ID: 1, Leader: 2, Replicas: []int32{2, 3}},
		}}},
	}
}

func TestTopicSizeIsTheSumOfAllReplicas(t *testing.T) {
	all := kadm.DescribedAllLogDirs{
		1: {"/data": logDir(1, nil, part("orders", 0, 100), part("__hidden", 0, 7))},
		2: {"/data": logDir(2, nil, part("orders", 0, 90), part("orders", 1, 50))},
		3: {"/data": logDir(3, nil, part("orders", 1, 50))},
	}
	snap := snapshotFixture()
	collectLogSizes(all, time.Unix(0, 0)).apply(&snap)

	orders := snap.Topics[0]
	if orders.SizeBytes == nil || *orders.SizeBytes != 290 {
		t.Fatalf("topic size = %v, want 290 (100+90+50+50, every replica)", orders.SizeBytes)
	}
	// A partition stays one replica's worth, because the balance analysis and
	// the reassignment planner multiply it by the replicas they place.
	if orders.Partitions[0].SizeBytes == nil || *orders.Partitions[0].SizeBytes != 100 {
		t.Fatalf("partition 0 must use the leader replica (100), got %v", orders.Partitions[0].SizeBytes)
	}
	if orders.Partitions[1].SizeBytes == nil || *orders.Partitions[1].SizeBytes != 50 {
		t.Fatalf("partition 1 = %v, want 50", orders.Partitions[1].SizeBytes)
	}
	wantBroker := map[int32]int64{1: 107, 2: 140, 3: 50}
	for _, b := range snap.Brokers {
		if b.SizeBytes == nil || *b.SizeBytes != wantBroker[b.ID] {
			t.Fatalf("broker %d size = %v, want %d (includes the hidden topic)", b.ID, b.SizeBytes, wantBroker[b.ID])
		}
	}
}

func TestFutureCopyOccupiesDiskButIsNotAPartitionReplica(t *testing.T) {
	future := part("orders", 0, 999)
	future.IsFuture = true
	all := kadm.DescribedAllLogDirs{
		1: {"/data": logDir(1, nil, part("orders", 0, 100))},
		2: {"/data": logDir(2, nil, part("orders", 0, 90), part("orders", 1, 50)), "/data2": logDirIn(2, "/data2", nil, future)},
		3: {"/data": logDir(3, nil, part("orders", 1, 50))},
	}
	snap := snapshotFixture()
	collectLogSizes(all, time.Unix(0, 0)).apply(&snap)
	if got := *snap.Topics[0].Partitions[0].SizeBytes; got != 100 {
		t.Fatalf("partition 0 = %d, a directory-move copy must not change the replica size", got)
	}
	if got := *snap.Brokers[1].SizeBytes; got != 90+50+999 {
		t.Fatalf("broker 2 = %d, the copy occupies disk and counts as physical", got)
	}
	if got := *snap.Topics[0].SizeBytes; got != 100+90+999+50+50 {
		t.Fatalf("topic = %d, all bytes on disk count, every log dir entry is summed", got)
	}
}

func TestSizesUnknownWhenABrokerDidNotReportOrADirFailed(t *testing.T) {
	all := kadm.DescribedAllLogDirs{
		1: {"/data": logDir(1, nil, part("orders", 0, 100))},
		2: {"/data": logDir(2, errors.New("KAFKA_STORAGE_ERROR"), part("orders", 1, 50))},
		// broker 3 did not answer at all
	}
	snap := snapshotFixture()
	collectLogSizes(all, time.Unix(0, 0)).apply(&snap)
	if snap.Brokers[0].SizeBytes == nil {
		t.Fatal("healthy broker 1 must have a size")
	}
	if snap.Brokers[1].SizeBytes != nil || snap.Brokers[2].SizeBytes != nil {
		t.Fatalf("broker with a failed dir or no answer must stay unknown: %v %v", snap.Brokers[1].SizeBytes, snap.Brokers[2].SizeBytes)
	}
	orders := snap.Topics[0]
	if orders.SizeBytes != nil {
		t.Fatalf("replicas of partition 0 (broker 2) and 1 (brokers 2, 3) are missing, a partial total would understate the topic: %d", *orders.SizeBytes)
	}
	if orders.Partitions[0].SizeBytes == nil {
		t.Fatal("partition 0 was reported by its leader")
	}
	// Partition 1: its leader (broker 2) has a failed directory, whose data is
	// not trusted, and the only other replica (broker 3) never answered.
	if orders.Partitions[1].SizeBytes != nil {
		t.Fatalf("no replica of partition 1 reported, so it must be unknown: %v", orders.Partitions[1].SizeBytes)
	}
}

func TestPartitionFallsBackToAFollowerWhenTheLeaderDidNotReport(t *testing.T) {
	all := kadm.DescribedAllLogDirs{
		// broker 1 (leader of partition 0) did not answer; broker 2 is a follower.
		2: {"/data": logDir(2, nil, part("orders", 0, 90), part("orders", 1, 50))},
		3: {"/data": logDir(3, nil, part("orders", 1, 60))},
	}
	snap := snapshotFixture()
	collectLogSizes(all, time.Unix(0, 0)).apply(&snap)
	orders := snap.Topics[0]
	if orders.Partitions[0].SizeBytes == nil || *orders.Partitions[0].SizeBytes != 90 {
		t.Fatalf("partition 0 should use the follower that reported (90): %v", orders.Partitions[0].SizeBytes)
	}
	// Partition 1's leader (broker 2) reported 50, so the larger follower (60) is not used.
	if orders.Partitions[1].SizeBytes == nil || *orders.Partitions[1].SizeBytes != 50 {
		t.Fatalf("partition 1 should use its leader replica (50): %v", orders.Partitions[1].SizeBytes)
	}
	if orders.SizeBytes != nil {
		t.Fatal("broker 1 never answered, so the topic total is incomplete and must be unknown")
	}
	if snap.Brokers[0].SizeBytes != nil {
		t.Fatal("broker 1 never answered and must stay unknown")
	}
}

func TestNilLogSizesLeavesSnapshotUntouched(t *testing.T) {
	snap := snapshotFixture()
	(*logSizes)(nil).apply(&snap)
	if snap.Brokers[0].SizeBytes != nil || snap.Topics[0].SizeBytes != nil {
		t.Fatal("unknown sizes must stay nil")
	}
}

func TestStaleSizesServedOnlyWithinMaxAge(t *testing.T) {
	n := &Native{}
	now := time.Now()
	n.sizeCache = &logSizes{observedAt: now.Add(-sizeMaxAge + time.Second)}
	if n.staleSizes(now) == nil {
		t.Fatal("recent observation should be served after a failed refresh")
	}
	n.sizeCache = &logSizes{observedAt: now.Add(-sizeMaxAge - time.Second)}
	if n.staleSizes(now) != nil {
		t.Fatal("observation older than sizeMaxAge must revert to unknown")
	}
}
