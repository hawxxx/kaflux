package kafka

import (
	"context"
	"strings"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestElectionNotNeededIsSuccess(t *testing.T) {
	ok := kadm.ElectLeadersResults{"a": {0: {Topic: "a", Partition: 0}, 1: {Topic: "a", Partition: 1, Err: kerr.ElectionNotNeeded}}}
	if e := electionError(ok); e != nil {
		t.Fatalf("unexpected error %v", e)
	}
	bad := kadm.ElectLeadersResults{"a": {2: {Topic: "a", Partition: 2, Err: kerr.PreferredLeaderNotAvailable}}}
	e := electionError(bad)
	if e == nil || !strings.Contains(e.Error(), "a-2") {
		t.Fatalf("error = %v", e)
	}
}

func TestDemoElectionAndSizes(t *testing.T) {
	d := NewDemo()
	ctx := context.Background()
	snap, _ := d.Snapshot(ctx)
	topic := snap.Topics[0]
	part := topic.Partitions[0]
	after := []int32{part.Replicas[1], part.Replicas[0], part.Replicas[2]}
	if e := d.Reassign(ctx, []model.Change{{Topic: topic.Name, Partition: part.ID, After: after}}); e != nil {
		t.Fatal(e)
	}
	d.mu.Lock()
	d.state.Topics[0].Partitions[0].Leader = after[1]
	d.mu.Unlock()
	if e := d.ElectPreferredLeaders(ctx, []model.Change{{Topic: topic.Name, Partition: part.ID}}); e != nil {
		t.Fatal(e)
	}
	snap, _ = d.Snapshot(ctx)
	if snap.Topics[0].Partitions[0].Leader != after[0] {
		t.Fatalf("leader = %d, want %d", snap.Topics[0].Partitions[0].Leader, after[0])
	}
	sizes := d.ReplicaSizes(ctx)
	if got := sizes[topic.Name][part.ID][after[2]]; got != *part.SizeBytes {
		t.Fatalf("replica size = %d", got)
	}
}
