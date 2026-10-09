package kafka

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Reversing a partition's replica order keeps its leader, so only an explicit
// preferred election moves leadership to the new first replica.
func TestNativePreferredElectionAndReplicaSizes(t *testing.T) {
	seed := os.Getenv("KAFLUX_TEST_KAFKA_SEED")
	if seed == "" {
		t.Skip("set KAFLUX_TEST_KAFKA_SEED for multi-broker integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers(seed))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	metadata, err := admin.Metadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata.Brokers) < 2 {
		t.Skip("two brokers required for leader election")
	}
	topic := fmt.Sprintf("kaflux-election-test-%d", time.Now().UnixNano())
	created, err := admin.CreateTopics(ctx, 2, 2, nil, topic)
	if err != nil {
		t.Fatal(err)
	}
	if err = created[topic].Err; err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		admin.DeleteTopics(cleanup, topic)
	}()
	native, err := NewNative(Config{Seeds: []string{seed}, AllowPlaintext: true})
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	original := awaitNativeTopic(t, ctx, native, topic, 2, 2)
	changes := []model.Change{}
	for _, entry := range original.Topics {
		if entry.Name != topic {
			continue
		}
		for _, p := range entry.Partitions {
			changes = append(changes, model.Change{Topic: topic, Partition: p.ID, Before: p.Replicas, After: []int32{p.Replicas[1], p.Replicas[0]}})
		}
	}
	if err = native.Reassign(ctx, changes); err != nil {
		t.Fatal(err)
	}
	for {
		pending, err := native.Pending(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) == 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err = native.ElectPreferredLeaders(ctx, changes); err != nil {
		t.Fatal(err)
	}
	if err = native.ElectPreferredLeaders(ctx, changes); err != nil {
		t.Fatalf("second election must treat ELECTION_NOT_NEEDED as success: %v", err)
	}
	snap, err := native.FreshSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range snap.Topics {
		if entry.Name != topic {
			continue
		}
		for _, p := range entry.Partitions {
			if p.Leader != p.Replicas[0] {
				t.Fatalf("partition %d leader %d, preferred %d", p.ID, p.Leader, p.Replicas[0])
			}
		}
	}
	// Sizes come from a log-dir observation cached for sizeTTL.
	deadline := time.Now().Add(sizeTTL + 5*time.Second)
	for {
		sizes := native.ReplicaSizes(ctx)
		if len(sizes[topic]) == 2 && len(sizes[topic][0]) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica sizes for %s = %v", topic, sizes[topic])
		}
		time.Sleep(time.Second)
	}
}
