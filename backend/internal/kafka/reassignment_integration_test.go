package kafka

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNativeReassignmentThrottleAndRollback(t *testing.T) {
	seed := os.Getenv("KAFLUX_TEST_KAFKA_SEED")
	if seed == "" {
		t.Skip("set KAFLUX_TEST_KAFKA_SEED for three-broker integration")
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
	if len(metadata.Brokers) < 3 {
		t.Skip("three brokers required for reassignment round trip")
	}
	topic := fmt.Sprintf("kaflux-rebalance-test-%d", time.Now().UnixNano())
	created, err := admin.CreateTopics(ctx, 3, 2, nil, topic)
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
	original := awaitNativeTopic(t, ctx, native, topic, 3, 2)
	brokerIDs := []int32{}
	for _, b := range original.Brokers {
		brokerIDs = append(brokerIDs, b.ID)
	}
	next := func(id int32) int32 {
		for i, b := range brokerIDs {
			if b == id {
				return brokerIDs[(i+1)%len(brokerIDs)]
			}
		}
		return -1
	}
	changes := []model.Change{}
	for _, entry := range original.Topics {
		if entry.Name != topic {
			continue
		}
		for _, p := range entry.Partitions {
			after := []int32{}
			for _, b := range p.Replicas {
				after = append(after, next(b))
			}
			changes = append(changes, model.Change{Topic: topic, Partition: p.ID, Before: append([]int32(nil), p.Replicas...), After: after})
		}
	}
	if len(changes) != 3 {
		t.Fatal("test topic metadata missing")
	}
	produced, err := native.Produce(ctx, model.Message{Topic: topic, Partition: 1, Key: "round-trip", Value: `{"rebalance":"preserves data"}`})
	if err != nil {
		t.Fatal(err)
	}
	configs, diagnosticErr := native.admin.DescribeBrokerConfigs(ctx, brokerIDs...)
	if diagnosticErr != nil {
		t.Fatal(diagnosticErr)
	}
	for _, resource := range configs {
		keys := []string{}
		for _, config := range resource.Configs {
			if strings.Contains(config.Key, "throttled") {
				keys = append(keys, config.Key)
			}
		}
		t.Logf("broker config resource=%q throttle keys=%v error=%v", resource.Name, keys, resource.Err)
	}
	records, err := native.PrepareThrottle(ctx, changes, 100000000)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := native.RestoreThrottle(cleanup, records); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err = native.ApplyThrottle(ctx, records); err != nil {
		t.Fatal(err)
	}
	wait := func(expected []model.Change) {
		t.Helper()
		for {
			pending, err := native.Pending(ctx)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := native.FreshSnapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			complete := 0
			for _, c := range expected {
				for _, entry := range snapshot.Topics {
					if entry.Name == c.Topic {
						for _, p := range entry.Partitions {
							if p.ID == c.Partition && reflect.DeepEqual(p.Replicas, c.After) && len(p.ISR) == len(p.Replicas) {
								complete++
							}
						}
					}
				}
			}
			if len(pending) == 0 && complete == len(expected) {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal("reassignment did not converge")
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	if err = native.Reassign(ctx, changes); err != nil {
		t.Fatal(err)
	}
	updated, err := RetargetThrottle(records, 50000000)
	if err != nil {
		t.Fatal(err)
	}
	records = updated // Keep both owned values available if the update is interrupted.
	if err = native.ApplyThrottle(ctx, records); err != nil {
		t.Fatal(err)
	}
	records = FinalizeThrottle(records)
	current, err := currentConfigs(ctx, native.admin, records)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if r.Resource == "broker" && current[r.Resource+"/"+r.Name+"/"+r.Key] != "50000000" {
			t.Fatal("updated native broker rate not verified")
		}
	}
	wait(changes)
	rollback := make([]model.Change, len(changes))
	for i, c := range changes {
		rollback[i] = model.Change{Topic: c.Topic, Partition: c.Partition, Before: c.After, After: c.Before}
	}
	if err = native.Reassign(ctx, rollback); err != nil {
		t.Fatal(err)
	}
	wait(rollback)
	messages, err := native.Messages(ctx, topic, 1, produced.Offset, 5)
	if err != nil || len(messages) == 0 || messages[0].Value != produced.Value {
		t.Fatalf("data not preserved: %v", err)
	}
	if err = native.RestoreThrottle(ctx, records); err != nil {
		t.Fatal(err)
	}
	restored, err := currentConfigs(ctx, native.admin, records)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if value := restored[r.Resource+"/"+r.Name+"/"+r.Key]; value != r.EffectiveBefore {
			t.Fatalf("throttle leaked: %s/%s/%s current=%q expected=%q", r.Resource, r.Name, r.Key, value, r.EffectiveBefore)
		}
	}
}
