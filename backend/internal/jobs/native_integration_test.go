package jobs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/balance"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Runs a throttled two-topic rebalance end to end on a real cluster: topics in
// order, throttle restored, preferred leaders elected, activity logged.
func TestNativeTopicByTopicRebalance(t *testing.T) {
	seed := os.Getenv("KAFLUX_TEST_KAFKA_SEED")
	if seed == "" {
		t.Skip("set KAFLUX_TEST_KAFKA_SEED for three-broker integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers(seed))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	suffix := time.Now().UnixNano()
	topics := []string{fmt.Sprintf("kaflux-steps-b-%d", suffix), fmt.Sprintf("kaflux-steps-a-%d", suffix)}
	for _, topic := range topics {
		created, err := admin.CreateTopics(ctx, 6, 2, nil, topic)
		if err == nil {
			err = created[topic].Err
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	defer admin.DeleteTopics(context.Background(), topics...)
	native, err := kafka.NewNative(kafka.Config{Seeds: []string{seed}, AllowPlaintext: true})
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	s, err := store.NewSQLite(ctx, filepath.Join(t.TempDir(), "kaflux.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var snapErr error
	for i := 0; i < 50; i++ {
		snap, e := FreshSnapshot(ctx, native)
		snapErr = e
		found := 0
		for _, tp := range snap.Topics {
			for _, want := range topics {
				if tp.Name == want && len(tp.Partitions) > 0 && tp.Partitions[0].Leader >= 0 {
					found++
				}
			}
		}
		if e == nil && found == len(topics) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if snapErr != nil {
		t.Fatal(snapErr)
	}
	// Squeeze every replica onto the first two brokers so a plan over all brokers moves data.
	snap, _ := FreshSnapshot(ctx, native)
	if len(snap.Brokers) < 3 {
		t.Skip("three brokers required")
	}
	pair := []int32{snap.Brokers[0].ID, snap.Brokers[1].ID}
	squeeze := []model.Change{}
	for _, tp := range snap.Topics {
		for _, want := range topics {
			if tp.Name == want {
				for _, x := range tp.Partitions {
					after := pair
					if x.ID%2 == 1 {
						after = []int32{pair[1], pair[0]}
					}
					squeeze = append(squeeze, model.Change{Topic: tp.Name, Partition: x.ID, After: after})
				}
			}
		}
	}
	if err = native.Reassign(ctx, squeeze); err != nil {
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
		time.Sleep(300 * time.Millisecond)
	}
	// The new first replica may still be catching up into the ISR.
	for i := 0; ; i++ {
		if err = native.ElectPreferredLeaders(ctx, squeeze); err == nil {
			break
		}
		if i == 30 {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	var p model.Plan
	for i := 0; ; i++ {
		snap, _ = FreshSnapshot(ctx, native)
		if p, err = balance.Generate(snap, balance.Request{Topics: topics, ThrottleBytesPerSec: 50 << 20}); err == nil {
			break
		}
		if i == 30 {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if len(p.Changes) == 0 {
		t.Skip("cluster placement is already balanced for the test topics")
	}
	p.ID, p.ClusterID, p.CreatedAt = "native-steps", "it", time.Now()
	if err = s.SaveJob(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err = Approve(ctx, s, p, "it", p.PlanHash); err != nil {
		t.Fatal(err)
	}
	w := &Worker{Store: s, Providers: map[string]kafka.Provider{"it": native}, Owner: "it"}
	for {
		w.Step(ctx)
		job, _ := s.Job(ctx, p.ID)
		if job.State == "completed" {
			break
		}
		if job.State != "running" && job.State != "queued" {
			events, _ := s.Events(ctx, p.ID, 0, 100)
			t.Fatalf("job %s (%s / %s): %v", job.State, job.Error, job.PauseReason, events)
		}
		if ctx.Err() != nil {
			t.Fatal("timed out")
		}
		time.Sleep(time.Second)
	}
	if records, _ := s.LoadThrottle(ctx, p.ID); len(records) != 0 {
		t.Fatalf("throttle not restored: %v", records)
	}
	events, _ := s.Events(ctx, p.ID, 0, 1000)
	log := []string{}
	for _, e := range events {
		log = append(log, e.Message)
	}
	text := strings.Join(log, "\n")
	first, second := strings.Index(text, "▶ "+p.Steps[0].Topic), strings.Index(text, "▶ "+p.Steps[len(p.Steps)-1].Topic)
	if first < 0 || second < first || !strings.Contains(text, "⇄ Preferred leader election") || !strings.Contains(text, "Throttle removed") {
		t.Fatalf("unexpected log:\n%s", text)
	}
	after, _ := FreshSnapshot(ctx, native)
	for _, tp := range after.Topics {
		for _, want := range topics {
			if tp.Name != want {
				continue
			}
			for _, x := range tp.Partitions {
				if x.Leader != x.Replicas[0] {
					t.Fatalf("%s-%d leader %d is not preferred %d", tp.Name, x.ID, x.Leader, x.Replicas[0])
				}
			}
		}
	}
}
