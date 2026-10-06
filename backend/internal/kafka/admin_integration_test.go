package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"testing"
	"time"
)

func TestNativeAdministrationIntegration(t *testing.T) {
	seed := os.Getenv("KAFLUX_TEST_KAFKA_SEED")
	if seed == "" {
		t.Skip("set KAFLUX_TEST_KAFKA_SEED for real Kafka integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	p, e := NewNative(Config{Seeds: []string{seed}, AllowPlaintext: true})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	topic := fmt.Sprintf("kaflux-admin-%d", time.Now().UnixNano())
	group := topic + "-group"
	if e = p.CreateTopic(ctx, model.TopicCreate{Name: topic, Partitions: 1, ReplicationFactor: 1, Config: map[string]string{"retention.ms": "60000"}}); e != nil {
		t.Fatal(e)
	}
	defer p.admin.DeleteGroups(context.Background(), group)
	defer p.DeleteTopic(context.Background(), topic)
	var cfg map[string]string
	for attempt := 0; attempt < 50; attempt++ {
		cfg, e = p.TopicConfig(ctx, topic)
		if e == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if e != nil || cfg["retention.ms"] != "60000" {
		t.Fatalf("config create %v %v", cfg, e)
	}
	if e = p.AlterTopicConfig(ctx, topic, map[string]string{"retention.ms": "120000"}); e != nil {
		t.Fatal(e)
	}
	for attempt := 0; attempt < 50; attempt++ {
		cfg, e = p.TopicConfig(ctx, topic)
		if e == nil && cfg["retention.ms"] == "120000" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if e != nil || cfg["retention.ms"] != "120000" {
		t.Fatalf("config alter %v %v", cfg, e)
	}
	for attempt := 0; attempt < 50; attempt++ {
		e = p.IncreasePartitions(ctx, topic, 2)
		if e == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if e != nil {
		t.Fatal(e)
	}
	if e = p.IncreasePartitions(ctx, topic, 2); e == nil {
		t.Fatal("nonincrease accepted")
	}
	awaitNativeTopic(t, ctx, p, topic, 2, 1)
	for i := 0; i < 3; i++ {
		if _, e = p.Produce(ctx, model.Message{Topic: topic, Partition: 0, Value: "sanitized"}); e != nil {
			t.Fatal(e)
		}
	}
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: 1, LeaderEpoch: -1})
	result, e := p.admin.CommitOffsets(ctx, group, offsets)
	if e != nil {
		t.Fatal(e)
	}
	if e = result.Error(); e != nil {
		t.Fatal(e)
	}
	detail, e := p.GroupDetail(ctx, group)
	if e != nil || detail.Members != 0 || len(detail.Offsets) != 1 || detail.Offsets[0].Lag != 2 {
		t.Fatalf("lag detail %v %+v", e, detail)
	}
	consumers, e := p.TopicConsumers(ctx, topic)
	if e != nil || len(consumers) != 1 || consumers[0].ID != group || len(consumers[0].Offsets) != 1 || *consumers[0].Lag != 2 {
		t.Fatalf("topic consumers %v %+v", e, consumers)
	}
	for _, mode := range []string{"earliest", "latest", "absolute", "shift", "timestamp"} {
		req := model.OffsetReset{Mode: mode, Offset: 1, Shift: -1, Timestamp: time.Now().Add(-time.Hour)}
		preview, e := p.PreviewOffsets(ctx, group, req)
		if e != nil {
			t.Fatalf("preview %s: %v", mode, e)
		}
		if len(preview.Changes) != 1 {
			t.Fatal("missing offset preview")
		}
	}
	req := model.OffsetReset{Mode: "latest"}
	preview, e := p.PreviewOffsets(ctx, group, req)
	if e != nil {
		t.Fatal(e)
	}
	req.PreviewHash = "stale"
	req.Confirmation = group
	if _, e = p.ResetOffsets(ctx, group, req); e == nil {
		t.Fatal("stale preview accepted")
	}
	req.PreviewHash = preview.PreviewHash
	applied, e := p.ResetOffsets(ctx, group, req)
	if e != nil || !applied.Applied {
		t.Fatalf("reset %v %+v", e, applied)
	}
	detail, e = p.GroupDetail(ctx, group)
	if e != nil || detail.Offsets[0].CommittedOffset != 3 || detail.Offsets[0].Lag != 0 {
		t.Fatalf("committed reset %v %+v", e, detail)
	}
	consumer, e := kgo.NewClient(kgo.SeedBrokers(seed), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.DisableAutoCommit())
	if e != nil {
		t.Fatal(e)
	}
	consumeCtx, stop := context.WithCancel(ctx)
	finished := make(chan struct{})
	go func() { defer close(finished); consumer.PollRecords(consumeCtx, 1) }()
	active := false
	for attempt := 0; attempt < 100; attempt++ {
		current, err := p.GroupDetail(ctx, group)
		if err == nil && current.Members > 0 {
			active = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !active {
		stop()
		consumer.Close()
		<-finished
		t.Fatal("consumer did not join group")
	}
	if _, err := p.PreviewOffsets(ctx, group, model.OffsetReset{Mode: "earliest"}); !errors.Is(err, ErrActiveGroup) {
		stop()
		consumer.Close()
		<-finished
		t.Fatalf("active reset accepted: %v", err)
	}
	stop()
	consumer.Close()
	<-finished
	if e = p.DeleteTopic(ctx, topic); e != nil {
		t.Fatal(e)
	}
	for attempt := 0; attempt < 50; attempt++ {
		_, e = p.TopicDetail(ctx, topic)
		if e != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if e == nil {
		t.Fatal("deleted topic remains")
	}
}
