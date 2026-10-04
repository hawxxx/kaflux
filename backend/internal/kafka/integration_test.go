package kafka

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"testing"
	"time"
)

func TestNativeKafkaIntegration(t *testing.T) {
	seed := os.Getenv("KAFLUX_TEST_KAFKA_SEED")
	if seed == "" {
		t.Skip("set KAFLUX_TEST_KAFKA_SEED for real Kafka integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, e := kgo.NewClient(kgo.SeedBrokers(seed))
	if e != nil {
		t.Fatal(e)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)
	topic := fmt.Sprintf("kaflux-integration-%d", time.Now().UnixNano())
	r, e := admin.CreateTopics(ctx, 2, 1, nil, topic)
	if e != nil {
		t.Fatal(e)
	}
	if e = r[topic].Err; e != nil {
		t.Fatal(e)
	}
	defer admin.DeleteTopics(context.Background(), topic)
	p, e := NewNative(Config{Seeds: []string{seed}, AllowPlaintext: true})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	snap := awaitNativeTopic(t, ctx, p, topic, 2, 1)
	if len(snap.Brokers) == 0 {
		t.Fatalf("metadata failed %v", e)
	}
	m, e := p.Produce(ctx, model.Message{Topic: topic, Partition: 1, Key: "integration", Value: `{"sanitized":true}`})
	if e != nil || m.Partition != 1 {
		t.Fatalf("produce %v %+v", e, m)
	}
	messages, e := p.Messages(ctx, topic, 1, m.Offset, 5)
	if e != nil || len(messages) != 1 || messages[0].Value != m.Value {
		t.Fatalf("consume failed %v %+v", e, messages)
	}
	pending, e := p.Pending(ctx)
	if e != nil || len(pending) != 0 {
		t.Fatalf("pending failed %v", e)
	}
	raw := []byte{0xff, 0, 1, 0xfe}
	binary, e := p.Produce(ctx, model.Message{Topic: topic, Partition: 0, Value: string(raw)})
	if e != nil {
		t.Fatal(e)
	}
	preview, e := p.Messages(ctx, topic, 0, binary.Offset, 1)
	if e != nil || len(preview) != 1 || preview[0].ValueBase64 != base64.StdEncoding.EncodeToString(raw) {
		t.Fatalf("binary payload lost: %v %+v", e, preview)
	}
	detail, e := p.TopicDetail(ctx, topic)
	if e != nil || detail.CleanupPolicy == "" || detail.Partitions[1].EndOffset == nil || *detail.Partitions[1].EndOffset != 1 {
		t.Fatalf("topic details missing %+v %v", detail, e)
	}
}
