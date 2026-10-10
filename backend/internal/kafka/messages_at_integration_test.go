package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNativeMessagesAtReadsPartitionsThroughOneClient(t *testing.T) {
	seed := os.Getenv("KAFLUX_TEST_KAFKA_SEED")
	if seed == "" {
		t.Skip("set KAFLUX_TEST_KAFKA_SEED for real Kafka integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, e := kgo.NewClient(kgo.SeedBrokers(seed))
	if e != nil {
		t.Fatal(e)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)
	topic := fmt.Sprintf("kaflux-multi-%d", time.Now().UnixNano())
	r, e := admin.CreateTopics(ctx, 3, 1, nil, topic)
	if e != nil || r[topic].Err != nil {
		t.Fatalf("create %v %v", e, r[topic].Err)
	}
	defer admin.DeleteTopics(context.Background(), topic)
	p, e := NewNative(Config{Seeds: []string{seed}, AllowPlaintext: true})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	awaitNativeTopic(t, ctx, p, topic, 3, 1)
	before := time.Now().Add(-time.Minute)
	for part := int32(0); part < 3; part++ {
		for i := 0; i < 3; i++ {
			if _, e = p.Produce(ctx, model.Message{Topic: topic, Partition: part, Value: fmt.Sprintf("p%d-%d", part, i)}); e != nil {
				t.Fatal(e)
			}
		}
	}
	got, e := p.MessagesAt(ctx, topic, map[int32]int64{0: 0, 1: 1, 2: 0}, 2)
	if e != nil {
		t.Fatal(e)
	}
	count := map[int32]int{}
	for _, m := range got {
		count[m.Partition]++
	}
	if count[0] != 2 || count[1] != 2 || count[2] != 2 {
		t.Fatalf("per-partition limit not honoured: %v", count)
	}
	past, e := p.MessagesAt(ctx, topic, map[int32]int64{0: 3, 1: 3}, 5)
	if e != nil || len(past) != 0 {
		t.Fatalf("reading from the end must return nothing: %v %v", e, past)
	}
	if _, e = p.MessagesAt(ctx, topic, map[int32]int64{7: 0}, 5); !errors.Is(e, ErrUnknownPartition) {
		t.Fatalf("unknown partition must report ErrUnknownPartition: %v", e)
	}
	// Large records on partition 0 must not use up the budget the other partitions are owed.
	big := strings.Repeat("x", 500<<10)
	for i := 0; i < 4; i++ {
		if _, e = p.Produce(ctx, model.Message{Topic: topic, Partition: 0, Value: big}); e != nil {
			t.Fatal(e)
		}
	}
	fair, e := p.MessagesAt(ctx, topic, map[int32]int64{0: 3, 1: 0, 2: 0}, 10)
	if e != nil {
		t.Fatal(e)
	}
	count = map[int32]int{}
	for _, m := range fair {
		count[m.Partition]++
	}
	if count[0] == 0 || count[1] != 3 || count[2] != 3 {
		t.Fatalf("one partition crowded out the others: %v", count)
	}
	starts, e := p.OffsetsAt(ctx, topic, before)
	if e != nil || len(starts) != 3 || starts[0] != 0 {
		t.Fatalf("offsets at a past time start at the beginning: %v %v", e, starts)
	}
	ends, e := p.OffsetsAt(ctx, topic, time.Now().Add(time.Hour))
	if e != nil || ends[1] != 3 {
		t.Fatalf("offsets after the last record resume at the end: %v %v", e, ends)
	}
}
