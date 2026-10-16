package kafka

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNativeSearchScansTheTopicOnTheServer(t *testing.T) {
	seed := os.Getenv("KAFLUX_TEST_KAFKA_SEED")
	if seed == "" {
		t.Skip("set KAFLUX_TEST_KAFKA_SEED for real Kafka integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cl, e := kgo.NewClient(kgo.SeedBrokers(seed), kgo.ProducerBatchMaxBytes(2<<20))
	if e != nil {
		t.Fatal(e)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)
	topic := fmt.Sprintf("kaflux-search-%d", time.Now().UnixNano())
	r, e := admin.CreateTopics(ctx, 3, 1, map[string]*string{"max.message.bytes": kadm.StringPtr("2097152")}, topic)
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
	// Keyed records go through the client's default partitioner, which hashes keys like Java.
	records := []*kgo.Record{}
	for i := 0; i < 300; i++ {
		records = append(records, &kgo.Record{Topic: topic, Key: []byte(fmt.Sprintf("device-%03d", i)), Value: []byte(fmt.Sprintf(`{"mac":"02:00:00:00:%02x:%02x"}`, i/256, i%256))})
	}
	// The needle sits past the 256 KiB preview cut, so only a full-record match finds it.
	records = append(records, &kgo.Record{Topic: topic, Key: []byte("big"), Value: []byte(strings.Repeat("x", 300<<10) + "needle-mac 0a:0b:0c")})
	if e = cl.ProduceSync(ctx, records...).FirstErr(); e != nil {
		t.Fatal(e)
	}
	find := func(q SearchQuery) SearchResult {
		t.Helper()
		out, e := p.Search(ctx, q)
		if e != nil {
			t.Fatal(e)
		}
		return out
	}
	match := func(in, op, text string, sensitive bool) Matcher {
		m, e := NewMatcher(in, op, text, sensitive)
		if e != nil {
			t.Fatal(e)
		}
		return m
	}
	got := find(SearchQuery{Topic: topic, Match: match("value", "contains", "0A:0B:0C", false), MaxMatches: 50})
	if !got.Done || len(got.Matches) != 1 || got.Matches[0].Key != "big" || !got.Matches[0].Truncated || got.Scanned.Records != 301 {
		t.Fatalf("full-record match: %+v %+v", got.Scanned, got.Matches)
	}
	keyed := find(SearchQuery{Topic: topic, Partitioner: "murmur2", Match: match("key", "equals", "device-123", true), MaxMatches: 50})
	if !keyed.Done || len(keyed.Matches) != 1 || keyed.Scanned.Records >= 301 {
		t.Fatalf("key shortcut: %+v %d matches", keyed.Scanned, len(keyed.Matches))
	}
	defer func(b int64) { searchRecordBudget = b }(searchRecordBudget)
	searchRecordBudget = 40
	q := SearchQuery{Topic: topic, Match: match("value", "contains", `"mac":"02:00:00:00:00:`, true), MaxMatches: 500}
	seen := map[string]bool{}
	for round := 0; ; round++ {
		if round > 20 {
			t.Fatal("search never finished")
		}
		r := find(q)
		for _, m := range r.Matches {
			id := fmt.Sprintf("%d/%d", m.Partition, m.Offset)
			if seen[id] {
				t.Fatalf("%s returned twice", id)
			}
			seen[id] = true
		}
		if r.Done {
			break
		}
		if r.StoppedBy != "records" {
			t.Fatalf("stopped by %q", r.StoppedBy)
		}
		q.FromOffsets = r.Resume
	}
	if len(seen) != 256 {
		t.Fatalf("resumed search found %d of 256", len(seen))
	}
}

// A committed transaction leaves a marker after its last record. The consumer does not return
// markers by default, so a search of a topic that ends in one must still reach its end promptly.
func TestNativeSearchFinishesATopicEndingInTransactionMarkers(t *testing.T) {
	seed := os.Getenv("KAFLUX_TEST_KAFKA_SEED")
	if seed == "" {
		t.Skip("set KAFLUX_TEST_KAFKA_SEED for real Kafka integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	topic := fmt.Sprintf("kaflux-search-tx-%d", time.Now().UnixNano())
	admin, e := kgo.NewClient(kgo.SeedBrokers(seed))
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	adm := kadm.NewClient(admin)
	if r, e := adm.CreateTopics(ctx, 2, 1, nil, topic); e != nil || r[topic].Err != nil {
		t.Fatalf("create %v %v", e, r[topic].Err)
	}
	defer adm.DeleteTopics(context.Background(), topic)
	p, e := NewNative(Config{Seeds: []string{seed}, AllowPlaintext: true})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	awaitNativeTopic(t, ctx, p, topic, 2, 1)
	producer, e := kgo.NewClient(kgo.SeedBrokers(seed), kgo.TransactionalID(topic), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if e != nil {
		t.Fatal(e)
	}
	defer producer.Close()
	for round := 0; round < 3; round++ {
		if e = producer.BeginTransaction(); e != nil {
			t.Fatal(e)
		}
		for part := int32(0); part < 2; part++ {
			for i := 0; i < 5; i++ {
				v := fmt.Sprintf("round-%d-%d", round, i)
				if round == 2 && part == 1 && i == 4 {
					v = "needle"
				}
				if e = producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: part, Value: []byte(v)}).FirstErr(); e != nil {
					t.Fatal(e)
				}
			}
		}
		if e = producer.EndTransaction(ctx, kgo.TryCommit); e != nil {
			t.Fatal(e)
		}
	}
	m, e := NewMatcher("value", "contains", "needle", true)
	if e != nil {
		t.Fatal(e)
	}
	start := time.Now()
	got, e := p.Search(ctx, SearchQuery{Topic: topic, Match: m, MaxMatches: 10})
	if e != nil {
		t.Fatal(e)
	}
	if !got.Done || got.StoppedBy != "end" || len(got.Resume) != 0 || got.Scanned.Records != 30 || len(got.Matches) != 1 {
		t.Fatalf("topic ending in markers: %+v %+v", got, got.Matches)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Fatalf("search took %s", took)
	}
}
