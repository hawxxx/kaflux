package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMatcherMatchesFullRecordParts(t *testing.T) {
	r := &kgo.Record{Key: []byte("AA:BB:CC:01"), Value: []byte(`{"mac":"aa:bb:cc:02"}`), Headers: []kgo.RecordHeader{{Key: "device", Value: []byte("aa:bb:cc:03")}}}
	for _, c := range []struct {
		in, op, text string
		sensitive    bool
		want         bool
	}{
		{"key", "contains", "aa:bb:cc:01", false, true},
		{"key", "contains", "aa:bb:cc:01", true, false},
		{"key", "equals", "AA:BB:CC:01", true, true},
		{"key", "equals", "AA:BB:CC", true, false},
		{"value", "contains", "cc:02", false, true},
		{"value", "contains", "cc:03", false, false},
		{"headers", "contains", "cc:03", false, true},
		{"headers", "equals", "device", true, true},
		{"any", "regex", `cc:0[3-9]`, false, true},
		{"any", "regex", `^AA:BB:CC:0[2-9]`, true, false},
		{"", "", "CC:02", false, true},
	} {
		m, e := NewMatcher(c.in, c.op, c.text, c.sensitive)
		if e != nil {
			t.Fatal(e)
		}
		if got := m.Record(r); got != c.want {
			t.Fatalf("%+v: got %v", c, got)
		}
	}
	for _, bad := range [][3]string{{"body", "contains", "x"}, {"key", "like", "x"}, {"key", "contains", ""}, {"key", "regex", "("}} {
		if _, e := NewMatcher(bad[0], bad[1], bad[2], false); e == nil {
			t.Fatalf("%v accepted", bad)
		}
	}
}

func TestKeyPartitionMatchesProducerPartitioners(t *testing.T) {
	// Java: Utils.toPositive(Utils.murmur2(key)) % partitions, with murmur2("21") = -973932308
	// and murmur2("abc") = 479470107.
	for key, want := range map[string]int32{"21": int32((-973932308 & 0x7fffffff) % 7), "abc": 479470107 % 7} {
		if got, _ := keyPartition("murmur2", []byte(key), 7); got != want {
			t.Fatalf("murmur2 %q: got %d, want %d", key, got, want)
		}
	}
	// librdkafka consistent: crc32("abc") = 0x352441c2.
	if got, _ := keyPartition("crc32", []byte("abc"), 7); got != 0x352441c2%7 {
		t.Fatalf("crc32: got %d", got)
	}
}

func search(t *testing.T, d *Demo, q SearchQuery) SearchResult {
	t.Helper()
	r, e := d.Search(context.Background(), q)
	if e != nil {
		t.Fatal(e)
	}
	return r
}

func matcher(t *testing.T, in, op, text string, sensitive bool) Matcher {
	t.Helper()
	m, e := NewMatcher(in, op, text, sensitive)
	if e != nil {
		t.Fatal(e)
	}
	return m
}

func TestDemoSearchFindsOneRecordAcrossPartitions(t *testing.T) {
	d := NewDemo()
	// Partition 3 of the first topic holds order IDs 72 to 95.
	r := search(t, d, SearchQuery{Topic: "orders.created", Match: matcher(t, "value", "contains", "ord-000077", false), MaxMatches: 50})
	if !r.Done || r.StoppedBy != "end" || len(r.Matches) != 1 || r.Matches[0].Partition != 3 || r.Matches[0].Offset != 5 {
		t.Fatalf("unexpected result: %+v", r)
	}
	if r.Scanned.Records != 12*24 || len(r.Resume) != 0 {
		t.Fatalf("whole topic not scanned: %+v", r.Scanned)
	}
}

func TestDemoSearchResumesWithoutGapsOrDuplicates(t *testing.T) {
	defer func(b int64) { searchRecordBudget = b }(searchRecordBudget)
	for name, budget := range map[string]struct {
		records int64
		matches int
	}{"matches": {1 << 40, 5}, "records": {30, 500}} {
		searchRecordBudget = budget.records
		d := NewDemo()
		q := SearchQuery{Topic: "orders.created", Match: matcher(t, "key", "equals", "order-3", true), MaxMatches: budget.matches}
		seen := map[string]bool{}
		for round := 0; ; round++ {
			if round > 40 {
				t.Fatalf("%s: search never finished", name)
			}
			r := search(t, d, q)
			for _, m := range r.Matches {
				id := fmt.Sprintf("%d/%d", m.Partition, m.Offset)
				if seen[id] {
					t.Fatalf("%s: %s returned twice", name, id)
				}
				seen[id] = true
			}
			if r.Done {
				break
			}
			if r.StoppedBy != name || len(r.Resume) == 0 {
				t.Fatalf("%s: stopped by %q with resume %v", name, r.StoppedBy, r.Resume)
			}
			q.FromOffsets = r.Resume
		}
		if len(seen) != 12 {
			t.Fatalf("%s: expected one match per partition, got %d", name, len(seen))
		}
	}
}

func TestDemoSearchRangesAndPartitions(t *testing.T) {
	d := NewDemo()
	from, to := time.Date(2026, 10, 1, 12, 0, 10, 0, time.UTC), time.Date(2026, 10, 1, 12, 0, 15, 0, time.UTC)
	r := search(t, d, SearchQuery{Topic: "orders.created", FromTime: &from, ToTime: &to, Match: matcher(t, "any", "contains", "demo", false), MaxMatches: 500})
	if r.Scanned.Records != 12*5 || len(r.Matches) != 60 || r.Matches[0].Offset != 10 {
		t.Fatalf("time range not applied: %+v", r.Scanned)
	}
	r = search(t, d, SearchQuery{Topic: "orders.created", Partitions: []int32{2, 4}, Match: matcher(t, "key", "contains", "order", false), MaxMatches: 500})
	if r.Scanned.Records != 48 {
		t.Fatalf("partition filter ignored: %+v", r.Scanned)
	}
	only, _ := keyPartition("murmur2", []byte("order-7"), 12)
	r = search(t, d, SearchQuery{Topic: "orders.created", Partitioner: "murmur2", Match: matcher(t, "key", "equals", "order-7", true), MaxMatches: 50})
	if r.Scanned.Records != 24 || len(r.Matches) != 1 || r.Matches[0].Partition != only {
		t.Fatalf("key partition shortcut not applied: %+v", r)
	}
	if _, e := d.Search(context.Background(), SearchQuery{Topic: "orders.created", Partitions: []int32{99}, Match: matcher(t, "key", "contains", "x", false), MaxMatches: 1}); !errors.Is(e, ErrUnknownPartition) {
		t.Fatalf("unknown partition: %v", e)
	}
	if _, e := d.Search(context.Background(), SearchQuery{Topic: "missing", Match: matcher(t, "key", "contains", "x", false), MaxMatches: 1}); !errors.Is(e, ErrUnknownTopic) {
		t.Fatalf("unknown topic: %v", e)
	}
}
