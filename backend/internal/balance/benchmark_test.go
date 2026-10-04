package balance

import (
	"github.com/hawxxx/kaflux/backend/internal/simulator"
	"testing"
)

func BenchmarkLargeClusterPlanner(b *testing.B) {
	s, e := simulator.Metadata(100, 10000, 100000, 3)
	if e != nil {
		b.Fatal(e)
	}
	topics := make([]string, 500)
	for i := range topics {
		topics[i] = s.Topics[i].Name
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, e := Generate(s, Request{Topics: topics, RackAware: true})
		if e != nil {
			b.Fatal(e)
		}
		if len(p.Changes) != 5000 {
			b.Fatal("wrong selected partition count")
		}
	}
}

func TestLargeRackAwarePlanIsDeterministicAndRestorable(t *testing.T) {
	s, e := simulator.Metadata(15, 6000, 30000, 3)
	if e != nil {
		t.Fatal(e)
	}
	request := Request{Topics: []string{s.Topics[0].Name, s.Topics[100].Name}, Brokers: []int32{2, 3, 4, 5, 6, 7}, RackAware: true}
	p, e := Generate(s, request)
	if e != nil {
		t.Fatal(e)
	}
	q, e := Generate(s, request)
	if e != nil || q.PlanHash != p.PlanHash {
		t.Fatal("nondeterministic plan")
	}
	racks := map[int32]string{}
	for _, broker := range s.Brokers {
		racks[broker.ID] = broker.Rack
	}
	originals := map[string][]int32{}
	for _, topic := range s.Topics {
		if topic.Name == request.Topics[0] || topic.Name == request.Topics[1] {
			for _, part := range topic.Partitions {
				originals[topic.Name+string(rune(part.ID))] = part.Replicas
			}
		}
	}
	for _, change := range p.Changes {
		if change.Topic != request.Topics[0] && change.Topic != request.Topics[1] {
			t.Fatal("unrelated topic changed")
		}
		seen := map[string]bool{}
		if len(change.Before) != 3 || len(change.After) != 3 {
			t.Fatal("replication factor changed")
		}
		for _, id := range change.After {
			if id == 1 || id > 7 || seen[racks[id]] {
				t.Fatal("excluded broker or rack violation")
			}
			seen[racks[id]] = true
		}
		original := originals[change.Topic+string(rune(change.Partition))]
		for i, id := range change.Before {
			if id != original[i] {
				t.Fatal("original assignment lost")
			}
		}
	}
}
