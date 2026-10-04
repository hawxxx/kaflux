package simulator

import (
	"encoding/json"
	"testing"
)

func TestLargeMetadataDimensions(t *testing.T) {
	s, e := Metadata(100, 10000, 100000, 3)
	if e != nil {
		t.Fatal(e)
	}
	parts := 0
	replicas := 0
	for _, topic := range s.Topics {
		parts += len(topic.Partitions)
		for _, p := range topic.Partitions {
			if len(p.Replicas) != 3 || len(p.ISR) != 3 {
				t.Fatal("replication invariant")
			}
			seen := map[int32]bool{}
			for _, id := range p.Replicas {
				if seen[id] {
					t.Fatal("duplicate replica")
				}
				seen[id] = true
			}
		}
	}
	for _, b := range s.Brokers {
		replicas += b.Partitions
	}
	if len(s.Topics) != 10000 || parts != 100000 || replicas != 300000 {
		t.Fatal("wrong metadata dimensions")
	}
}
func BenchmarkMetadataSerialization(b *testing.B) {
	for _, scenario := range []struct {
		name                        string
		brokers, topics, partitions int
	}{{"15b-6000t-30000p", 15, 6000, 30000}, {"100b-10000t-100000p", 100, 10000, 100000}} {
		b.Run(scenario.name, func(b *testing.B) {
			s, e := Metadata(scenario.brokers, scenario.topics, scenario.partitions, 3)
			if e != nil {
				b.Fatal(e)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, e = json.Marshal(s); e != nil {
					b.Fatal(e)
				}
			}
		})
	}
}
