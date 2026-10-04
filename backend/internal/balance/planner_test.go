package balance

import (
	"github.com/hawxxx/kaflux/backend/internal/model"
	"testing"
)

func TestPlannerInvariants(t *testing.T) {
	s := model.Snapshot{Brokers: []model.Broker{{ID: 1, Rack: "a"}, {ID: 2, Rack: "b"}, {ID: 3, Rack: "c"}}, Topics: []model.Topic{{Name: "selected", Partitions: []model.Partition{{ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}}}}, {Name: "other", Partitions: []model.Partition{{ID: 0, Leader: 3, Replicas: []int32{3}, ISR: []int32{3}}}}}}
	p, e := Generate(s, Request{Topics: []string{"selected"}, Brokers: []int32{2, 3}, RackAware: true})
	if e != nil {
		t.Fatal(e)
	}
	for _, c := range p.Changes {
		if c.Topic != "selected" || len(c.After) != 2 || c.After[0] == c.After[1] {
			t.Fatalf("invalid change %+v", c)
		}
		for _, b := range c.After {
			if b == 1 {
				t.Fatal("excluded broker used")
			}
		}
	}
	if s.Topics[0].Partitions[0].Replicas[0] != 1 {
		t.Fatal("mutated snapshot")
	}
	q, _ := Generate(s, Request{Topics: []string{"selected"}, Brokers: []int32{2, 3}, RackAware: true})
	if p.PlanHash != q.PlanHash {
		t.Fatal("nondeterministic hash")
	}
	s.Topics[0].Partitions[0].ISR = nil
	if _, e := Generate(s, Request{Topics: []string{"selected"}}); e == nil {
		t.Fatal("unsafe ISR accepted")
	}
}
