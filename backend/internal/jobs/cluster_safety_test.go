package jobs

import (
	"context"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"testing"
)

func TestClusterSafetyIncludesUnrelatedPartitions(t *testing.T) {
	s, _ := kafka.NewDemo().Snapshot(context.Background())
	if e := ValidateCluster(s); e != nil {
		t.Fatal(e)
	}
	s.Topics[len(s.Topics)-1].Partitions[0].ISR = nil
	if ValidateCluster(s) == nil {
		t.Fatal("unrelated under-replicated partition allowed execution")
	}
	s.Controller = nil
	if ValidateCluster(s) == nil {
		t.Fatal("unknown controller allowed execution")
	}
}
