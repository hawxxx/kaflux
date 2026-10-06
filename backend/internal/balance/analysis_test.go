package balance

import (
	"github.com/hawxxx/kaflux/backend/internal/model"
	"math"
	"testing"
)

func TestSkewStatisticsAreTransparentAndStable(t *testing.T) {
	d := Statistics("replicas", map[int32]float64{1: 10, 2: 20, 3: 30})
	if d.Mean != 20 || d.Min != 10 || d.Max != 30 || d.Range != 20 || math.Abs(d.CV-math.Sqrt(200./3.)/20) > 1e-10 || d.MaxToMean != 1.5 || d.Status != "HIGH_SKEW" {
		t.Fatalf("bad statistics %+v", d)
	}
	if d.Brokers[0].Percentage != -50 || d.Brokers[2].Difference != 10 {
		t.Fatal("deviation incorrect")
	}
	if z := Statistics("leaders", map[int32]float64{1: 0, 2: 0}); math.IsNaN(z.CV) || z.CV != 0 {
		t.Fatal("zero mean produced invalid skew")
	}
}
func TestUnknownStorageDoesNotLookBalanced(t *testing.T) {
	d := Analyze(model.Snapshot{Brokers: []model.Broker{{ID: 1}, {ID: 2}}})
	if d[2].Status != "UNAVAILABLE" || len(d[2].Brokers) != 0 {
		t.Fatal("unknown data implied balance")
	}
}
func TestTopicAnalysisFlagsPlacementAndReplicaHealth(t *testing.T) {
	size := int64(100)
	s := model.Snapshot{Brokers: []model.Broker{{ID: 1, Rack: "a"}, {ID: 2, Rack: "a"}, {ID: 3, Rack: "b"}}}
	topic := model.Topic{Name: "orders", ReplicationFactor: 2, Partitions: []model.Partition{
		{ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}, SizeBytes: &size},
		{ID: 1, Leader: 1, Replicas: []int32{2, 1}, ISR: []int32{1}, SizeBytes: &size},
		{ID: 2, Leader: -1, Replicas: []int32{1, 3}, ISR: []int32{}, SizeBytes: &size},
	}}
	a := AnalyzeTopic(s, topic)
	if a.Brokers[0].Replicas != 3 || a.Brokers[0].Leaders != 2 || a.Brokers[1].OutOfSync != 1 || a.Brokers[2].Replicas != 1 || *a.Brokers[0].Bytes != 300 {
		t.Fatalf("bad broker counts %+v", a.Brokers)
	}
	if !a.RackAware || a.SingleRackPartitions != 2 || a.URP != 2 || a.Offline != 1 || math.Abs(a.PreferredLeaderRatio-1./3) > 1e-9 {
		t.Fatalf("bad topic health %+v", a)
	}
	if a.Dimensions[0].Status != "HIGH_SKEW" || a.Findings[0].Severity != "critical" || a.Findings[0].Kind != "offline" {
		t.Fatalf("bad skew or findings %+v %+v", a.Dimensions[0], a.Findings)
	}
}
func TestTopicAnalysisTreatsSpreadOfOneAsBalanced(t *testing.T) {
	s := model.Snapshot{Brokers: []model.Broker{{ID: 1}, {ID: 2}, {ID: 3}}}
	a := AnalyzeTopic(s, model.Topic{Name: "tiny", Partitions: []model.Partition{{ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}})
	if a.Dimensions[0].Status != "GOOD" || a.Dimensions[1].Status != "GOOD" || a.Dimensions[2].Status != "UNAVAILABLE" || a.RackAware {
		t.Fatalf("single partition topic misreported %+v", a.Dimensions)
	}
}
