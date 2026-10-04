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
