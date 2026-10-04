package balance

import (
	"github.com/hawxxx/kaflux/backend/internal/model"
	"math"
	"sort"
)

type BrokerDeviation struct {
	Broker     int32   `json:"broker"`
	Value      float64 `json:"value"`
	Difference float64 `json:"differenceFromMean"`
	Percentage float64 `json:"percentageDifference"`
}
type Dimension struct {
	ID        string            `json:"id"`
	Status    string            `json:"status"`
	Mean      float64           `json:"mean"`
	Min       float64           `json:"min"`
	Max       float64           `json:"max"`
	Range     float64           `json:"maxMinusMin"`
	StdDev    float64           `json:"standardDeviation"`
	CV        float64           `json:"coefficientOfVariation"`
	MaxToMean float64           `json:"maxToMeanRatio"`
	Brokers   []BrokerDeviation `json:"brokers"`
	Reason    string            `json:"reason,omitempty"`
}

func Statistics(id string, values map[int32]float64) Dimension {
	d := Dimension{ID: id, Status: "GOOD", Brokers: []BrokerDeviation{}}
	if len(values) == 0 {
		d.Status = "UNAVAILABLE"
		return d
	}
	d.Min = math.Inf(1)
	for _, v := range values {
		d.Mean += v
		d.Min = math.Min(d.Min, v)
		d.Max = math.Max(d.Max, v)
	}
	d.Mean /= float64(len(values))
	for broker, v := range values {
		difference := v - d.Mean
		percent := 0.
		if d.Mean > 0 {
			percent = difference / d.Mean * 100
		}
		d.Brokers = append(d.Brokers, BrokerDeviation{broker, v, difference, percent})
		d.StdDev += difference * difference
	}
	d.StdDev = math.Sqrt(d.StdDev / float64(len(values)))
	d.Range = d.Max - d.Min
	if d.Mean > 0 {
		d.CV = d.StdDev / d.Mean
		d.MaxToMean = d.Max / d.Mean
	}
	if d.CV >= .25 {
		d.Status = "HIGH_SKEW"
	} else if d.CV >= .1 {
		d.Status = "MODERATE"
	}
	sort.Slice(d.Brokers, func(i, j int) bool { return d.Brokers[i].Broker < d.Brokers[j].Broker })
	return d
}
func Analyze(s model.Snapshot) []Dimension {
	parts, leaders, storage := map[int32]float64{}, map[int32]float64{}, map[int32]float64{}
	storageKnown := len(s.Brokers) > 0
	for _, b := range s.Brokers {
		parts[b.ID] = float64(b.Partitions)
		leaders[b.ID] = float64(b.Leaders)
		if b.SizeBytes == nil {
			storageKnown = false
		} else {
			storage[b.ID] = float64(*b.SizeBytes)
		}
	}
	out := []Dimension{Statistics("replicas", parts), Statistics("leaders", leaders)}
	if storageKnown {
		out = append(out, Statistics("bytes", storage))
	} else {
		out = append(out, Dimension{ID: "bytes", Status: "UNAVAILABLE", Reason: "Complete broker storage observations are required", Brokers: []BrokerDeviation{}})
	}
	for _, id := range []string{"ingress", "egress", "request-rate"} {
		out = append(out, Dimension{ID: id, Status: "UNAVAILABLE", Reason: "Traffic analysis requires time-aligned broker metric series", Brokers: []BrokerDeviation{}})
	}
	return out
}
