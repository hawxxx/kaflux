package balance

import (
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"math"
	"sort"
	"time"
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

type TopicBroker struct {
	Broker    int32  `json:"broker"`
	Rack      string `json:"rack"`
	Replicas  int    `json:"replicas"`
	Leaders   int    `json:"leaders"`
	Preferred int    `json:"preferredReplicas"`
	OutOfSync int    `json:"outOfSync"`
	Bytes     *int64 `json:"bytes"`
}
type TopicPartition struct {
	model.Partition
	PreferredLeader bool `json:"preferredLeader"`
	UnderReplicated bool `json:"underReplicated"`
	Offline         bool `json:"offline"`
	SingleRack      bool `json:"singleRack"`
}
type Finding struct {
	Severity  string `json:"severity"`
	Kind      string `json:"kind"`
	Message   string `json:"message"`
	Partition *int32 `json:"partition,omitempty"`
	Broker    *int32 `json:"broker,omitempty"`
}
type TopicAnalysis struct {
	Topic                string           `json:"topic"`
	ReplicationFactor    int              `json:"replicationFactor"`
	Brokers              []TopicBroker    `json:"brokers"`
	Partitions           []TopicPartition `json:"partitions"`
	Dimensions           []Dimension      `json:"dimensions"`
	PreferredLeaderRatio float64          `json:"preferredLeaderRatio"`
	URP                  int              `json:"urp"`
	Offline              int              `json:"offline"`
	RackAware            bool             `json:"rackAware"`
	SingleRackPartitions int              `json:"singleRackPartitions"`
	Findings             []Finding        `json:"findings"`
	ObservedAt           time.Time        `json:"observedAt"`
}

// AnalyzeTopic reports how one topic's replicas and leaders spread across every broker in the cluster.
// Brokers hosting nothing of the topic count as zero, so placement that ignores brokers shows as skew.
// A count dimension whose spread is at most one is the best integer placement and is reported as GOOD.
func AnalyzeTopic(s model.Snapshot, t model.Topic) TopicAnalysis {
	a := TopicAnalysis{Topic: t.Name, ReplicationFactor: t.ReplicationFactor, Brokers: []TopicBroker{}, Partitions: []TopicPartition{}, Findings: []Finding{}, ObservedAt: s.ObservedAt}
	index, racks := map[int32]int{}, map[string]bool{}
	a.RackAware = len(s.Brokers) > 0
	for _, b := range s.Brokers {
		index[b.ID] = len(a.Brokers)
		a.Brokers = append(a.Brokers, TopicBroker{Broker: b.ID, Rack: b.Rack})
		if b.Rack == "" {
			a.RackAware = false
		}
		racks[b.Rack] = true
	}
	a.RackAware = a.RackAware && len(racks) > 1
	sizeKnown, sizes, preferred := true, make([]int64, len(a.Brokers)), 0
	for _, p := range t.Partitions {
		isr := map[int32]bool{}
		for _, r := range p.ISR {
			isr[r] = true
		}
		tp := TopicPartition{Partition: p, Offline: p.Leader < 0, UnderReplicated: len(p.ISR) < len(p.Replicas)}
		tp.PreferredLeader = len(p.Replicas) > 0 && p.Leader == p.Replicas[0]
		if tp.PreferredLeader {
			preferred++
		}
		if p.SizeBytes == nil {
			sizeKnown = false
		}
		used := map[string]bool{}
		for i, r := range p.Replicas {
			j, ok := index[r]
			if !ok {
				continue
			}
			b := &a.Brokers[j]
			b.Replicas++
			used[b.Rack] = true
			if i == 0 {
				b.Preferred++
			}
			if !isr[r] {
				b.OutOfSync++
			}
			if p.SizeBytes != nil {
				sizes[j] += *p.SizeBytes
			}
		}
		if j, ok := index[p.Leader]; ok {
			a.Brokers[j].Leaders++
		}
		tp.SingleRack = a.RackAware && len(p.Replicas) > 1 && len(used) == 1
		id := p.ID
		switch {
		case tp.Offline:
			a.Offline++
			a.Findings = append(a.Findings, Finding{"critical", "offline", fmt.Sprintf("Partition %d has no leader", p.ID), &id, nil})
		case tp.UnderReplicated:
			a.Findings = append(a.Findings, Finding{"warning", "under-replicated", fmt.Sprintf("Partition %d has %d of %d replicas in sync", p.ID, len(p.ISR), len(p.Replicas)), &id, nil})
		}
		if tp.UnderReplicated {
			a.URP++
		}
		if tp.SingleRack {
			a.SingleRackPartitions++
			a.Findings = append(a.Findings, Finding{"warning", "single-rack", fmt.Sprintf("Partition %d keeps every replica in one rack", p.ID), &id, nil})
		}
		if !tp.Offline && !tp.PreferredLeader && len(p.Replicas) > 0 {
			a.Findings = append(a.Findings, Finding{"info", "non-preferred-leader", fmt.Sprintf("Partition %d is led by broker %d instead of preferred broker %d", p.ID, p.Leader, p.Replicas[0]), &id, nil})
		}
		a.Partitions = append(a.Partitions, tp)
	}
	sort.Slice(a.Partitions, func(i, j int) bool { return a.Partitions[i].ID < a.Partitions[j].ID })
	if len(t.Partitions) > 0 {
		a.PreferredLeaderRatio = float64(preferred) / float64(len(t.Partitions))
	}
	replicas, leaders, storage := map[int32]float64{}, map[int32]float64{}, map[int32]float64{}
	for i := range a.Brokers {
		b := &a.Brokers[i]
		replicas[b.Broker], leaders[b.Broker] = float64(b.Replicas), float64(b.Leaders)
		if sizeKnown {
			b.Bytes = &sizes[i]
			storage[b.Broker] = float64(sizes[i])
		}
	}
	a.Dimensions = []Dimension{integerSpread(Statistics("replicas", replicas)), integerSpread(Statistics("leaders", leaders))}
	if sizeKnown && len(t.Partitions) > 0 {
		a.Dimensions = append(a.Dimensions, Statistics("bytes", storage))
	} else {
		a.Dimensions = append(a.Dimensions, Dimension{ID: "bytes", Status: "UNAVAILABLE", Reason: "Every partition needs a size observation", Brokers: []BrokerDeviation{}})
	}
	for _, d := range a.Dimensions {
		if d.Status != "HIGH_SKEW" && d.Status != "MODERATE" {
			continue
		}
		severity := "warning"
		if d.Status == "MODERATE" {
			severity = "info"
		}
		top := d.Brokers[0]
		for _, b := range d.Brokers {
			if b.Value > top.Value {
				top = b
			}
		}
		broker := top.Broker
		a.Findings = append(a.Findings, Finding{severity, d.ID + "-skew", fmt.Sprintf("Broker %d carries %+.0f%% %s against the mean (CV %.1f%%)", top.Broker, top.Percentage, d.ID, d.CV*100), nil, &broker})
	}
	rank := map[string]int{"critical": 0, "warning": 1, "info": 2}
	sort.SliceStable(a.Findings, func(i, j int) bool { return rank[a.Findings[i].Severity] < rank[a.Findings[j].Severity] })
	return a
}

func integerSpread(d Dimension) Dimension {
	if (d.Status == "MODERATE" || d.Status == "HIGH_SKEW") && d.Range <= 1 {
		d.Status = "GOOD"
		d.Reason = "Spread is within one, the most even integer placement"
	}
	return d
}
