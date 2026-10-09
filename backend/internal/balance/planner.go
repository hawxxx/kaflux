package balance

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"sort"
)

type Request struct {
	Topics              []string `json:"topics"`
	Brokers             []int32  `json:"brokers"`
	RackAware           bool     `json:"rackAware"`
	ThrottleBytesPerSec int64    `json:"throttleBytesPerSec"`
}

func Hash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func Fingerprint(s model.Snapshot, names []string) string {
	var p []model.Change
	wanted := map[string]bool{}
	for _, n := range names {
		wanted[n] = true
	}
	for _, t := range s.Topics {
		if wanted[t.Name] {
			for _, x := range t.Partitions {
				p = append(p, model.Change{Topic: t.Name, Partition: x.ID, Before: x.Replicas})
			}
		}
	}
	sort.Slice(p, func(i, j int) bool {
		if p[i].Topic == p[j].Topic {
			return p[i].Partition < p[j].Partition
		}
		return p[i].Topic < p[j].Topic
	})
	return Hash(p)
}
func Generate(s model.Snapshot, r Request) (model.Plan, error) {
	p := model.Plan{State: "planned", Topics: append([]string{}, r.Topics...), Changes: []model.Change{}, ThrottleBytesPerSec: r.ThrottleBytesPerSec, RackAware: r.RackAware}
	sort.Strings(p.Topics)
	if len(r.Topics) == 0 {
		return p, fmt.Errorf("select at least one topic")
	}
	wanted := map[string]bool{}
	for _, n := range r.Topics {
		if wanted[n] {
			return p, fmt.Errorf("duplicate topic")
		}
		wanted[n] = true
	}
	brokers := map[int32]model.Broker{}
	for _, b := range s.Brokers {
		brokers[b.ID] = b
	}
	ids := append([]int32{}, r.Brokers...)
	if len(ids) == 0 {
		for id := range brokers {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	seen := map[int32]bool{}
	for _, id := range ids {
		if _, ok := brokers[id]; !ok || seen[id] {
			return p, fmt.Errorf("invalid broker %d", id)
		}
		seen[id] = true
		if r.RackAware && brokers[id].Rack == "" {
			return p, fmt.Errorf("broker %d has unknown rack", id)
		}
	}
	loads := map[int32]int{}
	estimated := int64(0)
	sizesKnown := true
	leaders := map[int32]int{}
	original := map[int32]model.Distribution{}
	final := map[int32]model.Distribution{}
	for _, id := range ids {
		final[id] = model.Distribution{Broker: id}
	}
	ts := append([]model.Topic{}, s.Topics...)
	sort.Slice(ts, func(i, j int) bool { return ts[i].Name < ts[j].Name })
	found := 0
	for _, t := range ts {
		if !wanted[t.Name] {
			continue
		}
		found++
		parts := append([]model.Partition{}, t.Partitions...)
		sort.Slice(parts, func(i, j int) bool { return parts[i].ID < parts[j].ID })
		for _, x := range parts {
			if x.Leader < 0 || len(x.ISR) < len(x.Replicas) {
				return p, fmt.Errorf("unsafe partition %s/%d: offline or insufficient ISR", t.Name, x.ID)
			}
			rf := len(x.Replicas)
			if rf == 0 || rf > len(ids) {
				return p, fmt.Errorf("insufficient eligible brokers")
			}
			racks := map[string]bool{}
			for _, id := range ids {
				racks[brokers[id].Rack] = true
			}
			if r.RackAware && len(racks) < rf {
				return p, fmt.Errorf("insufficient rack diversity")
			}
			after := []int32{}
			used := map[int32]bool{}
			usedRack := map[string]bool{}
			for k := 0; k < rf; k++ {
				candidate := int32(-1)
				for _, id := range ids {
					if used[id] || (r.RackAware && usedRack[brokers[id].Rack]) {
						continue
					}
					if candidate < 0 || loads[id] < loads[candidate] || (loads[id] == loads[candidate] && k == 0 && leaders[id] < leaders[candidate]) {
						candidate = id
					}
				}
				after = append(after, candidate)
				used[candidate] = true
				usedRack[brokers[candidate].Rack] = true
				loads[candidate]++
				if k == 0 {
					leaders[candidate]++
				}
			}
			for i, id := range x.Replicas {
				d := original[id]
				d.Broker = id
				d.Replicas++
				if i == 0 {
					d.Leaders++
				}
				original[id] = d
			}
			for i, id := range after {
				d := final[id]
				d.Replicas++
				if i == 0 {
					d.Leaders++
				}
				final[id] = d
			}
			p.Changes = append(p.Changes, model.Change{Topic: t.Name, Partition: x.ID, Before: append([]int32{}, x.Replicas...), After: after})
			if x.SizeBytes == nil {
				sizesKnown = false
			} else {
				old := map[int32]bool{}
				for _, id := range x.Replicas {
					old[id] = true
				}
				for _, id := range after {
					if !old[id] {
						estimated += *x.SizeBytes
					}
				}
			}
		}
	}
	if found != len(wanted) {
		return p, fmt.Errorf("unknown topic")
	}
	for _, d := range original {
		p.Before = append(p.Before, d)
	}
	for _, d := range final {
		p.After = append(p.After, d)
	}
	sort.Slice(p.Before, func(i, j int) bool { return p.Before[i].Broker < p.Before[j].Broker })
	sort.Slice(p.After, func(i, j int) bool { return p.After[i].Broker < p.After[j].Broker })
	p.Fingerprint = Fingerprint(s, p.Topics)
	p.Steps = Steps(r.Topics, p.Changes, s)
	p.PartitionsTotal = len(p.Changes)
	if sizesKnown {
		p.EstimatedBytes = &estimated
	}
	p.PlanHash = Hash(struct {
		Fingerprint string
		Changes     []model.Change
		Throttle    int64
	}{p.Fingerprint, p.Changes, p.ThrottleBytesPerSec})
	return p, nil
}
