package balance

import (
	"fmt"

	"github.com/hawxxx/kaflux/backend/internal/model"
)

// Steps groups changes into one pending step per topic, in the given order.
// Topics without changes get no step; topics missing from order follow in
// the order their changes appear. Bytes is the data to copy (one replica's
// size per new replica) and stays nil when any size is unknown.
func Steps(order []string, changes []model.Change, s model.Snapshot) []model.TopicStep {
	sizes := map[string]*int64{}
	for _, t := range s.Topics {
		for _, p := range t.Partitions {
			sizes[partitionKey(t.Name, p.ID)] = p.SizeBytes
		}
	}
	type acc struct {
		partitions int
		bytes      int64
		known      bool
	}
	byTopic := map[string]*acc{}
	seen := []string{}
	for _, c := range changes {
		a, ok := byTopic[c.Topic]
		if !ok {
			a = &acc{known: true}
			byTopic[c.Topic] = a
			seen = append(seen, c.Topic)
		}
		a.partitions++
		size := sizes[partitionKey(c.Topic, c.Partition)]
		if size == nil {
			a.known = false
			continue
		}
		old := map[int32]bool{}
		for _, id := range c.Before {
			old[id] = true
		}
		for _, id := range c.After {
			if !old[id] {
				a.bytes += *size
			}
		}
	}
	ordered := []string{}
	used := map[string]bool{}
	for _, t := range append(append([]string{}, order...), seen...) {
		if byTopic[t] != nil && !used[t] {
			used[t] = true
			ordered = append(ordered, t)
		}
	}
	out := make([]model.TopicStep, 0, len(ordered))
	for _, t := range ordered {
		a := byTopic[t]
		step := model.TopicStep{Topic: t, State: model.StepPending, Partitions: a.partitions}
		if a.known {
			b := a.bytes
			step.Bytes = &b
		}
		out = append(out, step)
	}
	return out
}

func partitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}
