package jobs

import (
	"fmt"
	"reflect"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
)

// rateSmoothing weights the newest rate sample in the moving average.
const rateSmoothing = 0.3

// partitionDone reports whether a change reached its target with a full ISR.
func partitionDone(x model.Partition, c model.Change) bool {
	return reflect.DeepEqual(x.Replicas, c.After) && len(x.ISR) == len(x.Replicas)
}

func partitionIndex(s model.Snapshot) map[string]model.Partition {
	out := map[string]model.Partition{}
	for _, t := range s.Topics {
		for _, x := range t.Partitions {
			out[changeKey(t.Name, x.ID)] = x
		}
	}
	return out
}

func changeKey(topic string, partition int32) string { return fmt.Sprintf("%s/%d", topic, partition) }

// updateProgress recomputes partition, byte, rate and ETA figures from the
// latest metadata. sizes may be nil when the cluster does not report them.
func updateProgress(p *model.Plan, partitions map[string]model.Partition, sizes map[string]map[int32]map[int32]int64, now time.Time) {
	done := 0
	for i := range p.Changes {
		c := &p.Changes[i]
		if x, ok := partitions[changeKey(c.Topic, c.Partition)]; ok && partitionDone(x, *c) {
			if c.FinishedAt == nil {
				t := now
				c.FinishedAt = &t
			}
		}
		if c.FinishedAt != nil {
			done++
		}
	}
	p.PartitionsDone = done
	p.PartitionsTotal = len(p.Changes)

	bytesKnown := len(p.Steps) > 0
	var total, copied int64
	for i := range p.Steps {
		step := &p.Steps[i]
		stepDone := 0
		var live int64
		liveKnown := sizes != nil
		for _, c := range p.Changes {
			if c.Topic != step.Topic {
				continue
			}
			if c.FinishedAt != nil {
				stepDone++
			}
			if step.State == model.StepMoving && liveKnown {
				n, ok := copiedBytes(c, partitions[changeKey(c.Topic, c.Partition)], sizes)
				if !ok {
					liveKnown = false
				}
				live += n
			}
		}
		step.PartitionsDone = stepDone
		if step.Bytes == nil {
			bytesKnown = false
			continue
		}
		total += *step.Bytes
		var stepCopied int64
		switch step.State {
		case model.StepDone, model.StepElecting:
			stepCopied = *step.Bytes
		case model.StepMoving:
			if liveKnown {
				stepCopied = min(live, *step.Bytes)
			} else if step.Partitions > 0 {
				stepCopied = *step.Bytes * int64(stepDone) / int64(step.Partitions)
			}
		}
		step.BytesDone = &stepCopied
		copied += stepCopied
	}

	if bytesKnown {
		if p.BytesDone != nil && p.ProgressAt != nil {
			if dt := now.Sub(*p.ProgressAt).Seconds(); dt > 0 && copied >= *p.BytesDone {
				sample := float64(copied-*p.BytesDone) / dt
				rate := sample
				if p.RateBytesPerSec != nil {
					rate = rateSmoothing*sample + (1-rateSmoothing)*float64(*p.RateBytesPerSec)
				}
				r := int64(rate)
				p.RateBytesPerSec = &r
			}
		}
		p.BytesTotal, p.BytesDone = &total, &copied
		if total > 0 {
			p.Progress = int(copied * 100 / total)
		} else {
			p.Progress = 100
		}
	} else {
		p.BytesTotal, p.BytesDone, p.RateBytesPerSec = nil, nil, nil
		if len(p.Changes) > 0 {
			p.Progress = done * 100 / len(p.Changes)
		}
	}
	at := now
	p.ProgressAt = &at
	p.ETASeconds, p.ETABasis = estimate(p, now)
}

// copiedBytes is how much of a change's new replicas exists on their brokers,
// capped at the leader's size. Finished changes count in full.
func copiedBytes(c model.Change, x model.Partition, sizes map[string]map[int32]map[int32]int64) (int64, bool) {
	byBroker, ok := sizes[c.Topic][c.Partition]
	if !ok {
		return 0, false
	}
	full, ok := byBroker[x.Leader]
	if !ok {
		return 0, false
	}
	old := map[int32]bool{}
	for _, id := range c.Before {
		old[id] = true
	}
	var n int64
	for _, id := range c.After {
		if old[id] {
			continue
		}
		if c.FinishedAt != nil {
			n += full
		} else {
			n += min(byBroker[id], full)
		}
	}
	return n, true
}

// estimate prefers remaining bytes over the measured rate; without sizes it
// falls back to the script's method: average time per finished topic.
func estimate(p *model.Plan, now time.Time) (*int64, string) {
	if p.BytesTotal != nil && p.BytesDone != nil && p.RateBytesPerSec != nil && *p.RateBytesPerSec > 0 {
		left := (*p.BytesTotal - *p.BytesDone) / *p.RateBytesPerSec
		return &left, "bytes"
	}
	finished, remaining := 0, 0
	for _, s := range p.Steps {
		switch s.State {
		case model.StepDone, model.StepSkipped:
			finished++
		default:
			remaining++
		}
	}
	if finished == 0 || p.StartedAt.IsZero() {
		return nil, ""
	}
	left := int64(now.Sub(p.StartedAt).Seconds()) / int64(finished) * int64(remaining)
	return &left, "topics"
}
