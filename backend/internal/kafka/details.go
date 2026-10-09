package kafka

import (
	"context"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"strconv"
	"time"
)

func (n *Native) TopicDetail(ctx context.Context, name string) (model.Topic, error) {
	s, e := n.Snapshot(ctx)
	if e != nil {
		return model.Topic{}, e
	}
	var t model.Topic
	found := false
	for _, x := range s.Topics {
		if x.Name == name {
			t = x
			t.Partitions = append([]model.Partition{}, x.Partitions...)
			found = true
			break
		}
	}
	if !found {
		return t, fmt.Errorf("topic not found")
	}
	c, done, e := n.bounded(ctx)
	if e != nil {
		return t, e
	}
	defer done()
	start, e1 := n.admin.ListStartOffsets(c, name)
	end, e2 := n.admin.ListEndOffsets(c, name)
	for i := range t.Partitions {
		if e1 == nil {
			if x, ok := start.Lookup(name, t.Partitions[i].ID); ok && x.Err == nil {
				v := x.Offset
				t.Partitions[i].StartOffset = &v
			}
		}
		if e2 == nil {
			if x, ok := end.Lookup(name, t.Partitions[i].ID); ok && x.Err == nil {
				v := x.Offset
				t.Partitions[i].EndOffset = &v
			}
		}
	}
	configs, e := n.admin.DescribeTopicConfigs(c, name)
	if e == nil {
		for _, resource := range configs {
			if resource.Err != nil {
				continue
			}
			for _, cfg := range resource.Configs {
				if cfg.Value == nil {
					continue
				}
				switch cfg.Key {
				case "cleanup.policy":
					t.CleanupPolicy = *cfg.Value
				case "retention.ms":
					if v, e := strconv.ParseInt(*cfg.Value, 10, 64); e == nil {
						t.RetentionMs = &v
					}
				}
			}
		}
	}
	return t, nil
}
func (n *Native) OffsetAt(ctx context.Context, topic string, partition int32, at time.Time) (int64, error) {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return 0, e
	}
	defer done()
	offsets, e := n.admin.ListOffsetsAfterMilli(c, at.UnixMilli(), topic)
	if e != nil {
		return 0, e
	}
	o, ok := offsets.Lookup(topic, partition)
	if !ok {
		return 0, fmt.Errorf("partition not found")
	}
	if o.Err != nil {
		return 0, o.Err
	}
	if o.Offset < 0 {
		end, e := n.admin.ListEndOffsets(c, topic)
		if e != nil {
			return 0, e
		}
		x, ok := end.Lookup(topic, partition)
		if !ok || x.Err != nil {
			return 0, fmt.Errorf("partition end offset unavailable")
		}
		return x.Offset, nil
	}
	return o.Offset, nil
}
func (n *Native) OffsetsAt(ctx context.Context, topic string, at time.Time) (map[int32]int64, error) {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	offsets, e := n.admin.ListOffsetsAfterMilli(c, at.UnixMilli(), topic)
	if e != nil {
		return nil, e
	}
	// Partitions with nothing at or after the time report -1; they resume at their end.
	var end kadm.ListedOffsets
	loaded := false
	out := map[int32]int64{}
	offsets.Each(func(o kadm.ListedOffset) {
		if e != nil {
			return
		}
		if o.Err != nil {
			e = o.Err
		} else if o.Offset >= 0 {
			out[o.Partition] = o.Offset
		} else {
			if !loaded {
				end, e = n.admin.ListEndOffsets(c, topic)
				loaded = true
			}
			if e != nil {
				return
			}
			x, ok := end.Lookup(topic, o.Partition)
			if !ok || x.Err != nil {
				e = fmt.Errorf("partition end offset unavailable")
				return
			}
			out[o.Partition] = x.Offset
		}
	})
	return out, e
}
func (d *Demo) OffsetAt(ctx context.Context, topic string, partition int32, at time.Time) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	messages, ok := d.messages[fmt.Sprintf("%s/%d", topic, partition)]
	if !ok {
		return 0, fmt.Errorf("partition not found")
	}
	for _, m := range messages {
		if !m.Timestamp.Before(at) {
			return m.Offset, nil
		}
	}
	return int64(len(messages)), nil
}
