package kafka

import (
	"context"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"sort"
	"strconv"
)

func (d *Demo) CreateTopic(ctx context.Context, in model.TopicCreate) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, t := range d.state.Topics {
		if t.Name == in.Name {
			return fmt.Errorf("topic exists")
		}
	}
	if int(in.ReplicationFactor) > len(d.state.Brokers) {
		return fmt.Errorf("replication exceeds broker count")
	}
	t := model.Topic{Name: in.Name, ReplicationFactor: int(in.ReplicationFactor), CleanupPolicy: in.Config["cleanup.policy"]}
	if t.CleanupPolicy == "" {
		t.CleanupPolicy = "delete"
	}
	if v, ok := in.Config["retention.ms"]; ok {
		r, e := strconv.ParseInt(v, 10, 64)
		if e != nil {
			return e
		}
		t.RetentionMs = &r
	}
	extra := map[string]string{}
	for k, v := range in.Config {
		if _, ok := demoTopicDefaults[k]; !ok {
			return fmt.Errorf("unknown topic config %q", k)
		}
		if k != "cleanup.policy" && k != "retention.ms" {
			extra[k] = v
		}
	}
	d.addDemoPartitions(&t, in.Partitions)
	d.state.Topics = append(d.state.Topics, t)
	d.topicConfigs[t.Name] = extra
	return nil
}
func (d *Demo) addDemoPartitions(t *model.Topic, count int32) {
	for i := int32(len(t.Partitions)); i < count; i++ {
		replicas := []int32{}
		for j := 0; j < t.ReplicationFactor; j++ {
			replicas = append(replicas, d.state.Brokers[(int(i)+j)%len(d.state.Brokers)].ID)
		}
		zero := int64(0)
		t.Partitions = append(t.Partitions, model.Partition{ID: i, Leader: replicas[0], Replicas: replicas, ISR: append([]int32{}, replicas...), StartOffset: &zero, EndOffset: &zero})
		d.messages[fmt.Sprintf("%s/%d", t.Name, i)] = []model.Message{}
	}
}
func (d *Demo) DeleteTopic(ctx context.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, t := range d.state.Topics {
		if t.Name == name {
			for _, p := range t.Partitions {
				delete(d.messages, fmt.Sprintf("%s/%d", name, p.ID))
			}
			d.state.Topics = append(d.state.Topics[:i], d.state.Topics[i+1:]...)
			delete(d.topicConfigs, name)
			return nil
		}
	}
	return fmt.Errorf("topic not found")
}

// demoTopicDefaults mirrors the topic-level configuration keys and defaults
// of Apache Kafka 3.7 so the simulator describes the same surface a broker does.
var demoTopicDefaults = map[string]string{
	"cleanup.policy":                          "delete",
	"compression.type":                        "producer",
	"delete.retention.ms":                     "86400000",
	"file.delete.delay.ms":                    "60000",
	"flush.messages":                          "9223372036854775807",
	"flush.ms":                                "9223372036854775807",
	"follower.replication.throttled.replicas": "",
	"index.interval.bytes":                    "4096",
	"leader.replication.throttled.replicas":   "",
	"local.retention.bytes":                   "-2",
	"local.retention.ms":                      "-2",
	"max.compaction.lag.ms":                   "9223372036854775807",
	"max.message.bytes":                       "1048588",
	"message.downconversion.enable":           "true",
	"message.timestamp.after.max.ms":          "9223372036854775807",
	"message.timestamp.before.max.ms":         "9223372036854775807",
	"message.timestamp.difference.max.ms":     "9223372036854775807",
	"message.timestamp.type":                  "CreateTime",
	"min.cleanable.dirty.ratio":               "0.5",
	"min.compaction.lag.ms":                   "0",
	"min.insync.replicas":                     "1",
	"preallocate":                             "false",
	"remote.storage.enable":                   "false",
	"retention.bytes":                         "-1",
	"retention.ms":                            "604800000",
	"segment.bytes":                           "1073741824",
	"segment.index.bytes":                     "10485760",
	"segment.jitter.ms":                       "0",
	"segment.ms":                              "604800000",
	"unclean.leader.election.enable":          "false",
}

func (d *Demo) TopicConfig(ctx context.Context, name string) ([]model.ConfigEntry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, t := range d.state.Topics {
		if t.Name != name {
			continue
		}
		overrides := map[string]string{}
		for k, v := range d.topicConfigs[name] {
			overrides[k] = v
		}
		if t.CleanupPolicy != "" && t.CleanupPolicy != demoTopicDefaults["cleanup.policy"] {
			overrides["cleanup.policy"] = t.CleanupPolicy
		}
		if t.RetentionMs != nil {
			overrides["retention.ms"] = strconv.FormatInt(*t.RetentionMs, 10)
		}
		out := []model.ConfigEntry{}
		for k, v := range demoTopicDefaults {
			value, override := overrides[k]
			source := "DYNAMIC_TOPIC_CONFIG"
			if !override {
				value, source = v, "DEFAULT_CONFIG"
			}
			out = append(out, model.ConfigEntry{Name: k, Value: &value, Source: source, Override: override})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		return out, nil
	}
	return nil, fmt.Errorf("topic not found")
}
func (d *Demo) AlterTopicConfig(ctx context.Context, name string, set map[string]string, reset []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, k := range append(keys(set), reset...) {
		if _, ok := demoTopicDefaults[k]; !ok {
			return fmt.Errorf("unknown topic config %q", k)
		}
	}
	for i := range d.state.Topics {
		t := &d.state.Topics[i]
		if t.Name != name {
			continue
		}
		var retention *int64
		if v, ok := set["retention.ms"]; ok {
			r, e := strconv.ParseInt(v, 10, 64)
			if e != nil {
				return e
			}
			retention = &r
		}
		overrides := d.topicConfigs[name]
		if overrides == nil {
			overrides = map[string]string{}
			d.topicConfigs[name] = overrides
		}
		for _, k := range reset {
			delete(overrides, k)
			switch k {
			case "cleanup.policy":
				t.CleanupPolicy = demoTopicDefaults[k]
			case "retention.ms":
				t.RetentionMs = nil
			}
		}
		for k, v := range set {
			switch k {
			case "cleanup.policy":
				t.CleanupPolicy = v
			case "retention.ms":
				t.RetentionMs = retention
			default:
				overrides[k] = v
			}
		}
		return nil
	}
	return fmt.Errorf("topic not found")
}
func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
func (d *Demo) IncreasePartitions(ctx context.Context, name string, count int32) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.state.Topics {
		t := &d.state.Topics[i]
		if t.Name == name {
			if int(count) <= len(t.Partitions) {
				return fmt.Errorf("partition count must increase")
			}
			d.addDemoPartitions(t, count)
			return nil
		}
	}
	return fmt.Errorf("topic not found")
}
func (d *Demo) GroupDetail(ctx context.Context, id string) (model.GroupDetail, error) {
	groups, e := d.Groups(ctx)
	if e != nil {
		return model.GroupDetail{}, e
	}
	for _, g := range groups {
		if g.ID == id {
			detail := model.GroupDetail{Group: g, Offsets: []model.GroupOffset{}}
			total := int64(0)
			s, _ := d.Snapshot(ctx)
			for _, t := range s.Topics {
				for _, name := range g.Topics {
					if t.Name != name {
						continue
					}
					for _, p := range t.Partitions {
						committed := *p.EndOffset - 1
						d.mu.Lock()
						if saved, ok := d.groupOffsets[fmt.Sprintf("%s/%s/%d", id, t.Name, p.ID)]; ok {
							committed = saved
						}
						d.mu.Unlock()
						if committed < 0 {
							committed = 0
						}
						detail.Offsets = append(detail.Offsets, model.GroupOffset{Topic: t.Name, Partition: p.ID, CommittedOffset: committed, StartOffset: *p.StartOffset, EndOffset: *p.EndOffset, Lag: *p.EndOffset - committed})
						total += *p.EndOffset - committed
					}
				}
			}
			detail.Lag = &total
			return detail, nil
		}
	}
	return model.GroupDetail{}, fmt.Errorf("group not found")
}
func (d *Demo) TopicConsumers(ctx context.Context, topic string) ([]model.GroupDetail, error) {
	groups, e := d.Groups(ctx)
	if e != nil {
		return nil, e
	}
	out := []model.GroupDetail{}
	for _, g := range groups {
		detail, e := d.GroupDetail(ctx, g.ID)
		if e != nil {
			return nil, e
		}
		offsets := []model.GroupOffset{}
		total := int64(0)
		for _, o := range detail.Offsets {
			if o.Topic == topic {
				offsets = append(offsets, o)
				total += o.Lag
			}
		}
		if len(offsets) == 0 {
			continue
		}
		detail.Topics, detail.Offsets, detail.Lag = []string{topic}, offsets, &total
		out = append(out, detail)
	}
	return out, nil
}
func (d *Demo) PreviewOffsets(ctx context.Context, id string, in model.OffsetReset) (model.OffsetPreview, error) {
	return previewOffsets(ctx, d, id, in)
}
func (d *Demo) ResetOffsets(ctx context.Context, id string, in model.OffsetReset) (model.OffsetPreview, error) {
	out, e := d.PreviewOffsets(ctx, id, in)
	if e != nil {
		return out, e
	}
	if in.Confirmation != id || in.PreviewHash != out.PreviewHash {
		return out, ErrStaleOffsets
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, v := range out.Changes {
		d.groupOffsets[fmt.Sprintf("%s/%s/%d", id, v.Topic, v.Partition)] = v.After
	}
	out.Applied = true
	return out, nil
}
