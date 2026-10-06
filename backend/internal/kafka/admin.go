package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
	"math"
	"sort"
	"time"
)

var ErrActiveGroup = errors.New("consumer group must be inactive")
var ErrStaleOffsets = errors.New("offset preview changed; preview again")

type AdminProvider interface {
	CreateTopic(context.Context, model.TopicCreate) error
	DeleteTopic(context.Context, string) error
	TopicConfig(context.Context, string) ([]model.ConfigEntry, error)
	AlterTopicConfig(context.Context, string, map[string]string, []string) error
	IncreasePartitions(context.Context, string, int32) error
	GroupDetail(context.Context, string) (model.GroupDetail, error)
	TopicConsumers(context.Context, string) ([]model.GroupDetail, error)
	PreviewOffsets(context.Context, string, model.OffsetReset) (model.OffsetPreview, error)
	ResetOffsets(context.Context, string, model.OffsetReset) (model.OffsetPreview, error)
}

func (n *Native) CreateTopic(ctx context.Context, in model.TopicCreate) error {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return e
	}
	defer done()
	cfg := map[string]*string{}
	for k, v := range in.Config {
		value := v
		cfg[k] = &value
	}
	out, e := n.admin.CreateTopics(c, in.Partitions, in.ReplicationFactor, cfg, in.Name)
	if e != nil {
		return e
	}
	n.invalidateAdminMetadata()
	return out.Error()
}
func (n *Native) DeleteTopic(ctx context.Context, name string) error {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return e
	}
	defer done()
	out, e := n.admin.DeleteTopics(c, name)
	if e != nil {
		return e
	}
	n.invalidateAdminMetadata()
	return out.Error()
}
func (n *Native) TopicConfig(ctx context.Context, name string) ([]model.ConfigEntry, error) {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	out, e := n.admin.DescribeTopicConfigs(c, name)
	if e != nil {
		return nil, e
	}
	cfg := []model.ConfigEntry{}
	for _, resource := range out {
		if resource.Err != nil {
			return nil, resource.Err
		}
		for _, v := range resource.Configs {
			entry := model.ConfigEntry{Name: v.Key, Source: v.Source.String(), Override: v.Source == kmsg.ConfigSourceDynamicTopicConfig, Sensitive: v.Sensitive}
			if !v.Sensitive {
				entry.Value = v.Value
			}
			cfg = append(cfg, entry)
		}
	}
	sort.Slice(cfg, func(i, j int) bool { return cfg[i].Name < cfg[j].Name })
	return cfg, nil
}
func (n *Native) AlterTopicConfig(ctx context.Context, name string, set map[string]string, reset []string) error {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return e
	}
	defer done()
	changes := []kadm.AlterConfig{}
	for k, v := range set {
		value := v
		changes = append(changes, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: &value})
	}
	for _, k := range reset {
		changes = append(changes, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: k})
	}
	out, e := n.admin.AlterTopicConfigs(c, changes, name)
	if e != nil {
		return e
	}
	for _, v := range out {
		if v.Err != nil {
			return v.Err
		}
	}
	return nil
}
func (n *Native) IncreasePartitions(ctx context.Context, name string, count int32) error {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return e
	}
	defer done()
	metadata, e := n.admin.Metadata(c, name)
	if e != nil {
		return e
	}
	topic, ok := metadata.Topics[name]
	if !ok {
		return fmt.Errorf("topic not found")
	}
	if topic.Err != nil {
		return topic.Err
	}
	if int(count) <= len(topic.Partitions) {
		return fmt.Errorf("partition count must increase")
	}
	out, e := n.admin.UpdatePartitions(c, int(count), name)
	if e != nil {
		return e
	}
	n.invalidateAdminMetadata()
	return out.Error()
}
func (n *Native) invalidateAdminMetadata() { n.mu.Lock(); n.expires = time.Time{}; n.mu.Unlock() }
func (n *Native) GroupDetail(ctx context.Context, id string) (model.GroupDetail, error) {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return model.GroupDetail{}, e
	}
	defer done()
	groups, e := n.admin.DescribeGroups(c, id)
	if e != nil {
		return model.GroupDetail{}, e
	}
	g, ok := groups[id]
	if !ok {
		return model.GroupDetail{}, fmt.Errorf("group not found")
	}
	if g.Err != nil {
		return model.GroupDetail{}, g.Err
	}
	committed, e := n.admin.FetchOffsets(c, id)
	if e != nil {
		return model.GroupDetail{}, e
	}
	topics := []string{}
	for topic := range committed {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	detail := model.GroupDetail{Group: model.Group{ID: id, State: g.State, Members: len(g.Members), Topics: topics}, Offsets: []model.GroupOffset{}}
	total := int64(0)
	if len(topics) == 0 {
		detail.Lag = &total
		return detail, nil
	}
	start, e := n.admin.ListStartOffsets(c, topics...)
	if e != nil {
		return detail, e
	}
	end, e := n.admin.ListEndOffsets(c, topics...)
	if e != nil {
		return detail, e
	}
	for topic, partitions := range committed {
		for partition, offset := range partitions {
			if offset.Err != nil {
				return detail, offset.Err
			}
			o, e := groupOffset(topic, partition, offset.At, start, end)
			if e != nil {
				return detail, e
			}
			total += o.Lag
			detail.Offsets = append(detail.Offsets, o)
		}
	}
	sortOffsets(detail.Offsets)
	detail.Lag = &total
	return detail, nil
}

// TopicConsumers returns every group with committed offsets on topic, limited to that topic's partitions.
func (n *Native) TopicConsumers(ctx context.Context, topic string) ([]model.GroupDetail, error) {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	groups, e := n.admin.DescribeGroups(c)
	if e != nil {
		return nil, e
	}
	ids := []string{}
	for id := range groups {
		ids = append(ids, id)
	}
	out := []model.GroupDetail{}
	if len(ids) == 0 {
		return out, nil
	}
	fetched := n.admin.FetchManyOffsets(c, ids...)
	if fetched.AllFailed() {
		return nil, fetched.Error()
	}
	var start, end kadm.ListedOffsets
	for _, id := range ids {
		f, ok := fetched[id]
		if !ok || f.Err != nil || len(f.Fetched[topic]) == 0 {
			continue
		}
		if start == nil {
			if start, e = n.admin.ListStartOffsets(c, topic); e != nil {
				return nil, e
			}
			if end, e = n.admin.ListEndOffsets(c, topic); e != nil {
				return nil, e
			}
		}
		g := groups[id]
		detail := model.GroupDetail{Group: model.Group{ID: id, State: g.State, Members: len(g.Members), Topics: []string{topic}}, Offsets: []model.GroupOffset{}}
		total := int64(0)
		for partition, offset := range f.Fetched[topic] {
			if offset.Err != nil {
				return nil, offset.Err
			}
			o, e := groupOffset(topic, partition, offset.At, start, end)
			if e != nil {
				return nil, e
			}
			total += o.Lag
			detail.Offsets = append(detail.Offsets, o)
		}
		sortOffsets(detail.Offsets)
		detail.Lag = &total
		out = append(out, detail)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func groupOffset(topic string, partition int32, committed int64, start, end kadm.ListedOffsets) (model.GroupOffset, error) {
	lo, lok := start.Lookup(topic, partition)
	hi, hok := end.Lookup(topic, partition)
	if !lok || !hok {
		return model.GroupOffset{}, fmt.Errorf("offset bounds unavailable")
	}
	if lo.Err != nil {
		return model.GroupOffset{}, lo.Err
	}
	if hi.Err != nil {
		return model.GroupOffset{}, hi.Err
	}
	lag := hi.Offset - committed
	if committed < 0 {
		lag = hi.Offset - lo.Offset
	}
	if lag < 0 {
		lag = 0
	}
	return model.GroupOffset{Topic: topic, Partition: partition, CommittedOffset: committed, StartOffset: lo.Offset, EndOffset: hi.Offset, Lag: lag}, nil
}
func sortOffsets(offsets []model.GroupOffset) {
	sort.Slice(offsets, func(i, j int) bool {
		a, b := offsets[i], offsets[j]
		if a.Topic == b.Topic {
			return a.Partition < b.Partition
		}
		return a.Topic < b.Topic
	})
}
func previewOffsets(ctx context.Context, p AdminProvider, id string, in model.OffsetReset) (model.OffsetPreview, error) {
	out := model.OffsetPreview{GroupID: id, Changes: []model.OffsetChange{}}
	detail, e := p.GroupDetail(ctx, id)
	if e != nil {
		return out, e
	}
	if detail.Members != 0 || (detail.State != "Empty" && detail.State != "Dead") {
		return out, ErrActiveGroup
	}
	selected := map[string]bool{}
	for _, topic := range in.Topics {
		selected[topic] = true
	}
	matched := map[string]bool{}
	for _, v := range detail.Offsets {
		if len(selected) > 0 && !selected[v.Topic] {
			continue
		}
		matched[v.Topic] = true
		after := int64(0)
		switch in.Mode {
		case "earliest":
			after = v.StartOffset
		case "latest":
			after = v.EndOffset
		case "absolute":
			after = in.Offset
		case "shift":
			if v.CommittedOffset < 0 {
				return out, fmt.Errorf("cannot shift an uncommitted offset")
			}
			if (in.Shift > 0 && v.CommittedOffset > math.MaxInt64-in.Shift) || (in.Shift < 0 && v.CommittedOffset < math.MinInt64-in.Shift) {
				return out, fmt.Errorf("offset overflow")
			}
			after = v.CommittedOffset + in.Shift
		case "timestamp":
			if in.Timestamp.IsZero() {
				return out, fmt.Errorf("timestamp required")
			}
			resolver, ok := p.(interface {
				OffsetAt(context.Context, string, int32, time.Time) (int64, error)
			})
			if !ok {
				return out, fmt.Errorf("timestamp lookup unavailable")
			}
			after, e = resolver.OffsetAt(ctx, v.Topic, v.Partition, in.Timestamp)
			if e != nil {
				return out, e
			}
		default:
			return out, fmt.Errorf("invalid reset mode")
		}
		if after < v.StartOffset || after > v.EndOffset {
			return out, fmt.Errorf("offset is outside retained partition bounds")
		}
		out.Changes = append(out.Changes, model.OffsetChange{Topic: v.Topic, Partition: v.Partition, Before: v.CommittedOffset, After: after})
	}
	if len(out.Changes) == 0 {
		return out, fmt.Errorf("no committed partitions selected")
	}
	for topic := range selected {
		if !matched[topic] {
			return out, fmt.Errorf("selected topic has no committed offsets")
		}
	}
	data, _ := json.Marshal(struct {
		ID      string
		Changes []model.OffsetChange
	}{id, out.Changes})
	out.PreviewHash = fmt.Sprintf("%x", sha256.Sum256(data))
	return out, nil
}
func (n *Native) PreviewOffsets(ctx context.Context, id string, in model.OffsetReset) (model.OffsetPreview, error) {
	return previewOffsets(ctx, n, id, in)
}
func (n *Native) ResetOffsets(ctx context.Context, id string, in model.OffsetReset) (model.OffsetPreview, error) {
	out, e := n.PreviewOffsets(ctx, id, in)
	if e != nil {
		return out, e
	}
	if in.Confirmation != id || in.PreviewHash != out.PreviewHash {
		return out, ErrStaleOffsets
	}
	c, done, e := n.bounded(ctx)
	if e != nil {
		return out, e
	}
	defer done()
	offsets := kadm.Offsets{}
	for _, v := range out.Changes {
		offsets.Add(kadm.Offset{Topic: v.Topic, Partition: v.Partition, At: v.After, LeaderEpoch: -1})
	}
	result, e := n.admin.CommitOffsets(c, id, offsets)
	if e != nil {
		return out, e
	}
	if e = result.Error(); e != nil {
		return out, e
	}
	out.Applied = true
	return out, nil
}
