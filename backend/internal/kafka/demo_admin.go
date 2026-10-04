package kafka

import (
	"context"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
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
	d.addDemoPartitions(&t, in.Partitions)
	d.state.Topics = append(d.state.Topics, t)
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
			return nil
		}
	}
	return fmt.Errorf("topic not found")
}
func (d *Demo) TopicConfig(ctx context.Context, name string) (map[string]string, error) {
	s, e := d.Snapshot(ctx)
	if e != nil {
		return nil, e
	}
	for _, t := range s.Topics {
		if t.Name == name {
			cfg := map[string]string{"cleanup.policy": t.CleanupPolicy}
			if t.RetentionMs != nil {
				cfg["retention.ms"] = strconv.FormatInt(*t.RetentionMs, 10)
			}
			return cfg, nil
		}
	}
	return nil, fmt.Errorf("topic not found")
}
func (d *Demo) AlterTopicConfig(ctx context.Context, name string, cfg map[string]string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.state.Topics {
		t := &d.state.Topics[i]
		if t.Name == name {
			if v, ok := cfg["cleanup.policy"]; ok {
				t.CleanupPolicy = v
			}
			if v, ok := cfg["retention.ms"]; ok {
				r, e := strconv.ParseInt(v, 10, 64)
				if e != nil {
					return e
				}
				t.RetentionMs = &r
			}
			return nil
		}
	}
	return fmt.Errorf("topic not found")
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
