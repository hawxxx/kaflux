package kafka

import (
	"context"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"sort"
)

// FreshOffsets reads start and end offsets from the brokers, not the snapshot cache.
func (n *Native) FreshOffsets(ctx context.Context, topic string) (map[int32][2]int64, error) {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	start, e := n.admin.ListStartOffsets(c, topic)
	if e != nil {
		return nil, e
	}
	end, e := n.admin.ListEndOffsets(c, topic)
	if e != nil {
		return nil, e
	}
	if e = start.Error(); e != nil {
		return nil, e
	}
	if e = end.Error(); e != nil {
		return nil, e
	}
	out := map[int32][2]int64{}
	for partition, lo := range start[topic] {
		hi, ok := end.Lookup(topic, partition)
		if !ok {
			return nil, fmt.Errorf("end offset missing for %s/%d", topic, partition)
		}
		out[partition] = [2]int64{lo.Offset, hi.Offset}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("topic %s has no partitions", topic)
	}
	return out, nil
}

// GroupUsage lists every consumer group referencing each topic through members
// (assignments and subscriptions, including Stable groups that never committed)
// or committed offsets. Any failure describing or fetching returns an error.
func (n *Native) GroupUsage(ctx context.Context) (map[string][]model.TopicGroupUse, error) {
	c, done, e := n.bounded(ctx)
	if e != nil {
		return nil, e
	}
	defer done()
	described, e := n.admin.DescribeGroups(c)
	if e != nil {
		return nil, e
	}
	ids := make([]string, 0, len(described))
	for id := range described {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var tooMany error
	if len(ids) > MaxUsageGroups {
		ids, tooMany = ids[:MaxUsageGroups], ErrTooManyGroups
	}
	out := map[string][]model.TopicGroupUse{}
	if len(ids) == 0 {
		return out, tooMany
	}
	fetched := n.admin.FetchManyOffsets(c, ids...)
	for _, id := range ids {
		g := described[id]
		if g.Err != nil {
			return nil, g.Err
		}
		f, ok := fetched[id]
		if !ok {
			return nil, fmt.Errorf("offsets for group %s were not returned", id)
		}
		if f.Err != nil {
			return nil, f.Err
		}
		views := make([]memberView, 0, len(g.Members))
		for _, m := range g.Members {
			var v memberView
			if a, ok := m.Assigned.AsConsumer(); ok {
				v.assignedOK = true
				for _, t := range a.Topics {
					v.assigned = append(v.assigned, t.Topic)
				}
			}
			if j, ok := m.Join.AsConsumer(); ok {
				v.subscribedOK = true
				v.subscribed = j.Topics
			}
			views = append(views, v)
		}
		members, e := usageFromMembers(g.State, views)
		if e != nil {
			return nil, fmt.Errorf("group %s: %w", id, e)
		}
		committed := map[string]bool{}
		for topic, parts := range f.Fetched {
			for _, o := range parts {
				if o.Err != nil {
					return nil, o.Err
				}
				if o.At >= 0 {
					committed[topic] = true
				}
			}
		}
		topics := map[string]bool{}
		for t := range members {
			topics[t] = true
		}
		for t := range committed {
			topics[t] = true
		}
		for t := range topics {
			out[t] = append(out[t], model.TopicGroupUse{Group: id, State: g.State, Members: len(g.Members), HasCommittedOffsets: committed[t], AssignedMembers: members[t]})
		}
	}
	return out, tooMany
}

// FreshOffsets reads the demo state directly.
func (d *Demo) FreshOffsets(_ context.Context, topic string) (map[int32][2]int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, t := range d.state.Topics {
		if t.Name != topic {
			continue
		}
		out := map[int32][2]int64{}
		for _, p := range t.Partitions {
			if p.StartOffset == nil || p.EndOffset == nil {
				return nil, fmt.Errorf("offsets unavailable")
			}
			out[p.ID] = [2]int64{*p.StartOffset, *p.EndOffset}
		}
		return out, nil
	}
	return nil, fmt.Errorf("topic not found")
}

// GroupUsage reports the demo groups, each assigning all its members to every topic it reads.
func (d *Demo) GroupUsage(ctx context.Context) (map[string][]model.TopicGroupUse, error) {
	groups, e := d.Groups(ctx)
	if e != nil {
		return nil, e
	}
	out := map[string][]model.TopicGroupUse{}
	for _, g := range groups {
		detail, e := d.GroupDetail(ctx, g.ID)
		if e != nil {
			return nil, e
		}
		committed := map[string]bool{}
		for _, o := range detail.Offsets {
			committed[o.Topic] = true
		}
		for _, t := range g.Topics {
			out[t] = append(out[t], model.TopicGroupUse{Group: g.ID, State: g.State, Members: g.Members, HasCommittedOffsets: committed[t], AssignedMembers: g.Members})
		}
	}
	return out, nil
}

// memberView is one group member's parsed consumer-protocol data.
type memberView struct {
	assigned     []string
	assignedOK   bool
	subscribed   []string
	subscribedOK bool
}

// usageFromMembers counts, per topic, the members assigned to or subscribed to
// it. A member whose data cannot be parsed as consumer protocol could be reading
// anything, so in a live group that is an error; Empty and Dead groups have no
// members to worry about.
func usageFromMembers(state string, members []memberView) (map[string]int, error) {
	out := map[string]int{}
	if state == "Empty" || state == "Dead" {
		return out, nil
	}
	for _, m := range members {
		if !m.assignedOK && !m.subscribedOK {
			return nil, fmt.Errorf("member assignment cannot be parsed in state %s", state)
		}
		seen := map[string]bool{}
		for _, t := range m.assigned {
			seen[t] = true
		}
		for _, t := range m.subscribed {
			seen[t] = true
		}
		for t := range seen {
			out[t]++
		}
	}
	return out, nil
}
