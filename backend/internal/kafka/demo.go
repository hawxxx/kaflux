package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"sync"
	"time"
)

type Demo struct {
	mu           sync.Mutex
	state        model.Snapshot
	messages     map[string][]model.Message
	groupOffsets map[string]int64
	acls         map[string]model.ACL
	// topicConfigs holds per-topic overrides beyond the cleanup policy and
	// retention already modelled on model.Topic.
	topicConfigs map[string]map[string]string
}

func NewDemo() *Demo {
	d := &Demo{messages: map[string][]model.Message{}, groupOffsets: map[string]int64{}, topicConfigs: map[string]map[string]string{}}
	controller := int32(1)
	d.state.Controller = &controller
	for i := int32(1); i <= 6; i++ {
		d.state.Brokers = append(d.state.Brokers, model.Broker{ID: i, Host: fmt.Sprintf("demo-broker-%d", i), Port: 9092, Rack: fmt.Sprintf("zone-%d", (i-1)%3)})
	}
	for n, name := range []string{"orders.created", "payments.authorized", "inventory.updated", "notifications.email", "events.audit", "shipping.dispatched"} {
		t := model.Topic{Name: name, ReplicationFactor: 3, CleanupPolicy: "delete"}
		for p := int32(0); p < 12; p++ {
			r := []int32{1 + p%6, 1 + (p+1)%6, 1 + (p+2)%6}
			start, end := int64(0), int64(24)
			// Explicit simulator metadata models historical storage independently of the bounded message fixture.
			size := int64(n+1)*128*1024*1024 + int64(p)*16*1024*1024
			t.Partitions = append(t.Partitions, model.Partition{ID: p, Leader: r[0], Replicas: r, ISR: append([]int32{}, r...), StartOffset: &start, EndOffset: &end, SizeBytes: &size})
			for j := 0; j < 24; j++ {
				v := fmt.Sprintf(`{"eventId":"demo-%d-%d-%d","orderId":"ORD-%06d","amount":%.2f,"currency":"USD","status":"confirmed","demo":true}`, n, p, j, n*1000+int(p)*24+j, float64(j+1)*19.95)
				k := fmt.Sprintf("%s/%d", name, p)
				d.messages[k] = append(d.messages[k], model.Message{Topic: name, Partition: p, Offset: int64(j), Key: fmt.Sprintf("order-%d", j), Value: v, Timestamp: time.Date(2026, 10, 1, 12, 0, j, 0, time.UTC), Headers: []model.Header{{Key: "source", Value: "development-simulator"}}})
			}
		}
		total := int64(0)
		for _, p := range t.Partitions {
			total += *p.SizeBytes
		}
		t.SizeBytes = &total
		retention := int64(604800000)
		t.RetentionMs = &retention
		d.state.Topics = append(d.state.Topics, t)
	}
	return d
}
func (d *Demo) Snapshot(ctx context.Context) (model.Snapshot, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	b, _ := json.Marshal(d.state)
	var s model.Snapshot
	_ = json.Unmarshal(b, &s)
	s.ObservedAt = time.Now().UTC()
	for i := range s.Topics {
		s.Topics[i].ObservedAt = s.ObservedAt
	}
	for i := range s.Brokers {
		size := int64(0)
		for _, t := range s.Topics {
			for _, p := range t.Partitions {
				if p.Leader == s.Brokers[i].ID {
					s.Brokers[i].Leaders++
				}
				for _, r := range p.Replicas {
					if r == s.Brokers[i].ID {
						s.Brokers[i].Partitions++
						if p.SizeBytes != nil {
							size += *p.SizeBytes
						}
					}
				}
			}
		}
		s.Brokers[i].SizeBytes = &size
	}
	return s, nil
}
func (d *Demo) Groups(context.Context) ([]model.Group, error) {
	l := int64(17)
	return []model.Group{{ID: "demo-order-service", State: "Stable", Members: 3, Topics: []string{"orders.created"}, Lag: &l}, {ID: "demo-analytics", State: "Stable", Members: 2, Topics: []string{"payments.authorized", "events.audit"}, Lag: &l}, {ID: "demo-inactive", State: "Empty", Members: 0, Topics: []string{"orders.created"}, Lag: &l}}, nil
}
func (d *Demo) Messages(ctx context.Context, t string, p int32, o int64, l int) ([]model.Message, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	all, ok := d.messages[fmt.Sprintf("%s/%d", t, p)]
	if !ok {
		return nil, fmt.Errorf("unknown topic partition")
	}
	out := []model.Message{}
	for _, m := range all {
		if m.Offset >= o && len(out) < l {
			m.KeyBase64 = base64.StdEncoding.EncodeToString([]byte(m.Key))
			m.ValueBase64 = base64.StdEncoding.EncodeToString([]byte(m.Value))
			out = append(out, m)
		}
	}
	return out, nil
}
func (d *Demo) Produce(ctx context.Context, m model.Message) (model.Message, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	k := fmt.Sprintf("%s/%d", m.Topic, m.Partition)
	all, ok := d.messages[k]
	if !ok {
		return m, fmt.Errorf("unknown topic partition")
	}
	m.Offset = int64(len(all))
	for _, t := range d.state.Topics {
		for _, p := range t.Partitions {
			if t.Name == m.Topic && p.ID == m.Partition && p.EndOffset != nil {
				m.Offset = *p.EndOffset
			}
		}
	}
	m.Timestamp = time.Now().UTC()
	m.KeyBase64 = base64.StdEncoding.EncodeToString([]byte(m.Key))
	m.ValueBase64 = base64.StdEncoding.EncodeToString([]byte(m.Value))
	d.messages[k] = append(all, m)
	for i := range d.state.Topics {
		if d.state.Topics[i].Name == m.Topic {
			for j := range d.state.Topics[i].Partitions {
				if d.state.Topics[i].Partitions[j].ID == m.Partition {
					v := m.Offset + 1
					d.state.Topics[i].Partitions[j].EndOffset = &v
				}
			}
		}
	}
	return m, nil
}
func (d *Demo) Reassign(ctx context.Context, c []model.Change) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, x := range c {
		for i := range d.state.Topics {
			if d.state.Topics[i].Name == x.Topic {
				for j := range d.state.Topics[i].Partitions {
					p := &d.state.Topics[i].Partitions[j]
					if p.ID == x.Partition {
						p.Replicas = append([]int32{}, x.After...)
						p.ISR = append([]int32{}, x.After...)
						p.Leader = x.After[0]
					}
				}
			}
		}
	}
	return nil
}
func (d *Demo) Pending(context.Context) (map[string][]int32, error) { return map[string][]int32{}, nil }
func (d *Demo) Close()                                              {}
func (d *Demo) Simulated() bool                                     { return true }
