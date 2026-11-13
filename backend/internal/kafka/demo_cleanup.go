package kafka

import (
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"time"
)

// cleanupFixture describes one simulated topic for the topic cleanup view.
// filled lists the partitions that hold records; the rest stay empty.
type cleanupFixture struct {
	name       string
	partitions int
	filled     int
	records    int
	lastWrite  time.Time
	group      string
}

// Fixed dates keep the simulator deterministic: the topics only get older, so
// each stays in its classification for any realistic minimum age.
var cleanupFixtures = []cleanupFixture{
	{name: "tmp.load-test", partitions: 3},
	{name: "scratch.schema-poc", partitions: 1},
	{name: "legacy.billing.v1", partitions: 4, filled: 4, records: 40, lastWrite: time.Date(2026, 3, 14, 9, 30, 0, 0, time.UTC)},
	{name: "test.consumer-smoke", partitions: 4, filled: 2, records: 10, lastWrite: time.Date(2026, 8, 2, 16, 5, 0, 0, time.UTC)},
	{name: "reports.nightly-export", partitions: 3, filled: 3, records: 30, lastWrite: time.Date(2026, 8, 30, 3, 0, 0, 0, time.UTC), group: "demo-reports-batch"},
	{name: "archive.clickstream-2025", partitions: 6, filled: 6, records: 60, lastWrite: time.Date(2026, 5, 20, 22, 45, 0, 0, time.UTC), group: "demo-archive-loader"},
}

// seedCleanupFixtures adds unused, likely-unused and idle topics, plus the
// empty consumer groups that make a topic idle. Callers hold no lock.
func (d *Demo) seedCleanupFixtures() {
	for n, f := range cleanupFixtures {
		t := model.Topic{Name: f.name, ReplicationFactor: 3, CleanupPolicy: "delete"}
		total := int64(0)
		for p := 0; p < f.partitions; p++ {
			r := []int32{int32(1 + (p+n)%6), int32(1 + (p+n+1)%6), int32(1 + (p+n+2)%6)}
			start, end, size := int64(0), int64(0), int64(0)
			key := fmt.Sprintf("%s/%d", f.name, p)
			d.messages[key] = []model.Message{}
			if p < f.filled {
				end = int64(f.records)
				size = end * 2048
				for j := 0; j < f.records; j++ {
					at := f.lastWrite.Add(-time.Duration(f.records-1-j) * time.Minute)
					d.messages[key] = append(d.messages[key], model.Message{Topic: f.name, Partition: int32(p), Offset: int64(j), Key: fmt.Sprintf("k-%d", j), Value: fmt.Sprintf(`{"seq":%d,"demo":true}`, j), Timestamp: at, Headers: []model.Header{{Key: "source", Value: "development-simulator"}}})
				}
			}
			total += size
			t.Partitions = append(t.Partitions, model.Partition{ID: int32(p), Leader: r[0], Replicas: r, ISR: append([]int32{}, r...), StartOffset: &start, EndOffset: &end, SizeBytes: &size})
		}
		t.SizeBytes = &total
		retention := int64(604800000)
		t.RetentionMs = &retention
		d.state.Topics = append(d.state.Topics, t)
		if f.group != "" {
			d.extraGroups = append(d.extraGroups, model.Group{ID: f.group, State: "Empty", Members: 0, Topics: []string{f.name}})
		}
	}
}
