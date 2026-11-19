package api

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
)

// slowCluster simulates broker round trips so evaluation time can be measured
// without a real cluster. Every read costs latency.
type slowCluster struct {
	kafka.Provider
	topics   int
	latency  time.Duration
	inflight atomic.Int32
	peak     atomic.Int32
}

func (s *slowCluster) wait(ctx context.Context) error {
	n := s.inflight.Add(1)
	defer s.inflight.Add(-1)
	for {
		p := s.peak.Load()
		if n <= p || s.peak.CompareAndSwap(p, n) {
			break
		}
	}
	select {
	case <-time.After(s.latency):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *slowCluster) Snapshot(context.Context) (model.Snapshot, error) {
	out := model.Snapshot{}
	for i := 0; i < s.topics; i++ {
		out.Topics = append(out.Topics, model.Topic{Name: fmt.Sprintf("topic.%04d", i)})
	}
	return out, nil
}

func (s *slowCluster) GroupUsage(context.Context) (map[string][]model.TopicGroupUse, error) {
	return map[string][]model.TopicGroupUse{}, nil
}

func (s *slowCluster) FreshOffsets(ctx context.Context, _ string) (map[int32][2]int64, error) {
	return map[int32][2]int64{0: {0, 10}}, s.wait(ctx)
}

func (s *slowCluster) MessagesAt(ctx context.Context, topic string, from map[int32]int64, _ int) ([]model.Message, error) {
	if e := s.wait(ctx); e != nil {
		return nil, e
	}
	out := []model.Message{}
	for p := range from {
		out = append(out, model.Message{Topic: topic, Partition: p, Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)})
	}
	return out, nil
}

func TestEvaluationRunsConcurrentlyAndReportsProgress(t *testing.T) {
	const topics, latency = 120, 20 * time.Millisecond
	s := &slowCluster{topics: topics, latency: latency}
	a := New(Options{})
	var last atomic.Int64
	start := time.Now()
	rows, partial, reason, e := a.evaluateUnusedWith(context.Background(), s, time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), 24*time.Hour, false, nil, unusedEval{timeout: 30 * time.Second, maxCandidates: 1000, progress: func(done, total int) {
		for {
			cur := last.Load()
			if int64(done) <= cur || last.CompareAndSwap(cur, int64(done)) {
				break
			}
		}
	}})
	elapsed := time.Since(start)
	if e != nil || partial {
		t.Fatalf("err %v partial %v %s", e, partial, reason)
	}
	if len(rows) != topics || rows[0].Name != "topic.0000" || rows[topics-1].Name != fmt.Sprintf("topic.%04d", topics-1) {
		t.Fatalf("rows %d, order not preserved", len(rows))
	}
	if last.Load() != topics {
		t.Fatalf("final progress %d", last.Load())
	}
	sequential := time.Duration(topics) * 2 * latency
	t.Logf("%d topics: %v concurrent vs %v sequential (%.1fx), peak in flight %d", topics, elapsed.Round(time.Millisecond), sequential, float64(sequential)/float64(elapsed), s.peak.Load())
	if s.peak.Load() > unusedWorkers {
		t.Fatalf("peak %d exceeds %d workers", s.peak.Load(), unusedWorkers)
	}
	if elapsed > sequential/3 {
		t.Fatalf("not concurrent: %v vs sequential %v", elapsed, sequential)
	}
}

func (s *slowCluster) OffsetsAt(context.Context, string, time.Time) (map[int32]int64, error) {
	return nil, nil
}
