package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
)

const (
	// sizeTTL bounds how often DescribeLogDirs runs. It is a heavy call: every
	// broker reports every partition it hosts, so the cost grows with the
	// number of partitions in the cluster.
	sizeTTL = 15 * time.Second
	// sizeMaxAge is how long the last good result is served after a refresh
	// fails. Beyond it sizes revert to unknown rather than show old data.
	sizeMaxAge = 5 * time.Minute
	// sizeRetryDelay spaces out refresh attempts after a failure.
	sizeRetryDelay = 15 * time.Second
)

// logSizes holds one DescribeLogDirs observation.
//
// brokers is physical storage: every replica on the broker, including hidden
// topics. A broker is present only when all of its log directories were
// described.
//
// replicas maps topic -> partition -> broker -> replica bytes, excluding future
// (same-broker directory move) copies. It drives the per-partition size, which
// stays one replica's worth because the balance analysis and reassignment
// planner multiply it by the replicas they place.
//
// topics is the all-replica total per topic, which is what Kafka admin tools
// report as a topic's size, future copies included because they occupy disk.
type logSizes struct {
	observedAt time.Time
	brokers    map[int32]int64
	replicas   map[string]map[int32]map[int32]int64
	topics     map[string]int64
}

func collectLogSizes(all kadm.DescribedAllLogDirs, observedAt time.Time) *logSizes {
	s := &logSizes{observedAt: observedAt, brokers: map[int32]int64{}, replicas: map[string]map[int32]map[int32]int64{}, topics: map[string]int64{}}
	for broker, dirs := range all {
		complete := len(dirs) > 0
		var total int64
		for _, dir := range dirs {
			if dir.Err != nil {
				complete = false
				continue
			}
			dir.Topics.Each(func(p kadm.DescribedLogDirPartition) {
				total += p.Size
				s.topics[p.Topic] += p.Size
				if p.IsFuture {
					return
				}
				parts, ok := s.replicas[p.Topic]
				if !ok {
					parts = map[int32]map[int32]int64{}
					s.replicas[p.Topic] = parts
				}
				byBroker, ok := parts[p.Partition]
				if !ok {
					byBroker = map[int32]int64{}
					parts[p.Partition] = byBroker
				}
				byBroker[broker] = p.Size
			})
		}
		if complete {
			s.brokers[broker] = total
		}
	}
	return s
}

// partitionSize is the leader replica's size, which approximates the logical
// partition size. When the leader did not report (offline, or its broker
// failed to answer) the largest reporting replica is used. No report at all
// means unknown.
func (s *logSizes) partitionSize(topic string, p model.Partition) (int64, bool) {
	byBroker := s.replicas[topic][p.ID]
	if v, ok := byBroker[p.Leader]; ok {
		return v, true
	}
	var best int64
	found := false
	for _, v := range byBroker {
		if !found || v > best {
			best, found = v, true
		}
	}
	return best, found
}

// apply fills SizeBytes on brokers, partitions and topics. Anything not
// reported stays nil so the UI shows it as unknown rather than zero.
//
// A partition's size is one replica's bytes (see partitionSize). A topic's size
// is the sum over all of its replicas, as Kafka admin tools report it, and is
// known only when every replica the metadata lists has reported:
// a partial sum would silently understate the topic.
func (s *logSizes) apply(snap *model.Snapshot) {
	if s == nil {
		return
	}
	for i := range snap.Brokers {
		if v, ok := s.brokers[snap.Brokers[i].ID]; ok {
			size := v
			snap.Brokers[i].SizeBytes = &size
		}
	}
	for ti := range snap.Topics {
		t := &snap.Topics[ti]
		complete := len(t.Partitions) > 0
		for pi := range t.Partitions {
			p := &t.Partitions[pi]
			if v, ok := s.partitionSize(t.Name, *p); ok {
				size := v
				p.SizeBytes = &size
			}
			reported := s.replicas[t.Name][p.ID]
			for _, r := range p.Replicas {
				if _, ok := reported[r]; !ok {
					complete = false
				}
			}
		}
		if complete {
			size := s.topics[t.Name]
			t.SizeBytes = &size
		}
	}
}

// sizes returns the current log-dir observation, refreshing it when stale.
// It never fails: when the brokers cannot be described it serves the last good
// observation for a bounded time and otherwise reports nil (sizes unknown).
func (n *Native) sizes(ctx context.Context) *logSizes {
	n.sizeMu.Lock()
	defer n.sizeMu.Unlock()
	now := time.Now()
	if n.sizeCache != nil && now.Sub(n.sizeCache.observedAt) < sizeTTL {
		return n.sizeCache
	}
	if now.Before(n.sizeRetryAt) {
		return n.staleSizes(now)
	}
	c, done, e := n.bounded(ctx)
	if e != nil {
		return n.staleSizes(now)
	}
	defer done()
	all, e := n.admin.DescribeAllLogDirs(c, nil)
	// A ShardErrors result with some answers means a broker (for example one
	// restarting) did not respond; the others are valid and that broker stays unknown.
	var shardErrs *kadm.ShardErrors
	if e != nil && !(errors.As(e, &shardErrs) && len(all) > 0) {
		n.sizeRetryAt = now.Add(sizeRetryDelay)
		return n.staleSizes(now)
	}
	n.sizeCache = collectLogSizes(all, time.Now().UTC())
	return n.sizeCache
}

func (n *Native) staleSizes(now time.Time) *logSizes {
	if n.sizeCache != nil && now.Sub(n.sizeCache.observedAt) < sizeMaxAge {
		return n.sizeCache
	}
	return nil
}
