package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// LeaderElector moves leadership back to each partition's preferred (first)
// replica, as kafka-leader-election.sh --election-type PREFERRED does.
type LeaderElector interface {
	ElectPreferredLeaders(context.Context, []model.Change) error
}

// ReplicaSizer reports replica bytes as topic -> partition -> broker -> bytes,
// or nil when sizes are unavailable.
type ReplicaSizer interface {
	ReplicaSizes(context.Context) map[string]map[int32]map[int32]int64
}

func (n *Native) ElectPreferredLeaders(ctx context.Context, changes []model.Change) error {
	if len(changes) == 0 {
		return nil
	}
	c, done, e := n.bounded(ctx)
	if e != nil {
		return e
	}
	defer done()
	set := kadm.TopicsSet{}
	for _, x := range changes {
		set.Add(x.Topic, x.Partition)
	}
	results, e := n.admin.ElectLeaders(c, kadm.ElectPreferredReplica, set)
	n.mu.Lock()
	n.expires = time.Time{}
	n.mu.Unlock()
	if e != nil {
		return e
	}
	return electionError(results)
}

// electionError keeps only real failures: a partition already led by its
// preferred replica reports ELECTION_NOT_NEEDED, which is success.
func electionError(results kadm.ElectLeadersResults) error {
	failed := []string{}
	for topic, parts := range results {
		for partition, r := range parts {
			if r.Err == nil || errors.Is(r.Err, kerr.ElectionNotNeeded) {
				continue
			}
			failed = append(failed, fmt.Sprintf("%s-%d: %v", topic, partition, r.Err))
		}
	}
	if len(failed) == 0 {
		return nil
	}
	sort.Strings(failed)
	return fmt.Errorf("preferred leader election failed for %s", strings.Join(failed, "; "))
}

// ReplicaSizes serves the cached log-dir observation; callers must not modify it.
func (n *Native) ReplicaSizes(ctx context.Context) map[string]map[int32]map[int32]int64 {
	s := n.sizes(ctx)
	if s == nil {
		return nil
	}
	return s.replicas
}
