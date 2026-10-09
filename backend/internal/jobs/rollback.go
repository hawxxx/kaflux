package jobs

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/balance"
	"github.com/hawxxx/kaflux/backend/internal/model"
)

// ErrNothingToRollBack means every partition is already at its original assignment.
var ErrNothingToRollBack = errors.New("every partition is already at its original assignment")

// RollbackPlan builds a planned job that restores the original assignments of
// the partitions p actually moved, in reverse topic order. It refuses when an
// original broker is gone, because the original placement cannot be restored.
func RollbackPlan(snap model.Snapshot, p model.Plan, actor string) (model.Plan, error) {
	brokers := map[int32]bool{}
	for _, b := range snap.Brokers {
		brokers[b.ID] = true
	}
	current := partitionIndex(snap)
	changes := []model.Change{}
	for _, c := range p.Changes {
		x, ok := current[changeKey(c.Topic, c.Partition)]
		if !ok {
			return model.Plan{}, fmt.Errorf("partition %s/%d no longer exists", c.Topic, c.Partition)
		}
		if reflect.DeepEqual(x.Replicas, c.Before) {
			continue
		}
		for _, b := range c.Before {
			if !brokers[b] {
				return model.Plan{}, fmt.Errorf("original broker %d no longer exists", b)
			}
		}
		changes = append(changes, model.Change{Topic: c.Topic, Partition: c.Partition, Before: append([]int32{}, x.Replicas...), After: append([]int32{}, c.Before...)})
	}
	if len(changes) == 0 {
		return model.Plan{}, ErrNothingToRollBack
	}
	if e := Validate(snap, changes); e != nil {
		return model.Plan{}, e
	}
	order := make([]string, 0, len(p.Steps))
	for i := len(p.Steps) - 1; i >= 0; i-- {
		order = append(order, p.Steps[i].Topic)
	}
	if len(order) == 0 {
		for i := len(p.Topics) - 1; i >= 0; i-- {
			order = append(order, p.Topics[i])
		}
	}
	r := model.Plan{
		ID:                  auth.Token(),
		ClusterID:           p.ClusterID,
		State:               "planned",
		Topics:              append([]string{}, p.Topics...),
		Changes:             changes,
		Before:              p.After,
		After:               p.Before,
		CreatedAt:           time.Now().UTC(),
		Actor:               actor,
		ThrottleBytesPerSec: p.ThrottleBytesPerSec,
		RackAware:           p.RackAware,
		RollbackOf:          p.ID,
		Steps:               balance.Steps(order, changes, snap),
		PartitionsTotal:     len(changes),
		Fingerprint:         balance.Fingerprint(snap, p.Topics),
		PlanHash:            balance.Hash(changes),
	}
	var estimated int64
	known := true
	for _, s := range r.Steps {
		if s.Bytes == nil {
			known = false
			break
		}
		estimated += *s.Bytes
	}
	if known {
		r.EstimatedBytes = &estimated
	}
	return r, nil
}
