package jobs

import (
	"context"
	"errors"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/balance"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/msk"
	"github.com/hawxxx/kaflux/backend/internal/store"
	"reflect"
	"sync"
	"time"
)

// Worker serializes cluster operations using durable leases. Broker assignments
// remain authoritative after submission timeouts and process restarts.
type Worker struct {
	Store     *store.Store
	Providers map[string]kafka.Provider
	Owner     string
	mu        sync.Mutex
}

func (w *Worker) Run(ctx context.Context) {
	w.Owner = auth.Token()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			w.Step(ctx)
		}
	}
}
func (w *Worker) Step(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	all, e := w.Store.ActiveJobs(ctx)
	if e != nil {
		return
	}
	var group sync.WaitGroup
	slots := make(chan struct{}, 8)
	defer group.Wait()
	for _, candidate := range all {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		group.Add(1)
		go func(candidate model.Plan) { defer group.Done(); defer func() { <-slots }(); w.stepJob(ctx, candidate) }(candidate)
	}
}
func (w *Worker) stepJob(ctx context.Context, candidate model.Plan) {
	if !w.Store.Claim(ctx, candidate.ID, w.Owner) {
		return
	}
	p, e := w.Store.Job(ctx, candidate.ID)
	if e != nil {
		return
	}
	provider := w.Providers[p.ClusterID]
	if provider == nil {
		return
	}
	jobCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-ticker.C:
				if !w.Store.Claim(jobCtx, p.ID, w.Owner) {
					cancel()
					return
				}
			}
		}
	}()
	w.process(jobCtx, p, provider)
	cancel()
	<-done
}
func (w *Worker) save(ctx context.Context, p model.Plan) error {
	return w.Store.SaveClaimedJob(ctx, p, w.Owner)
}
func FreshSnapshot(ctx context.Context, p kafka.Provider) (model.Snapshot, error) {
	if f, ok := p.(interface {
		FreshSnapshot(context.Context) (model.Snapshot, error)
	}); ok {
		return f.FreshSnapshot(ctx)
	}
	return p.Snapshot(ctx)
}
func (w *Worker) process(ctx context.Context, p model.Plan, provider kafka.Provider) {
	if p.CleanupPending {
		state := p.TerminalState
		if state == "" {
			state = "failed"
		}
		w.finish(ctx, p, provider, state, p.TerminalError)
		return
	}
	if p.State == "queued" && p.CancellationRequested {
		w.finish(ctx, p, provider, "canceled", "canceled before submission")
		return
	}
	snap, e := FreshSnapshot(ctx, provider)
	if e != nil {
		return
	}
	pending, e := provider.Pending(ctx)
	if e != nil {
		return
	}
	throttle, throttled := provider.(kafka.ThrottleProvider)
	if p.State == "queued" {
		if e = ValidateCluster(snap); e != nil {
			w.finish(ctx, p, provider, "failed", e.Error())
			return
		}
		if cap, e := Capabilities(ctx, provider, true); e != nil || !cap.ManualReassignmentAllowed {
			w.finish(ctx, p, provider, "failed", cap.Reason)
			return
		}
		if e = Validate(snap, p.Changes); e != nil {
			w.finish(ctx, p, provider, "failed", e.Error())
			return
		}
		if balance.Fingerprint(snap, p.Topics) != p.Fingerprint {
			w.finish(ctx, p, provider, "failed", "assignments changed since approval")
			return
		}
		if len(pending) > 0 {
			w.finish(ctx, p, provider, "failed", "conflicting broker reassignment")
			return
		}
		if throttled {
			records, e := w.Store.LoadThrottle(ctx, p.ID)
			if e != nil {
				return
			}
			if len(records) == 0 {
				records, e = throttle.PrepareThrottle(ctx, p.Changes, p.ThrottleBytesPerSec)
				if e != nil {
					w.finish(ctx, p, provider, "failed", e.Error())
					return
				}
				if e = w.Store.SaveThrottle(ctx, p.ID, records); e != nil {
					return
				}
				records, e = w.Store.LoadThrottle(ctx, p.ID)
				if e != nil {
					return
				}
			}
			if e = throttle.ApplyThrottle(ctx, records); e != nil {
				w.finish(ctx, p, provider, "failed", "Throttle application failed: "+e.Error())
				return
			}
		}
		p.State = "running"
		p.StartedAt = time.Now().UTC()
		if e = w.save(ctx, p); e != nil {
			return
		}
		latest, e := w.Store.Job(ctx, p.ID)
		if e != nil {
			return
		}
		if latest.CancellationRequested {
			w.finish(ctx, latest, provider, "canceled", "canceled before submission")
			return
		}
		if !w.Store.Claim(ctx, p.ID, w.Owner) {
			return
		}
		if e = provider.Reassign(ctx, p.Changes); e != nil {
			var blocked *msk.BlockedError
			if errors.As(e, &blocked) {
				w.finish(ctx, p, provider, "failed", blocked.Error())
				return
			}
			p.Error = "submission outcome uncertain; reconciling broker assignments"
			_ = w.save(ctx, p)
		}
		return
	}
	completed := 0
	partitions := map[string]model.Partition{}
	for _, t := range snap.Topics {
		for _, x := range t.Partitions {
			partitions[fmt.Sprintf("%s/%d", t.Name, x.ID)] = x
		}
	}
	ownPending := []model.Change{}
	for _, c := range p.Changes {
		key := fmt.Sprintf("%s/%d", c.Topic, c.Partition)
		x, ok := partitions[key]
		if ok && reflect.DeepEqual(x.Replicas, c.After) && len(x.ISR) == len(x.Replicas) {
			completed++
		}
		if _, ok := pending[key]; ok {
			ownPending = append(ownPending, c)
		}
	}
	if len(p.Changes) > 0 {
		p.Progress = completed * 100 / len(p.Changes)
	}
	if completed == len(p.Changes) && len(ownPending) == 0 {
		w.finish(ctx, p, provider, "completed", "")
		return
	}
	if p.CancellationRequested {
		if len(ownPending) == 0 {
			w.finish(ctx, p, provider, "canceled", "cancellation reconciled; inspect current assignments before rollback")
			return
		}
		if canceler, ok := provider.(kafka.CancellationProvider); ok {
			if e = canceler.CancelReassignment(ctx, ownPending); e != nil {
				p.Error = "Cancellation requires attention: " + e.Error()
			} else {
				p.Error = "Cancellation submitted; reconciling broker assignments"
			}
		} else {
			p.Error = "Kafka provider does not support reassignment cancellation"
		}
		_ = w.save(ctx, p)
		return
	}
	if len(pending) == 0 && time.Since(p.StartedAt) > 30*time.Second {
		w.finish(ctx, p, provider, "failed", "no broker reassignment remains and target assignments were not reached")
		return
	}
	if p.ThrottleRequest != nil {
		if e = w.applyRequestedThrottle(ctx, p, provider); e != nil {
			if p.ThrottleError != e.Error() {
				_ = w.throttleAudit(ctx, p, "failed-or-uncertain")
			}
			p.ThrottleError = e.Error()
			_ = w.save(ctx, p)
			return
		}
		fresh, e := w.Store.Job(ctx, p.ID)
		if e != nil {
			return
		}
		p.ThrottleRequest = fresh.ThrottleRequest
		p.ThrottleBytesPerSec = fresh.ThrottleBytesPerSec
		p.ThrottleError = fresh.ThrottleError
	}
	_ = w.save(ctx, p)
}
func (w *Worker) finish(ctx context.Context, p model.Plan, provider kafka.Provider, state, message string) {
	p.Error = message
	p.TerminalState = state
	p.TerminalError = message
	if throttle, ok := provider.(kafka.ThrottleProvider); ok {
		records, e := w.Store.LoadThrottle(ctx, p.ID)
		if e != nil {
			return
		}
		if len(records) > 0 {
			if e = throttle.RestoreThrottle(ctx, records); e != nil {
				p.State = "running"
				p.CleanupPending = true
				p.Error = "Throttle cleanup requires attention: " + e.Error()
				_ = w.save(ctx, p)
				return
			}
			if e = w.Store.DeleteThrottle(ctx, p.ID); e != nil {
				return
			}
		}
	}
	if e := w.Store.Audit(ctx, store.Audit{Actor: p.ApprovedBy, ClusterID: p.ClusterID, Action: "reassignment", Resource: p.ID, Result: state}); e != nil {
		return
	}
	p.State = state
	p.CleanupPending = false
	p.TerminalState = ""
	p.TerminalError = ""
	_ = w.save(ctx, p)
}
func Validate(s model.Snapshot, changes []model.Change) error {
	if len(changes) == 0 || len(changes) > 5000 {
		return fmt.Errorf("operation requires between 1 and 5000 partitions")
	}
	brokers := map[int32]bool{}
	for _, b := range s.Brokers {
		brokers[b.ID] = true
	}
	partitions := map[string]model.Partition{}
	for _, t := range s.Topics {
		for _, p := range t.Partitions {
			partitions[fmt.Sprintf("%s/%d", t.Name, p.ID)] = p
		}
	}
	for _, c := range changes {
		x, ok := partitions[fmt.Sprintf("%s/%d", c.Topic, c.Partition)]
		if !ok || x.Leader < 0 || len(x.ISR) != len(x.Replicas) {
			return fmt.Errorf("partition unavailable or insufficient ISR")
		}
		if len(c.After) != len(x.Replicas) {
			return fmt.Errorf("replication factor changed")
		}
		seen := map[int32]bool{}
		for _, id := range c.After {
			if !brokers[id] || seen[id] {
				return fmt.Errorf("invalid target broker")
			}
			seen[id] = true
		}
	}
	return nil
}

// Production safety checks consider the estate, including unrelated topics.
func ValidateCluster(s model.Snapshot) error {
	if s.Controller == nil {
		return fmt.Errorf("controller health is unknown")
	}
	// Dedicated KRaft controllers are not always listed as data brokers; a valid
	// nonnegative controller ID from metadata is still an authoritative controller.
	if *s.Controller < 0 {
		return fmt.Errorf("no active controller")
	}
	for _, topic := range s.Topics {
		for _, p := range topic.Partitions {
			if p.Leader < 0 {
				return fmt.Errorf("cluster has an offline partition")
			}
			if len(p.ISR) < len(p.Replicas) {
				return fmt.Errorf("cluster has under-replicated partitions")
			}
		}
	}
	return nil
}
func Approve(ctx context.Context, s *store.Store, p model.Plan, actor, hash string) error {
	if p.State != "planned" {
		return fmt.Errorf("plan cannot be executed in state %s", p.State)
	}
	if p.PlanHash != hash {
		return fmt.Errorf("plan hash mismatch")
	}
	p.State = "queued"
	p.ApprovedBy = actor
	return s.QueueJob(ctx, p)
}
func Capabilities(ctx context.Context, p kafka.Provider, force bool) (msk.Capabilities, error) {
	if c, ok := p.(interface {
		Capabilities(context.Context, bool) (msk.Capabilities, error)
	}); ok {
		return c.Capabilities(ctx, force)
	}
	return msk.Capabilities{Kind: "Kafka", ManualReassignmentAllowed: true, RebalancingStatus: "NOT_APPLICABLE", ObservedAt: time.Now().UTC()}, nil
}
