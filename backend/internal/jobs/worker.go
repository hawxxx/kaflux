package jobs

import (
	"context"
	"fmt"
	"github.com/hawxxx/kaflux/backend/internal/auth"
	"github.com/hawxxx/kaflux/backend/internal/balance"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/msk"
	"github.com/hawxxx/kaflux/backend/internal/store"
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
	if p.State == "queued" {
		if !w.start(ctx, &p, provider, snap, pending) {
			return
		}
	}
	w.run(ctx, p, provider, snap, pending)
}

// start runs the approval-time safety checks once and moves the job to running.
// It returns false when the job ended or must be retried on a later pass.
func (w *Worker) start(ctx context.Context, p *model.Plan, provider kafka.Provider, snap model.Snapshot, pending map[string][]int32) bool {
	if e := ValidateCluster(snap); e != nil {
		w.finish(ctx, *p, provider, "failed", e.Error())
		return false
	}
	if cap, e := Capabilities(ctx, provider, true); e != nil || !cap.ManualReassignmentAllowed {
		w.finish(ctx, *p, provider, "failed", cap.Reason)
		return false
	}
	if e := Validate(snap, p.Changes); e != nil {
		w.finish(ctx, *p, provider, "failed", e.Error())
		return false
	}
	if balance.Fingerprint(snap, p.Topics) != p.Fingerprint {
		w.finish(ctx, *p, provider, "failed", "assignments changed since approval")
		return false
	}
	if len(pending) > 0 {
		w.finish(ctx, *p, provider, "failed", "conflicting broker reassignment")
		return false
	}
	if len(p.Steps) == 0 {
		p.Steps = balance.Steps(p.Topics, p.Changes, snap)
	}
	p.State = "running"
	p.StartedAt = time.Now().UTC()
	p.PartitionsTotal = len(p.Changes)
	if e := w.save(ctx, *p); e != nil {
		return false
	}
	throttle := "unthrottled"
	if p.ThrottleBytesPerSec > 0 {
		throttle = "throttle " + humanRate(p.ThrottleBytesPerSec)
	}
	size := "size unknown"
	if p.EstimatedBytes != nil {
		size = humanBytes(*p.EstimatedBytes)
	}
	kind := "Job"
	if p.RollbackOf != "" {
		kind = "Rollback of " + p.RollbackOf
	}
	w.log(ctx, p.ID, levelInfo, "%s started · %d topics · %d partitions · %s · %s", kind, len(p.Steps), len(p.Changes), size, throttle)
	controller := "unknown"
	if snap.Controller != nil {
		controller = fmt.Sprint(*snap.Controller)
	}
	w.log(ctx, p.ID, levelInfo, "Preflight ok · controller %s · 0 under-replicated · 0 offline", controller)
	latest, e := w.Store.Job(ctx, p.ID)
	if e != nil {
		return false
	}
	if latest.CancellationRequested {
		w.finish(ctx, latest, provider, "canceled", "canceled before submission")
		return false
	}
	*p = latest
	return w.Store.Claim(ctx, p.ID, w.Owner)
}

func (w *Worker) finish(ctx context.Context, p model.Plan, provider kafka.Provider, state, message string) {
	w.finishWith(ctx, p, provider, state, message, nil)
}

// finishWith ends the job after restoring any throttle it still owns. A
// rollback job, when given, is stored in the same transaction.
func (w *Worker) finishWith(ctx context.Context, p model.Plan, provider kafka.Provider, state, message string, rollback *model.Plan) {
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
	p.ETASeconds = nil
	now := time.Now().UTC()
	p.FinishedAt = &now
	if rollback != nil {
		p.RollbackJob = rollback.ID
		if e := w.Store.FinishWithRollback(ctx, p, *rollback, w.Owner); e != nil {
			return
		}
	} else if e := w.save(ctx, p); e != nil {
		return
	}
	w.logFinish(ctx, p, state, message, rollback)
}

func (w *Worker) logFinish(ctx context.Context, p model.Plan, state, message string, rollback *model.Plan) {
	elapsed := ""
	if !p.StartedAt.IsZero() {
		elapsed = " in " + humanDuration(time.Since(p.StartedAt))
	}
	switch state {
	case "completed":
		done := 0
		for _, s := range p.Steps {
			if s.State == model.StepDone {
				done++
			}
		}
		suffix := ""
		if len(p.Warnings) > 0 {
			suffix = fmt.Sprintf(" · %d warnings", len(p.Warnings))
		}
		w.log(ctx, p.ID, levelInfo, "✔ Job completed · %d/%d topics · %d partitions%s%s", done, len(p.Steps), len(p.Changes), elapsed, suffix)
	case "canceled":
		w.log(ctx, p.ID, levelWarn, "Job canceled%s · %s", elapsed, message)
		if rollback != nil {
			w.log(ctx, p.ID, levelInfo, "↩ Rollback job %s queued · %d partitions", rollback.ID, len(rollback.Changes))
			w.log(ctx, rollback.ID, levelInfo, "Rollback of %s queued by %s", p.ID, rollback.ApprovedBy)
		}
	default:
		w.log(ctx, p.ID, levelError, "✖ Job %s · %s", state, message)
	}
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
		caps, err := c.Capabilities(ctx, force)
		// Whoever may run a plan may also plan it, whatever the provider reported.
		caps.PlanningAllowed = caps.PlanningAllowed || caps.ManualReassignmentAllowed
		return caps, err
	}
	return msk.Capabilities{Kind: "Kafka", ManualReassignmentAllowed: true, PlanningAllowed: true, RebalancingStatus: "NOT_APPLICABLE", ObservedAt: time.Now().UTC()}, nil
}
