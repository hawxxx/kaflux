package jobs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/balance"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/msk"
)

// Variables so tests can shorten the waits.
var (
	// healthWaitLimit is how long a topic waits for a healthy cluster before the job pauses.
	healthWaitLimit = 10 * time.Minute
	// stallLimit is how long a moving topic may have nothing pending in Kafka
	// without reaching its targets before the topic fails.
	stallLimit = 30 * time.Second
	// progressLogEvery spaces out progress lines in the activity log.
	progressLogEvery = 30 * time.Second
)

// electionAttempts bounds preferred leader election retries per topic.
const electionAttempts = 3

// run advances a running job: one topic at a time, the whole topic per
// reassignment, a preferred leader election after each topic. It returns after
// any step that has to wait for Kafka.
func (w *Worker) run(ctx context.Context, p model.Plan, provider kafka.Provider, snap model.Snapshot, pending map[string][]int32) {
	if len(p.Steps) == 0 {
		// Jobs submitted before topic steps existed sent every topic at once.
		p.Steps = balance.Steps(p.Topics, p.Changes, snap)
		for i := range p.Steps {
			p.Steps[i].State = model.StepMoving
			p.Steps[i].StartedAt = timePtr(p.StartedAt)
		}
	}
	partitions := partitionIndex(snap)
	var sizes map[string]map[int32]map[int32]int64
	if sizer, ok := provider.(kafka.ReplicaSizer); ok {
		sizes = sizer.ReplicaSizes(ctx)
	}
	now := time.Now().UTC()
	updateProgress(&p, partitions, sizes, now)

	if p.CancellationRequested {
		w.cancel(ctx, p, provider, snap, pending)
		return
	}
	// A topic finishing and the next one starting can happen in one pass;
	// waiting on Kafka ends the pass.
	for advanced := true; advanced; {
		if p.CurrentStep >= len(p.Steps) {
			w.finish(ctx, p, provider, "completed", "")
			return
		}
		var stop bool
		advanced, stop = w.advance(ctx, &p, provider, snap, partitions, pending, now)
		if stop {
			return
		}
	}
	updateProgress(&p, partitions, sizes, now)
	w.logProgress(ctx, &p, now)
	if p.ThrottleRequest != nil {
		if e := w.applyRequestedThrottle(ctx, p, provider); e != nil {
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

// advance works on the current step. advanced means the step moved on and the
// next one can be tried in this pass; stop means the job was saved or ended.
func (w *Worker) advance(ctx context.Context, p *model.Plan, provider kafka.Provider, snap model.Snapshot, partitions map[string]model.Partition, pending map[string][]int32, now time.Time) (advanced, stop bool) {
	step := &p.Steps[p.CurrentStep]
	changes := topicChanges(p.Changes, step.Topic)
	switch step.State {
	case model.StepDone, model.StepSkipped:
		p.CurrentStep++
		return true, false
	case model.StepFailed:
		w.pause(ctx, p, fmt.Sprintf("Topic %s failed: %s", step.Topic, step.Error))
		return false, true
	case model.StepPending:
		return w.begin(ctx, p, step, changes, provider, snap, partitions, pending, now)
	case model.StepMoving:
		return w.watch(ctx, p, step, changes, provider, partitions, pending, now)
	case model.StepElecting:
		return w.elect(ctx, p, step, changes, provider, now), false
	}
	return false, false
}

// begin starts a topic at a boundary: pause requests, cluster health and
// unchanged assignments come first, then the throttle and one reassignment.
func (w *Worker) begin(ctx context.Context, p *model.Plan, step *model.TopicStep, changes []model.Change, provider kafka.Provider, snap model.Snapshot, partitions map[string]model.Partition, pending map[string][]int32, now time.Time) (bool, bool) {
	inFlight := false
	for _, c := range changes {
		if _, ok := pending[changeKey(c.Topic, c.Partition)]; ok {
			inFlight = true
		}
	}
	if inFlight {
		// A previous pass submitted the topic but could not record it.
		step.State = model.StepMoving
		step.StartedAt = timePtr(now)
		step.Error = ""
		w.log(ctx, p.ID, levelInfo, "▶ %s already in flight in Kafka · resuming", step.Topic)
		return false, false
	}
	if p.PauseRequested {
		by := p.PauseRequestedBy
		if by == "" {
			by = "an operator"
		}
		w.pause(ctx, p, "Paused by "+by)
		return false, true
	}
	if reason := waitReason(snap, p, pending); reason != "" {
		if p.HealthWaitSince == nil {
			p.HealthWaitSince = timePtr(now)
			w.log(ctx, p.ID, levelWarn, "⚠ Waiting before %s · %s", step.Topic, reason)
		} else if now.Sub(*p.HealthWaitSince) >= healthWaitLimit {
			w.pause(ctx, p, fmt.Sprintf("Cluster not ready for %s after %s: %s", step.Topic, humanDuration(healthWaitLimit), reason))
			return false, true
		}
		return false, false
	}
	if p.HealthWaitSince != nil {
		w.log(ctx, p.ID, levelInfo, "✔ Cluster ready again after %s", humanDuration(now.Sub(*p.HealthWaitSince)))
		p.HealthWaitSince = nil
	}
	for _, c := range changes {
		x := partitions[changeKey(c.Topic, c.Partition)]
		if !reflect.DeepEqual(x.Replicas, c.Before) && !reflect.DeepEqual(x.Replicas, c.After) {
			w.failStep(ctx, p, step, fmt.Sprintf("partition %d assignment changed since approval", c.Partition))
			return false, true
		}
	}
	if cap, e := Capabilities(ctx, provider, true); e != nil || !cap.ManualReassignmentAllowed {
		w.blocked(ctx, *p, provider, cap.Reason)
		return false, true
	}
	if e := w.throttleTopic(ctx, p, changes, provider); e != nil {
		w.failStep(ctx, p, step, "throttle application failed: "+e.Error())
		return false, true
	}
	step.StartedAt = timePtr(now)
	w.log(ctx, p.ID, levelInfo, "▶ %s · %d partitions · %s · brokers %s → %s", step.Topic, step.Partitions, stepSize(step.Bytes), brokerSet(changes, true), brokerSet(changes, false))
	if e := provider.Reassign(ctx, changes); e != nil {
		var blocked *msk.BlockedError
		if errors.As(e, &blocked) {
			w.blocked(ctx, *p, provider, blocked.Error())
			return false, true
		}
		message := "submission outcome uncertain; reconciling broker assignments"
		if step.Error != message {
			w.log(ctx, p.ID, levelWarn, "⚠ %s · %s: %v", step.Topic, message, e)
		}
		step.Error = message
		p.Error = message
		return false, false
	}
	step.State = model.StepMoving
	step.Error = ""
	p.Error = ""
	return false, false
}

// watch follows a moving topic until every partition reached its target.
func (w *Worker) watch(ctx context.Context, p *model.Plan, step *model.TopicStep, changes []model.Change, provider kafka.Provider, partitions map[string]model.Partition, pending map[string][]int32, now time.Time) (bool, bool) {
	ownPending, done := 0, 0
	for _, c := range changes {
		key := changeKey(c.Topic, c.Partition)
		if _, ok := pending[key]; ok {
			ownPending++
		}
		if partitionDone(partitions[key], c) {
			done++
		}
	}
	if done < len(changes) || ownPending > 0 {
		started := p.StartedAt
		if step.StartedAt != nil {
			started = *step.StartedAt
		}
		if ownPending == 0 && now.Sub(started) > stallLimit {
			w.failStep(ctx, p, step, "no broker reassignment remains and target assignments were not reached")
			return false, true
		}
		return false, false
	}
	if e := w.releaseThrottle(ctx, p, provider); e != nil {
		message := "throttle cleanup requires attention: " + e.Error()
		if step.Error != message {
			w.log(ctx, p.ID, levelError, "✖ %s · %s", step.Topic, message)
		}
		step.Error = message
		return false, false
	}
	step.Error = ""
	step.State = model.StepElecting
	return true, false
}

// elect runs the preferred leader election for a finished topic. Repeated
// failure becomes a warning: the data already moved, so the job carries on.
func (w *Worker) elect(ctx context.Context, p *model.Plan, step *model.TopicStep, changes []model.Change, provider kafka.Provider, now time.Time) bool {
	if elector, ok := provider.(kafka.LeaderElector); ok {
		w.log(ctx, p.ID, levelInfo, "⇄ Preferred leader election · %s · %d partitions · PREFERRED", step.Topic, len(changes))
		if e := elector.ElectPreferredLeaders(ctx, changes); e != nil {
			step.ElectionAttempts++
			if step.ElectionAttempts < electionAttempts {
				w.log(ctx, p.ID, levelWarn, "⚠ Leader election for %s failed · retry %d/%d · %v", step.Topic, step.ElectionAttempts, electionAttempts, e)
				return false
			}
			warning := fmt.Sprintf("Leader election for %s failed after %d attempts: %v", step.Topic, electionAttempts, e)
			p.Warnings = append(p.Warnings, warning)
			w.log(ctx, p.ID, levelWarn, "⚠ %s", warning)
		}
	}
	step.State = model.StepDone
	step.FinishedAt = timePtr(now)
	took := ""
	if step.StartedAt != nil {
		took = " in " + humanDuration(now.Sub(*step.StartedAt))
	}
	w.log(ctx, p.ID, levelInfo, "✔ %s done%s", step.Topic, took)
	p.CurrentStep++
	return true
}

// cancel asks Kafka to cancel this job's in-flight moves and finishes once
// nothing is pending, queueing the rollback when it was requested.
func (w *Worker) cancel(ctx context.Context, p model.Plan, provider kafka.Provider, snap model.Snapshot, pending map[string][]int32) {
	ownPending := []model.Change{}
	for _, c := range p.Changes {
		if _, ok := pending[changeKey(c.Topic, c.Partition)]; ok {
			ownPending = append(ownPending, c)
		}
	}
	if len(ownPending) == 0 {
		if !p.RollbackRequested {
			w.finish(ctx, p, provider, "canceled", "cancellation reconciled; inspect current assignments before rollback")
			return
		}
		rollback, e := RollbackPlan(snap, p, p.RollbackRequestedBy)
		if errors.Is(e, ErrNothingToRollBack) {
			w.finish(ctx, p, provider, "canceled", "cancellation reconciled; no partition had moved, nothing to roll back")
			return
		}
		if e != nil {
			p.Warnings = append(p.Warnings, "Rollback could not be queued: "+e.Error())
			w.finish(ctx, p, provider, "canceled", "cancellation reconciled; rollback could not be queued: "+e.Error())
			return
		}
		rollback.State = "queued"
		rollback.ApprovedBy = p.RollbackRequestedBy
		w.finishWith(ctx, p, provider, "canceled", "cancellation reconciled; rollback queued", &rollback)
		return
	}
	message := ""
	if canceler, ok := provider.(kafka.CancellationProvider); ok {
		if e := canceler.CancelReassignment(ctx, ownPending); e != nil {
			message = "Cancellation requires attention: " + e.Error()
		} else {
			message = "Cancellation submitted; reconciling broker assignments"
		}
	} else {
		message = "Kafka provider does not support reassignment cancellation"
	}
	if p.Error != message {
		level := levelWarn
		if !strings.HasPrefix(message, "Cancellation submitted") {
			level = levelError
		}
		w.log(ctx, p.ID, level, "%s · %d partitions", message, len(ownPending))
	}
	p.Error = message
	_ = w.save(ctx, p)
}

func (w *Worker) pause(ctx context.Context, p *model.Plan, reason string) {
	p.State = "paused"
	p.PauseReason = reason
	p.ETASeconds = nil
	if e := w.save(ctx, *p); e == nil {
		w.log(ctx, p.ID, levelWarn, "⏸ %s", reason)
	}
}

// blocked handles Kafka refusing manual reassignment (for example MSK
// intelligent rebalancing turned on). Before anything moved the job fails;
// afterwards it pauses so the operator can resume, skip or roll back.
func (w *Worker) blocked(ctx context.Context, p model.Plan, provider kafka.Provider, reason string) {
	for _, s := range p.Steps {
		if s.State != model.StepPending {
			w.pause(ctx, &p, "Manual reassignment blocked: "+reason)
			return
		}
	}
	w.finish(ctx, p, provider, "failed", reason)
}

func (w *Worker) failStep(ctx context.Context, p *model.Plan, step *model.TopicStep, reason string) {
	step.State = model.StepFailed
	step.Error = reason
	step.FinishedAt = timePtr(time.Now().UTC())
	w.log(ctx, p.ID, levelError, "✖ %s failed · %s", step.Topic, reason)
	w.pause(ctx, p, fmt.Sprintf("Topic %s failed: %s", step.Topic, reason))
}

// throttleTopic captures and applies the replication throttle for one topic's
// replicas. Unthrottled jobs never touch broker configuration.
func (w *Worker) throttleTopic(ctx context.Context, p *model.Plan, changes []model.Change, provider kafka.Provider) error {
	throttle, ok := provider.(kafka.ThrottleProvider)
	if !ok || p.ThrottleBytesPerSec <= 0 {
		return nil
	}
	records, e := w.Store.LoadThrottle(ctx, p.ID)
	if e != nil {
		return e
	}
	if len(records) == 0 {
		if records, e = throttle.PrepareThrottle(ctx, changes, p.ThrottleBytesPerSec); e != nil {
			return e
		}
		if e = w.Store.SaveThrottle(ctx, p.ID, records); e != nil {
			return e
		}
		if records, e = w.Store.LoadThrottle(ctx, p.ID); e != nil {
			return e
		}
	}
	if e = throttle.ApplyThrottle(ctx, records); e != nil {
		return e
	}
	w.log(ctx, p.ID, levelInfo, "Throttle %s applied to %s (leader and follower replicas)", humanRate(p.ThrottleBytesPerSec), changes[0].Topic)
	return nil
}

// releaseThrottle restores the configuration captured for the finished topic.
func (w *Worker) releaseThrottle(ctx context.Context, p *model.Plan, provider kafka.Provider) error {
	throttle, ok := provider.(kafka.ThrottleProvider)
	if !ok {
		return nil
	}
	records, e := w.Store.LoadThrottle(ctx, p.ID)
	if e != nil || len(records) == 0 {
		return e
	}
	if e = throttle.RestoreThrottle(ctx, records); e != nil {
		return e
	}
	if e = w.Store.DeleteThrottle(ctx, p.ID); e != nil {
		return e
	}
	w.log(ctx, p.ID, levelInfo, "Throttle removed from %s", p.Steps[p.CurrentStep].Topic)
	return nil
}

// waitReason explains why a topic must not start yet: an unhealthy cluster or
// a reassignment this job does not own.
func waitReason(snap model.Snapshot, p *model.Plan, pending map[string][]int32) string {
	if e := ValidateCluster(snap); e != nil {
		return e.Error()
	}
	own := map[string]bool{}
	for _, c := range p.Changes {
		own[changeKey(c.Topic, c.Partition)] = true
	}
	foreign := 0
	for key := range pending {
		if !own[key] {
			foreign++
		}
	}
	if foreign > 0 {
		return fmt.Sprintf("%d reassignments by others in progress", foreign)
	}
	return ""
}

func (w *Worker) logProgress(ctx context.Context, p *model.Plan, now time.Time) {
	if p.CurrentStep >= len(p.Steps) || p.Steps[p.CurrentStep].State != model.StepMoving {
		return
	}
	if p.ProgressLoggedAt != nil && now.Sub(*p.ProgressLoggedAt) < progressLogEvery {
		return
	}
	step := p.Steps[p.CurrentStep]
	parts := []string{fmt.Sprintf("%d/%d partitions", step.PartitionsDone, step.Partitions)}
	percent := 0
	if step.Partitions > 0 {
		percent = step.PartitionsDone * 100 / step.Partitions
	}
	if step.Bytes != nil && step.BytesDone != nil {
		if *step.Bytes > 0 {
			percent = int(*step.BytesDone * 100 / *step.Bytes)
		}
		parts = append(parts, humanBytes(*step.BytesDone)+"/"+humanBytes(*step.Bytes))
	}
	if p.RateBytesPerSec != nil {
		parts = append(parts, humanRate(*p.RateBytesPerSec))
	}
	if p.ETASeconds != nil {
		parts = append(parts, "ETA "+humanDuration(time.Duration(*p.ETASeconds)*time.Second))
	}
	w.log(ctx, p.ID, levelInfo, "%s %d%% · %s", step.Topic, percent, strings.Join(parts, " · "))
	p.ProgressLoggedAt = timePtr(now)
}

func topicChanges(changes []model.Change, topic string) []model.Change {
	out := []model.Change{}
	for _, c := range changes {
		if c.Topic == topic {
			out = append(out, c)
		}
	}
	return out
}

// brokerSet lists the distinct brokers before (or after) a topic's moves.
func brokerSet(changes []model.Change, before bool) string {
	seen := map[int32]bool{}
	ids := []int32{}
	for _, c := range changes {
		list := c.After
		if before {
			list = c.Before
		}
		for _, id := range list {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return strings.ReplaceAll(fmt.Sprint(ids), " ", ",")
}

func stepSize(b *int64) string {
	if b == nil {
		return "size unknown"
	}
	return humanBytes(*b)
}

func timePtr(t time.Time) *time.Time { return &t }
