package jobs

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hawxxx/kaflux/backend/internal/balance"
	"github.com/hawxxx/kaflux/backend/internal/kafka"
	"github.com/hawxxx/kaflux/backend/internal/model"
	"github.com/hawxxx/kaflux/backend/internal/store"
)

// scripted is the demo cluster with recorded calls. Topics in hold stay
// pending in Kafka instead of moving; stall topics neither move nor pend.
type scripted struct {
	*kafka.Demo
	calls     [][]model.Change
	elections []string
	electErr  error
	hold      map[string]bool
	stall     map[string]bool
	pending   map[string][]int32
	prepared  [][]model.Change
	restored  int
	unhealthy bool
}

func newScripted() *scripted {
	return &scripted{Demo: kafka.NewDemo(), hold: map[string]bool{}, stall: map[string]bool{}, pending: map[string][]int32{}}
}

func (p *scripted) Snapshot(ctx context.Context) (model.Snapshot, error) {
	s, e := p.Demo.Snapshot(ctx)
	if p.unhealthy {
		s.Topics[len(s.Topics)-1].Partitions[0].ISR = s.Topics[len(s.Topics)-1].Partitions[0].Replicas[:1]
	}
	return s, e
}
func (p *scripted) Reassign(ctx context.Context, changes []model.Change) error {
	p.calls = append(p.calls, changes)
	apply := []model.Change{}
	for _, c := range changes {
		switch {
		case p.hold[c.Topic]:
			p.pending[fmt.Sprintf("%s/%d", c.Topic, c.Partition)] = c.After
		case p.stall[c.Topic]:
		default:
			apply = append(apply, c)
		}
	}
	return p.Demo.Reassign(ctx, apply)
}
func (p *scripted) Pending(context.Context) (map[string][]int32, error) {
	out := map[string][]int32{}
	for k, v := range p.pending {
		out[k] = v
	}
	return out, nil
}
func (p *scripted) CancelReassignment(_ context.Context, changes []model.Change) error {
	for _, c := range changes {
		delete(p.pending, fmt.Sprintf("%s/%d", c.Topic, c.Partition))
	}
	return nil
}
func (p *scripted) ElectPreferredLeaders(ctx context.Context, changes []model.Change) error {
	p.elections = append(p.elections, changes[0].Topic)
	if p.electErr != nil {
		return p.electErr
	}
	return p.Demo.ElectPreferredLeaders(ctx, changes)
}

// throttled adds broker throttle support to the scripted cluster.
type throttled struct{ *scripted }

func (p throttled) PrepareThrottle(_ context.Context, changes []model.Change, rate int64) ([]kafka.ThrottleRecord, error) {
	p.prepared = append(p.prepared, changes)
	return []kafka.ThrottleRecord{{Resource: "broker", Name: "1", Key: "leader.replication.throttled.rate", EffectiveBefore: "", Applied: fmt.Sprint(rate)}}, nil
}
func (p throttled) ApplyThrottle(context.Context, []kafka.ThrottleRecord) error { return nil }
func (p throttled) RestoreThrottle(context.Context, []kafka.ThrottleRecord) error {
	p.restored++
	return nil
}

func approvedJob(t *testing.T, s *store.Store, provider kafka.Provider, topics []string, throttle int64) model.Plan {
	t.Helper()
	ctx := context.Background()
	snap, _ := provider.Snapshot(ctx)
	p, e := balance.Generate(snap, balance.Request{Topics: topics, Brokers: []int32{2, 3, 4}, RackAware: true, ThrottleBytesPerSec: throttle})
	if e != nil {
		t.Fatal(e)
	}
	p.ID = "job"
	p.ClusterID = "demo"
	p.CreatedAt = time.Now()
	if e = s.SaveJob(ctx, p); e != nil {
		t.Fatal(e)
	}
	if e = Approve(ctx, s, p, "alice", p.PlanHash); e != nil {
		t.Fatal(e)
	}
	return p
}

// stepUntil runs worker passes until the job reaches one of the states.
func stepUntil(t *testing.T, w *Worker, id string, states ...string) model.Plan {
	t.Helper()
	for i := 0; i < 20; i++ {
		w.Step(context.Background())
		p, _ := w.Store.Job(context.Background(), id)
		for _, s := range states {
			if p.State == s {
				return p
			}
		}
	}
	p, _ := w.Store.Job(context.Background(), id)
	t.Fatalf("job stuck in %s (%s): %+v", p.State, p.PauseReason, p.Steps)
	return p
}

func eventText(t *testing.T, s *store.Store, id string) string {
	t.Helper()
	events, _ := s.Events(context.Background(), id, 0, 1000)
	lines := []string{}
	for _, e := range events {
		lines = append(lines, e.Message)
	}
	return strings.Join(lines, "\n")
}

func TestTopicsRunOneAtATimeInChosenOrderWithElections(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	provider := newScripted()
	approvedJob(t, s, provider, []string{"payments.authorized", "orders.created"}, 0)
	w := &Worker{Store: s, Providers: map[string]kafka.Provider{"demo": provider}, Owner: "w"}
	done := stepUntil(t, w, "job", "completed")
	if len(provider.calls) != 2 {
		t.Fatalf("reassign calls = %d, want one per topic", len(provider.calls))
	}
	for i, want := range []string{"payments.authorized", "orders.created"} {
		for _, c := range provider.calls[i] {
			if c.Topic != want {
				t.Fatalf("call %d mixes topic %s into %s", i, c.Topic, want)
			}
		}
	}
	if strings.Join(provider.elections, ",") != "payments.authorized,orders.created" {
		t.Fatalf("elections = %v", provider.elections)
	}
	if done.Progress != 100 || done.PartitionsDone != done.PartitionsTotal || done.FinishedAt == nil {
		t.Fatalf("final progress = %d %d/%d", done.Progress, done.PartitionsDone, done.PartitionsTotal)
	}
	log := eventText(t, s, "job")
	for _, want := range []string{"Job started", "▶ payments.authorized", "✔ payments.authorized done", "⇄ Preferred leader election · orders.created", "✔ Job completed · 2/2 topics"} {
		if !strings.Contains(log, want) {
			t.Fatalf("log misses %q:\n%s", want, log)
		}
	}
}

func TestPauseWaitsForTopicBoundaryAndResumeContinues(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	provider := newScripted()
	provider.hold["payments.authorized"] = true
	approvedJob(t, s, provider, []string{"payments.authorized", "orders.created"}, 0)
	w := &Worker{Store: s, Providers: map[string]kafka.Provider{"demo": provider}, Owner: "w"}
	w.Step(ctx)
	if e := s.RequestPause(ctx, "job", "alice"); e != nil {
		t.Fatal(e)
	}
	w.Step(ctx)
	if p, _ := s.Job(ctx, "job"); p.State != "running" {
		t.Fatalf("paused in the middle of a topic: %s", p.State)
	}
	// The held topic finishes: Kafka applies it and nothing is pending.
	var held []model.Change
	for _, c := range provider.calls[0] {
		held = append(held, c)
		delete(provider.pending, fmt.Sprintf("%s/%d", c.Topic, c.Partition))
	}
	_ = provider.Demo.Reassign(ctx, held)
	paused := stepUntil(t, w, "job", "paused")
	if len(provider.calls) != 1 || paused.CurrentStep != 1 || paused.PauseReason != "Paused by alice" {
		t.Fatalf("pause = calls %d step %d reason %q", len(provider.calls), paused.CurrentStep, paused.PauseReason)
	}
	w.Step(ctx)
	if len(provider.calls) != 1 {
		t.Fatal("worker touched Kafka while paused")
	}
	if _, e := s.Resume(ctx, "job", false); e != nil {
		t.Fatal(e)
	}
	stepUntil(t, w, "job", "completed")
	if len(provider.calls) != 2 {
		t.Fatalf("calls after resume = %d", len(provider.calls))
	}
}

func TestFailedTopicPausesAndCanBeSkipped(t *testing.T) {
	ctx := context.Background()
	defer func(old time.Duration) { stallLimit = old }(stallLimit)
	stallLimit = 0
	s, _ := store.New(ctx, "")
	provider := newScripted()
	provider.stall["payments.authorized"] = true
	approvedJob(t, s, provider, []string{"payments.authorized", "orders.created"}, 0)
	w := &Worker{Store: s, Providers: map[string]kafka.Provider{"demo": provider}, Owner: "w"}
	paused := stepUntil(t, w, "job", "paused")
	if paused.Steps[0].State != model.StepFailed || !strings.Contains(paused.PauseReason, "payments.authorized failed") {
		t.Fatalf("paused = %+v %q", paused.Steps[0], paused.PauseReason)
	}
	if _, e := s.Resume(ctx, "job", true); e != nil {
		t.Fatal(e)
	}
	done := stepUntil(t, w, "job", "completed")
	if done.Steps[0].State != model.StepSkipped || done.Steps[1].State != model.StepDone {
		t.Fatalf("steps = %+v", done.Steps)
	}
}

func TestUnhealthyClusterWaitsThenPauses(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	provider := newScripted()
	approvedJob(t, s, provider, []string{"payments.authorized", "orders.created"}, 0)
	w := &Worker{Store: s, Providers: map[string]kafka.Provider{"demo": provider}, Owner: "w"}
	w.Step(ctx) // first topic submitted
	provider.unhealthy = true
	w.Step(ctx)
	w.Step(ctx)
	p, _ := s.Job(ctx, "job")
	if p.State != "running" || p.HealthWaitSince == nil || len(provider.calls) != 1 {
		t.Fatalf("did not wait: state %s calls %d", p.State, len(provider.calls))
	}
	if strings.Count(eventText(t, s, "job"), "Waiting before") != 1 {
		t.Fatal("health wait must be logged once")
	}
	defer func(old time.Duration) { healthWaitLimit = old }(healthWaitLimit)
	healthWaitLimit = 0
	paused := stepUntil(t, w, "job", "paused")
	if !strings.Contains(paused.PauseReason, "under-replicated") {
		t.Fatalf("reason = %q", paused.PauseReason)
	}
}

func TestLeaderElectionFailureBecomesWarning(t *testing.T) {
	s, _ := store.New(context.Background(), "")
	provider := newScripted()
	provider.electErr = errors.New("NOT_CONTROLLER")
	approvedJob(t, s, provider, []string{"orders.created"}, 0)
	w := &Worker{Store: s, Providers: map[string]kafka.Provider{"demo": provider}, Owner: "w"}
	done := stepUntil(t, w, "job", "completed")
	if len(provider.elections) != electionAttempts || len(done.Warnings) != 1 {
		t.Fatalf("elections %d warnings %v", len(provider.elections), done.Warnings)
	}
}

func TestThrottleIsScopedToEachTopicAndSkippedWhenUnthrottled(t *testing.T) {
	// Throttle ownership needs a durable store.
	s, e := store.NewSQLite(context.Background(), filepath.Join(t.TempDir(), "kaflux.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	provider := throttled{newScripted()}
	approvedJob(t, s, provider, []string{"payments.authorized", "orders.created"}, 1000)
	w := &Worker{Store: s, Providers: map[string]kafka.Provider{"demo": provider}, Owner: "w"}
	stepUntil(t, w, "job", "completed")
	if len(provider.prepared) != 2 || provider.restored != 2 {
		t.Fatalf("prepared %d restored %d, want 2 each", len(provider.prepared), provider.restored)
	}
	for i, want := range []string{"payments.authorized", "orders.created"} {
		for _, c := range provider.prepared[i] {
			if c.Topic != want {
				t.Fatalf("throttle %d covers %s", i, c.Topic)
			}
		}
	}

	s2, _ := store.New(context.Background(), "")
	plain := throttled{newScripted()}
	approvedJob(t, s2, plain, []string{"orders.created"}, 0)
	w2 := &Worker{Store: s2, Providers: map[string]kafka.Provider{"demo": plain}, Owner: "w"}
	stepUntil(t, w2, "job", "completed")
	if len(plain.prepared) != 0 || plain.restored != 0 {
		t.Fatal("unthrottled job touched throttle configuration")
	}
}

func TestCancelAndRollbackQueuesReverseOfMovedTopicsOnly(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	provider := newScripted()
	provider.hold["orders.created"] = true
	approvedJob(t, s, provider, []string{"payments.authorized", "orders.created"}, 0)
	w := &Worker{Store: s, Providers: map[string]kafka.Provider{"demo": provider}, Owner: "w"}
	for i := 0; i < 5 && len(provider.calls) < 2; i++ {
		w.Step(ctx)
	}
	if e := s.RequestCancelRollback(ctx, "job", "bob"); e != nil {
		t.Fatal(e)
	}
	canceled := stepUntil(t, w, "job", "canceled")
	if canceled.RollbackJob == "" {
		t.Fatalf("no rollback queued: %s", canceled.Error)
	}
	rollback, e := s.Job(ctx, canceled.RollbackJob)
	if e != nil || rollback.State != "queued" || rollback.ApprovedBy != "bob" || rollback.RollbackOf != "job" {
		t.Fatalf("rollback = %+v %v", rollback, e)
	}
	for _, c := range rollback.Changes {
		if c.Topic != "payments.authorized" {
			t.Fatalf("rollback reverts unmoved topic %s", c.Topic)
		}
	}
	if len(rollback.Steps) != 1 || rollback.Steps[0].Topic != "payments.authorized" {
		t.Fatalf("rollback steps = %+v", rollback.Steps)
	}
	stepUntil(t, w, rollback.ID, "completed")
	snap, _ := provider.Snapshot(ctx)
	original, _ := s.Job(ctx, "job")
	if balance.Fingerprint(snap, []string{"payments.authorized"}) == "" {
		t.Fatal("no fingerprint")
	}
	current := partitionIndex(snap)
	for _, c := range original.Changes {
		if c.Topic == "payments.authorized" && fmt.Sprint(current[changeKey(c.Topic, c.Partition)].Replicas) != fmt.Sprint(c.Before) {
			t.Fatalf("partition %d not restored", c.Partition)
		}
	}
}

func TestPlanApprovedBeforeStepsStillRuns(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	provider := newScripted()
	p := approvedJob(t, s, provider, []string{"orders.created"}, 0)
	legacy, _ := s.Job(ctx, p.ID)
	legacy.Steps = nil
	_ = s.SaveJob(ctx, legacy)
	w := &Worker{Store: s, Providers: map[string]kafka.Provider{"demo": provider}, Owner: "w"}
	done := stepUntil(t, w, "job", "completed")
	if len(done.Steps) != 1 || done.Steps[0].State != model.StepDone {
		t.Fatalf("steps = %+v", done.Steps)
	}
}

func TestRestartDuringMovingTopicDoesNotResubmit(t *testing.T) {
	ctx := context.Background()
	s, _ := store.New(ctx, "")
	provider := newScripted()
	provider.hold["orders.created"] = true
	approvedJob(t, s, provider, []string{"orders.created"}, 0)
	first := &Worker{Store: s, Providers: map[string]kafka.Provider{"demo": provider}, Owner: "a"}
	first.Step(ctx)
	p, _ := s.Job(ctx, "job")
	// A crash before the moving state was saved leaves the step pending.
	p.Steps[0].State = model.StepPending
	_ = s.SaveJob(ctx, p)
	restarted := &Worker{Store: s, Providers: first.Providers, Owner: "a"}
	restarted.Step(ctx)
	restarted.Step(ctx)
	p, _ = s.Job(ctx, "job")
	if len(provider.calls) != 1 || p.Steps[0].State != model.StepMoving {
		t.Fatalf("calls %d state %s", len(provider.calls), p.Steps[0].State)
	}
}

func TestProgressUsesBytesThenFallsBackToTopicAverage(t *testing.T) {
	size := int64(1000)
	now := time.Now()
	p := model.Plan{
		StartedAt: now.Add(-10 * time.Minute),
		Changes: []model.Change{
			{Topic: "a", Partition: 0, Before: []int32{1}, After: []int32{2}},
			{Topic: "b", Partition: 0, Before: []int32{1}, After: []int32{2}},
		},
		Steps: []model.TopicStep{
			{Topic: "a", State: model.StepMoving, Partitions: 1, Bytes: &size},
			{Topic: "b", State: model.StepPending, Partitions: 1, Bytes: &size},
		},
	}
	parts := map[string]model.Partition{
		"a/0": {ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1}},
		"b/0": {ID: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
	}
	sizes := map[string]map[int32]map[int32]int64{"a": {0: {1: 1000, 2: 250}}, "b": {0: {1: 1000}}}
	updateProgress(&p, parts, sizes, now.Add(-10*time.Second))
	sizes["a"][0][2] = 750
	updateProgress(&p, parts, sizes, now)
	if *p.BytesDone != 750 || *p.BytesTotal != 2000 || p.Progress != 37 {
		t.Fatalf("bytes %d/%d progress %d", *p.BytesDone, *p.BytesTotal, p.Progress)
	}
	if p.RateBytesPerSec == nil || *p.RateBytesPerSec != 50 || p.ETABasis != "bytes" || *p.ETASeconds != 25 {
		t.Fatalf("rate %v eta %v %s", p.RateBytesPerSec, p.ETASeconds, p.ETABasis)
	}

	unknown := model.Plan{StartedAt: now.Add(-10 * time.Minute), Changes: p.Changes, Steps: []model.TopicStep{
		{Topic: "a", State: model.StepDone, Partitions: 1},
		{Topic: "b", State: model.StepPending, Partitions: 1},
	}}
	unknown.Changes[0].FinishedAt = &now
	updateProgress(&unknown, parts, nil, now)
	if unknown.BytesDone != nil || unknown.Progress != 50 || unknown.ETABasis != "topics" || *unknown.ETASeconds != 600 {
		t.Fatalf("fallback progress %d eta %v %s", unknown.Progress, unknown.ETASeconds, unknown.ETABasis)
	}
}
