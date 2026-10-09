# Rebalance Execution Control Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run rebalances topic by topic with leader election, live progress/ETA, an activity log, pause/resume/skip, cancel & roll back, downloads, optional throttle and colored job states.

**Architecture:** The job stays one JSON payload; the worker becomes a per-topic step machine driven by `steps`/`currentStep`. A new events table backs the console. The UI adds three focused components fed by polling.

**Tech Stack:** Go 1.x, franz-go kadm, pgx/Postgres, SQLite, React + TanStack Query, Vitest.

**Spec:** `docs/superpowers/specs/2026-10-08-rebalance-execution-control-design.md`

## Global Constraints

- One topic at a time, the whole topic per `Reassign` call, no per-broker cap.
- Preferred leader election after every topic; failure after 3 attempts is a warning, not a failure.
- Pause is graceful: it takes effect only at a topic boundary.
- `throttleBytesPerSec = 0` means unthrottled; `requireThrottle: true` on a cluster rejects it with `422 throttle_required`.
- Default throttle in the UI stays `10485760` (10 MiB/s).
- `paused` is an active state everywhere (lock index, delete guard, frontend `activePlan`) but is never claimed by the worker.
- Events: newest 2000 per job, deleted with the job.
- No AI attribution in commits or PRs.

## Review Focus

- A job approved before this change (no `steps`) must still run: the worker builds steps from `changes` grouped by topic in `topics` order.
- A worker restart in the middle of a `moving` step must not resubmit or lose progress.
- Cancel & roll back must never leave a canceled job without its rollback job (single transaction).
- Unthrottled jobs must not touch broker throttle configuration at all.
- Sizes unavailable (MSK permissions): progress falls back to partitions and the topic-average ETA, with no errors.

---

### Task 1: Model, planner and cluster config

**Files:** `backend/internal/model/model.go`, `backend/internal/balance/planner.go`, `backend/internal/config/config.go`, `backend/cmd/kaflux/main.go`, tests in `backend/internal/balance/planner_test.go`.

**Produces:** `model.TopicStep`, new `Plan` fields (spec table), `model.ActiveJobStates`, `model.Cluster.RequireThrottle`, `balance.Steps(order []string, changes []model.Change) []model.TopicStep`.

- [ ] Test: `Generate` with topics `["b","a"]` yields steps in order b, a, skipping topics without changes; `Topics` stays sorted (fingerprint compatibility).
- [ ] Implement, run `go test ./internal/balance/`.

### Task 2: Store — paused state, user flags, events, transactional rollback creation

**Files:** `backend/internal/store/{store.go,sqlite.go,cancellation.go,job_delete.go,events.go,control.go}` plus tests.

**Produces:** `(*Store).AppendEvents(ctx, jobID string, events []model.JobEvent) error`, `(*Store).Events(ctx, jobID string, after int64, limit int) ([]model.JobEvent, error)`, `(*Store).RequestPause(ctx, id, actor string) error`, `(*Store).Resume(ctx, id string, skip bool) (model.Plan, error)`, `(*Store).RequestCancel(ctx, id string, rollbackBy string) error`, `(*Store).FinishWithRollback(ctx, finished, rollback model.Plan, owner string) error`.

- [ ] Tests (memory + SQLite): paused blocks a second queued job on the cluster; paused jobs are not returned by `ActiveJobs` and cannot be claimed; delete refuses paused; `SaveClaimedJob` keeps `pauseRequested`/`rollbackRequested` set by the API; events keep only 2000 newest and vanish with the job; SQLite v1 database migrates to v2; resume with skip marks the current step skipped and advances.
- [ ] Implement, run `go test ./internal/store/`.

### Task 3: Provider — leader election and replica sizes

**Files:** `backend/internal/kafka/{election.go,sizes.go,demo.go}` plus tests and `reassignment_integration_test.go`.

**Produces:** `kafka.LeaderElector{ ElectPreferredLeaders(context.Context, []model.Change) error }`, `kafka.ReplicaSizer{ ReplicaSizes(context.Context) map[string]map[int32]map[int32]int64 }`.

- [ ] Unit test: election results with `ELECTION_NOT_NEEDED` count as success; other errors are returned.
- [ ] Integration test: after a reassignment, election succeeds and sizes report the new replica.

### Task 4: Worker step machine

**Files:** `backend/internal/jobs/{worker.go,steps.go,progress.go,rollback.go,events.go}`, tests `steps_test.go`, existing worker/cancellation tests updated.

**Consumes:** Tasks 1–3. **Produces:** `jobs.RollbackPlan(snap model.Snapshot, p model.Plan, actor string) (model.Plan, error)`.

- [ ] Tests with a scripted provider: topics run strictly in order; pause stops at the boundary; failed step auto-pauses with reason; skip advances; health wait logs once and auto-pauses after 10 minutes; election failure x3 becomes a warning and the job completes; throttle prepared per topic and restored after it; unthrottled job never calls throttle; cancel & roll back creates a queued rollback with reversed steps of moved partitions only; legacy plan without steps runs.
- [ ] Implement, run `go test ./internal/jobs/`.

### Task 5: API

**Files:** `backend/internal/api/{api.go,rebalance_control.go}` plus tests, `docs/openapi.yaml`.

- [ ] Tests: pause/resume/cancel-rollback state rules and audit; cancel on paused; execute without throttle allowed unless `requireThrottle`; events paging; backup JSON format; report CSV header; rollback endpoint uses `jobs.RollbackPlan`.
- [ ] Implement, run `go test ./internal/api/`.

### Task 6: Frontend

**Files:** `frontend/src/{JobState.tsx,RebalanceProgress.tsx,RebalanceProgress.css,JobConsole.tsx,main.tsx,styles.css}` plus tests.

- [ ] Tests: pill class per state and warning mark; progress cards and step list; console token coloring, spinner only while running, auto-scroll; throttle switch sends 0 when off and is locked when required; history shows summary for active jobs.
- [ ] Implement, run `npm test` and `npm run build`.

### Task 7: Docs and quality gates

- [ ] Update `docs/rebalancing.md`, `config.example.yaml`, `docs/openapi.yaml`.
- [ ] Run backend `go vet ./... && go test ./...`, frontend `npm test && npm run build`.
