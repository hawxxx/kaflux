# Rebalance execution control

Status: approved in conversation on 2026-10-08. Visual reference:
<https://claude.ai/artifact/PUnnobBz8hiYGdwAZDCi4P> (mockup with example data).

## Goal

Run rebalances the way the production-proven `kafka-rebalance-msk.sh` script does, inside Kaflux, with
the visibility and control operators expect from a console:

- one topic at a time, the whole topic per step, no per-broker cap;
- a preferred leader election after every topic (also after rollback topics);
- live progress: topics, partitions and bytes done/left, rate and ETA;
- a durable, colored activity log (console) per job;
- graceful pause at topic boundaries, resume, skip topic;
- cancel (unchanged semantics) and one-click cancel & roll back;
- downloads: activity log, backup in `kafka-reassign-partitions` format, report CSV;
- an optional replication throttle, which a cluster can make mandatory;
- distinct colors for every job state in Operation history.

Non-goals: per-broker concurrency caps, push transport (polling stays), turning a throttle off in the
middle of a run.

## Data model (`model.Plan`, stored in the existing JSON payload)

New fields:

| Field | Meaning |
| --- | --- |
| `steps []TopicStep` | One entry per topic that has changes, in the order the user picked them. |
| `currentStep int` | Index of the step being worked on. This is the checkpoint. |
| `pauseRequested bool` | User asked to pause after the current topic. Merged like `cancellationRequested`. |
| `pauseReason string` | Why the job is paused (user, failed topic, cluster unhealthy). |
| `rollbackRequested bool`, `rollbackRequestedBy string` | Set by cancel & roll back. |
| `rollbackOf string` | On a rollback job: the job it reverts. |
| `rollbackJob string` | On a canceled job: the rollback job created for it. |
| `healthWaitSince *time` | When the current step started waiting for cluster health. |
| `warnings []string` | Non-fatal problems, for example a leader election that kept failing. |
| `partitionsDone`, `partitionsTotal int` | Partition progress across the job. |
| `bytesTotal`, `bytesDone *int64` | Byte progress; nil when sizes are unknown. |
| `rateBytesPerSec *int64`, `etaSeconds *int64`, `etaBasis string` | `etaBasis` is `bytes` or `topics`. |
| `finishedAt time` | When the job reached a terminal state. |

`TopicStep`: `topic`, `state` (`pending|moving|electing|done|skipped|failed`), `partitions`,
`partitionsDone`, `bytes *int64`, `bytesDone *int64`, `startedAt`, `finishedAt`, `error`,
`electionAttempts`.

`Change` gains `finishedAt` (set when the worker first sees the partition at its target).

`throttleBytesPerSec = 0` means unthrottled.

New job state `paused`. A paused job is active: it holds the per-cluster lock and cannot be deleted,
but the worker does not claim it, so a paused job makes no Kafka calls. The active state list lives in
one place in the store package and is used by every query, guard and index. Existing `rollback-queued`
stays in the lists for compatibility; new rollback jobs use `queued` with `rollbackOf` set.

### Storage

- New table `kaflux_job_events(job_id, seq, at, level, message)`, primary key `(job_id, seq)`, in
  Postgres and SQLite. The newest 2000 rows per job are kept. Rows are deleted with the job. The in-memory
  store keeps the same shape.
- The active-job unique index is recreated as `kaflux_one_active_job_v2` including `paused`; the old index
  is dropped. SQLite schema version goes to 2 with an in-place migration from 1.
- `requireThrottle` is added to the cluster YAML (`config.Cluster`) and exposed on `model.Cluster` so the
  UI can lock the switch.

## Worker

The 2-second loop, lease and fencing stay. `process` becomes a step machine:

1. `queued`: existing preflight (cluster health, capabilities, validation, fingerprint, no conflicting
   reassignment). Build `steps` if absent (older plans), read sizes, log the job start, move to `running`.
   A rollback job keeps its reversed step order.
2. Step `pending`:
   1. `pauseRequested` → state `paused`, reason "Paused by <actor>", log.
   2. Cluster health (`ValidateCluster`). If unhealthy, log once and keep waiting. After 10 minutes the
      job auto-pauses with the reason.
   3. Every change of this topic must still be at its `before` assignment. Otherwise the step fails.
   4. If throttled: prepare, save and apply throttle records for this topic's changes only.
   5. Submit the topic's changes in one `Reassign` call; step becomes `moving`.
3. Step `moving`: per-partition completion as today (target replicas and full ISR). Byte progress from
   replica sizes on the new brokers versus the leader. A progress line is logged every 30 seconds.
   When all partitions are done and none is pending: restore and delete this topic's throttle, step
   becomes `electing`. If nothing is pending but targets were not reached for 30 seconds, the step fails.
4. Step `electing`: preferred leader election for the topic's partitions. Up to 3 attempts across passes;
   after that a warning is recorded and the step is still `done`.
5. After the last step the job is `completed`.
6. A failed step logs the error and auto-pauses the job with the reason.
7. Cancel: unchanged. Pending moves of the current topic are cancelled through Kafka; later topics are
   never started; the job ends `canceled`. If `rollbackRequested` is set, the worker builds the rollback
   job from the current metadata and stores the canceled job and the queued rollback job in one
   transaction.

Rate is an exponential moving average of `bytesDone` deltas. ETA: remaining bytes / rate when bytes are
known; otherwise average seconds per finished topic × topics left.

Live throttle changes retarget the current topic's records. Between topics, or for a job that started
unthrottled, the request is acknowledged and the new rate is used from the next topic (or applied to the
moving topic by preparing records for it).

Provider additions (optional interfaces, demo provider implements both):

- `LeaderElector.ElectPreferredLeaders(ctx, []model.Change) error` using `kadm.ElectLeaders`.
  `ELECTION_NOT_NEEDED` is success.
- `ReplicaSizer.ReplicaSizes(ctx) map[topic]map[partition]map[broker]int64` from the cached log-dir
  observation.

Rollback planning moves from the API into `jobs.RollbackPlan(snap, job, actor)`. It reverts only
partitions whose current replicas differ from their original ones, orders steps in reverse topic order,
checks the original brokers still exist, and validates the result.

## API

Existing route `POST /clusters/{id}/rebalances/{plan}/{action}` with confirmation, plan hash, audit and
authorization (`pause`, `resume`, `cancel-rollback` authorize as `execute`).

- `pause`: state `running`; sets `pauseRequested`. 409 otherwise.
- `resume` with `{"skip": bool}`: state `paused`; re-checks capabilities; flips to `running`, clears the
  pause, resets a failed current step to `pending`, or marks it `skipped` and advances when `skip`.
- `cancel`: also allowed on `paused` (the job returns to `running` with the cancel flag set).
- `cancel-rollback`: `running` or `paused`; cancel plus `rollbackRequested`.
- `rollback` for finished jobs keeps its behavior (a `planned` rollback to review).
- `execute`: a zero throttle is allowed unless the cluster has `requireThrottle`, which returns
  `422 throttle_required`.
- `GET …/{plan}/events?after=<seq>` → `{events:[{seq,at,level,message}], last}`.
- `GET …/{plan}/events.txt`, `…/backup.json`, `…/report.csv` are downloads.
- `progress` stays and is byte-based when bytes are known.

`docs/openapi.yaml` and `docs/rebalancing.md` are updated.

## UI

Follows the mockup.

- Plan form: throttle switch (on, 10 MiB/s default, readable size hint), locked on when the cluster
  requires it. Topic chips are numbered in run order and can be reordered.
- Job panel (`RebalanceProgress.tsx`): state pill, current topic, overall bar, ETA/rate/elapsed, the
  topics/partitions/data cards, the step list with a braille spinner on the moving topic, tinted action
  buttons: "Pause after this topic" (amber), Resume/Skip topic when paused, Cancel and Cancel & roll back
  (red). Paused jobs show an amber banner with the reason. Actions confirm in a dialog.
- Activity log (`JobConsole.tsx`): open while active, colored by token (topics purple, numbers and sizes
  number color, broker lists key color, durations string color, ✔ green, ▶ blue, ⚠ amber row, ✖ red row),
  braille spinner on the last line while running, auto-scroll with "Jump to latest", tinted downloads:
  log (blue), backup (green, shield), report (purple, table).
- Operation history: `JobState` pill per state (planned purple, queued blue, running blue pulsing,
  paused amber, completed green with ⚠ for warnings, failed red, canceled grey, rollback amber), a thin
  progress line on active rows, a short summary and relative time, "↩ rollback of …".
- Reduced motion stops the pulse and spinner.

## Testing

- Go: step machine (pause at boundary, auto-pause on failure, skip, health wait and timeout, election
  failure as warning, throttle scoped per topic, unthrottled jobs, cancel, cancel & roll back atomic
  creation, resume after restart), store (paused lock, events cap, migration), API (new actions,
  `requireThrottle`, downloads).
- Integration (real Kafka): leader election and replica sizes on the native provider.
- Vitest: `JobState`, `RebalanceProgress`, `JobConsole` coloring and auto-scroll, throttle switch.
