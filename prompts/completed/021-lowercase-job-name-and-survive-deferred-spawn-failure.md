---
status: completed
summary: Lowercased spawned Job names for RFC 1123 compliance and made the deferred-respawn loop skip a failed SpawnJob (logged, metric-counted) while keeping other errors fatal, with tests and changelog.
execution_id: agent-task-executor-spawn-survive-exec-021-lowercase-job-name-and-survive-deferred-spawn-failure
dark-factory-version: v0.198.7
created: "2026-10-22T09:35:51Z"
queued: "2026-10-22T09:40:14Z"
started: "2026-10-22T09:42:43Z"
completed: "2026-10-22T09:47:48Z"
---

# Lowercase spawned Job names and keep the deferred-respawn loop alive when one spawn fails

<summary>
- Spawned Job names are always lowercase, so a task whose identifier is an uppercase UUID no longer produces a Job name Kubernetes rejects
- On 2026-10-22 nuke dev rejected the Job "trading-backtest-agent-1AACF3FC-20261022092613" because Job names must be lowercase
- A single failed Job creation during a deferred respawn no longer stops the executor: the failure is logged with the task id, counted under its own metric label, and the loop moves on to the next due task
- Every other deferred-respawn failure (for example a failed trigger-count publish) still stops the loop and exits the process exactly as today
- A failed deferred spawn is not re-parked; the reconcile floor re-drives the task on its next pass, as for any other dropped deferral
- New tests prove the lowercase name for an uppercase task id and that a failed first spawn does not prevent the second due task from spawning
- The changelog records both fixes
</summary>

<objective>
Two production faults, one prompt. (1) `jobNameFromTask` in `pkg/spawner/job_spawner.go` concatenates the raw task id prefix, so an uppercase task id yields a Job name that violates RFC 1123 and `SpawnJob` fails. (2) That `SpawnJob` failure, when it happens inside `evalDeferredRespawns`, is returned up through `RunDeferredRespawnLoop`, which exits the whole process. End state: Job names are lowercase, and a `SpawnJob` failure in the deferred loop is logged, counted and skipped, while every other error from the deferred loop stays fatal.
</objective>

<context>
Read `README.md` for project conventions. Read these before editing:

- `pkg/spawner/job_spawner.go`
  - `func jobNameFromTask(assignee string, taskID lib.TaskIdentifier, now libtime.DateTime) string` and its doc comment. Current body:
    ```go
    if assignee == "" {
        assignee = "agent"
    }
    id := string(taskID)
    if len(id) > 8 {
        id = id[:8]
    }
    return assignee + "-" + id + "-" + now.UTC().Format("20060102150405")
    ```
  - Its only caller is `(*jobSpawner).SpawnJob` (`jobName := jobNameFromTask(assignee, task.TaskIdentifier, now)`); the returned name is also `SpawnJob`'s return value.
- `pkg/spawner/job_spawner_test.go` — `Describe("JobSpawner")` / `Describe("SpawnJob")`; the `BeforeEach` builds `fakeClient = fake.NewClientset()`, sets the clock to `2026-04-03T17:35:00Z`, and calls `spawner.NewJobSpawner(...)`. Model the new spec on `It("uses assignee from frontmatter in job name")`, which lists `fakeClient.BatchV1().Jobs("test-ns")` and asserts `jobs.Items[0].Name`. The package has no `export_test.go`; test through `SpawnJob` and the fake clientset only.
- `pkg/handler/task_event_handler.go`
  - Imports already include `stderrors "errors"` and `"github.com/bborbe/errors"`.
  - `spawnIfNeededClaimed(ctx, task, config, claim *deferredClaim) (bool, error)` — near its end:
    ```go
    jobName, err := h.jobSpawner.SpawnJob(ctx, task, *config)
    if err != nil {
        metrics.TaskEventsTotal.WithLabelValues("error").Inc()
        return false, errors.Wrapf(ctx, err, "spawn job for task %s failed", task.TaskIdentifier)
    }
    ```
    `finishSpawn` (called after a successful `SpawnJob`) can also return an error, e.g. a failed `recordTriggerSpawn` → `PublishIncrementTriggerCount`. That error must NOT be classified as a spawn failure.
  - Package-level sentinel style for reference: `pkg/handler/task_view.go` (`var errEmptyTaskEvent = stderrors.New(...)`).
- `pkg/handler/task_event_handler_deferred.go`
  - Imports: `context`, `sort`, `time`, `lib`, `github.com/bborbe/errors`, `glog`, `pkg`, `pkg/metrics` — no `stderrors` yet.
  - `evalDeferredRespawns(ctx)` — the loop over `ready`:
    ```go
    spawned, err := h.spawnIfNeededClaimed(ctx, entry.task, &entry.config, &replay.claim)
    if err != nil {
        return errors.Wrapf(
            ctx, err, "deferred respawn for task %s", entry.task.TaskIdentifier,
        )
    }
    ```
  - `RunDeferredRespawnLoop(ctx)` — returns any error from the initial eval and from each ticker tick; do not change it.
  - `EvalDeferredRespawns(ctx)` is the exported test entry point (interface `TaskEventHandler` in `task_event_handler.go`).
- `pkg/handler/task_event_handler_deferred_test.go`
  - `Describe("EvalDeferredRespawns (spec 037)")`: the `buildGraceTask` helper, the time constants (`insideGrace`, `graceExpiredR`, ...), and the `BeforeEach` setting `fakeSpawner.IsJobActiveReturns(false, nil)` and `fakeSpawner.SpawnJobReturns("job-deferred-1", nil)`.
  - `It("RunDeferredRespawnLoop returns error when initial evalDeferredRespawns fails")` and `It("RunDeferredRespawnLoop returns error when evalDeferredRespawns fails on ticker tick")` — they inject `PublishIncrementTriggerCount` failures and assert the loop returns them. They must stay green WITHOUT modification. The tick spec shows the two-grace-task setup to copy (`taskA`/`taskB` with distinct `TaskIdentifier`s, `ConsumeMessage` at `insideGrace`, clock moved to `graceExpiredR`).
  - Metric deltas are read with `testutil.ToFloat64(metrics.TaskEventsTotal.WithLabelValues("..."))` before/after.
- `pkg/metrics/metrics.go` — the block of `TaskEventsTotal.WithLabelValues("...").Add(0)` pre-initialisations (ends with `"deferred_claim_invalidated"`).
- `pkg/metrics/metrics_test.go` — `It("pre-initializes all task_events_total label combinations")` and its `ContainElements` list.
- `CHANGELOG.md` — top section is `## v0.26.3`; there is no `## Unreleased` yet.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
</context>

<requirements>
1. **Lowercase Job names.** In `pkg/spawner/job_spawner.go`, change `jobNameFromTask` to return `strings.ToLower(assignee + "-" + id + "-" + now.UTC().Format("20060102150405"))` (add the `"strings"` import if missing). Keep the empty-assignee `"agent"` default and the 8-char id prefix. Update the doc comment: state that the whole name is lowercased because task identifiers may be uppercase UUIDs and Kubernetes rejects a `metadata.name` that is not lowercase RFC 1123 (`[a-z0-9]([-a-z0-9]*[a-z0-9])?`); keep the note that assignees must be short so the name stays within 63 chars. Do not add truncation or any other sanitisation.

2. **Typed SpawnJob failure.** In `pkg/handler/task_event_handler.go`, add an unexported error type with a doc comment explaining it marks a failed `JobSpawner.SpawnJob` call so the deferred-respawn loop can skip that entry instead of exiting:
   ```go
   type spawnJobError struct {
       err error
   }

   func (e *spawnJobError) Error() string { return e.err.Error() }

   func (e *spawnJobError) Unwrap() error { return e.err }
   ```
   In `spawnIfNeededClaimed`, change only the `SpawnJob` error return to wrap the typed error, keeping the message text and the `"error"` metric increment unchanged:
   ```go
   return false, errors.Wrapf(ctx, &spawnJobError{err: err}, "spawn job for task %s failed", task.TaskIdentifier)
   ```
   Do not touch the `finishSpawn` error path or any other return in `spawnIfNeededClaimed`.

3. **Skip spawn failures in the deferred loop.** In `pkg/handler/task_event_handler_deferred.go`, add the import `stderrors "errors"` and change the error branch in `evalDeferredRespawns`'s loop to:
   ```go
   if err != nil {
       var spawnErr *spawnJobError
       if stderrors.As(err, &spawnErr) {
           glog.Warningf(
               "event=deferred_respawn_spawn_failed task=%s err=%v",
               entry.task.TaskIdentifier, err,
           )
           metrics.TaskEventsTotal.WithLabelValues("deferred_respawn_spawn_failed").Inc()
           continue
       }
       return errors.Wrapf(
           ctx, err, "deferred respawn for task %s", entry.task.TaskIdentifier,
       )
   }
   ```
   Add a short comment above the `stderrors.As` branch: a single failed Job create (e.g. a Job the API server rejects) must not exit the process and starve every other due entry; the entry is not re-parked, the reconcile floor re-drives the task; every other error stays fatal. Update the `evalDeferredRespawns` doc comment with one sentence saying the same. `RunDeferredRespawnLoop` is unchanged.

4. **Metric label.** In `pkg/metrics/metrics.go`, add `TaskEventsTotal.WithLabelValues("deferred_respawn_spawn_failed").Add(0)` directly after the `"deferred_claim_invalidated"` line. In `pkg/metrics/metrics_test.go`, add `"deferred_respawn_spawn_failed"` to the `ContainElements` list in `It("pre-initializes all task_events_total label combinations")`.

5. **Test SC1 — uppercase task id.** In `pkg/spawner/job_spawner_test.go`, inside `Describe("SpawnJob")`, add `It("lowercases the job name for an uppercase task id")`: task `lib.Task{TaskIdentifier: lib.TaskIdentifier("1AACF3FC-7F44-4E66-A45C-EE9ACCFB739D"), Frontmatter: lib.TaskFrontmatter{"assignee": "trading-backtest-agent"}}`, `SpawnJob` with `pkg.AgentConfiguration{Image: "img:latest", Env: map[string]string{}}`; assert no error; list Jobs in `"test-ns"`, assert exactly one, and for `jobs.Items[0].Name` assert:
   - `Equal("trading-backtest-agent-1aacf3fc-20260403173500")`
   - `validation.IsDNS1123Label(name)` returns no errors (`BeEmpty()`); import `"k8s.io/apimachinery/pkg/util/validation"`. This is the apimachinery check behind the API-server rejection (the fake clientset does not run it) and it also enforces the 63-char limit
   Also assert the name returned by `SpawnJob` equals `jobs.Items[0].Name`. The existing name specs (`"claude-abc12345-20260403173500"`, `"backtest-agent-abcdefgh-20260403173500"`, `"agent-abc-20260403173500"`) are already lowercase and must keep passing unchanged.

6. **Test SC2 — one failed deferred spawn does not stop the loop.** In `pkg/handler/task_event_handler_deferred_test.go`, inside `Describe("EvalDeferredRespawns (spec 037)")`, add `It("skips a deferred entry whose SpawnJob fails and spawns the next one")`:
   - `currentDateTime.SetNow(libtimetest.ParseDateTime(insideGrace))`; build `taskA := buildGraceTask(domain.TaskPhaseExecution, 0, 3)` with `TaskIdentifier` `"tid-deferred-spawnfail-a"` and `taskB` likewise with `"tid-deferred-spawnfail-b"`; `ConsumeMessage` both (each `Expect(err).To(BeNil())`); assert `fakeSpawner.SpawnJobCallCount()` is 0 (both suppressed by the grace window and parked).
   - Configure the fake by call index so the outcome does not depend on admission-priority order (both tasks have identical `job_started_at`, so either may be first): `fakeSpawner.SpawnJobReturnsOnCall(0, "", errors.Errorf(ctx, "job name rejected"))` and `fakeSpawner.SpawnJobReturnsOnCall(1, "job-deferred-ok", nil)`.
   - Record `before` for `TaskEventsTotal{"deferred_respawn_spawn_failed"}`; set the clock to `graceExpiredR`; call `h.EvalDeferredRespawns(ctx)`.
   - Assert: the error is nil; `SpawnJobCallCount()` is 2; the two `SpawnJobArgsForCall(i)` task identifiers are different and together are exactly `{tid-deferred-spawnfail-a, tid-deferred-spawnfail-b}` (use `ConsistOf`); the `deferred_respawn_spawn_failed` delta is exactly 1; the `respawn_after_grace_window` delta is exactly 1 (record its `before` alongside).
   Do not modify the two existing `RunDeferredRespawnLoop returns error ...` specs; they prove non-spawn errors stay fatal.

7. **CHANGELOG.** Insert a `## Unreleased` section above `## v0.26.3` in `CHANGELOG.md` with two bullets in the existing `- fix: **bold headline.** detail` style:
   - one for the lowercase Job name (mention the rejected `trading-backtest-agent-1AACF3FC-...` name and RFC 1123);
   - one for the deferred loop surviving a `SpawnJob` failure (logged, counted as `agent_executor_task_events_total{result="deferred_respawn_spawn_failed"}`, entry skipped, other errors still fatal).

8. Self-check: re-run `<verification>` and confirm it passes; walk each `<summary>` bullet against the diff. Keep the total diff well under 500 lines.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git
- Existing tests must still pass; the specs `RunDeferredRespawnLoop returns error when initial evalDeferredRespawns fails` and `RunDeferredRespawnLoop returns error when evalDeferredRespawns fails on ticker tick` must pass unchanged
- Only a `SpawnJob` failure in the deferred loop is skipped; every other error from `evalDeferredRespawns` (e.g. `PublishIncrementTriggerCount` via `finishSpawn`) stays fatal and `RunDeferredRespawnLoop` keeps returning it
- Do not change error handling on the Kafka `ConsumeMessage` path (returning the error leaves the message for redelivery, which is intended) or the reconcile pass (already logs and skips spawn errors in `driveCandidate`)
- Do not change the error message text of the wrapped `SpawnJob` error; `spawnJobError.Error()` returns the inner error's message
- Do not export spawner or handler internals; the spawner package has no `export_test.go`, so test through `SpawnJob` and the fake clientset
- Errors use `github.com/bborbe/errors` wrapping (`errors.Wrapf(ctx, ...)`); type detection uses stdlib `errors.As` imported as `stderrors`; logs use `glog`
- No `time.Sleep` in tests
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit` -- must pass.
Run `go test -mod=mod ./pkg/spawner/... ./pkg/handler/... ./pkg/metrics/...` -- must pass.
Run `grep -n 'strings.ToLower' pkg/spawner/job_spawner.go` -- must print the `jobNameFromTask` return line.
Run `grep -n 'deferred_respawn_spawn_failed' pkg/handler/task_event_handler_deferred.go pkg/metrics/metrics.go pkg/metrics/metrics_test.go CHANGELOG.md` -- must print at least one line per file.
</verification>
