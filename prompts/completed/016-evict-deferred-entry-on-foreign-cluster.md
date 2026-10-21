---
status: completed
summary: Evict pending deferred respawns when an event or reconcile read routes a task to another cluster, with regression and positive-control tests
execution_id: agent-task-executor-evict-foreign-deferred-exec-016-evict-deferred-entry-on-foreign-cluster
dark-factory-version: v0.198.7
created: "2026-10-21T12:00:00Z"
queued: "2026-10-21T11:53:58Z"
started: "2026-10-21T11:55:07Z"
completed: "2026-10-21T12:15:08Z"
---

# Evict a pending deferred respawn when a later event routes the task to another cluster

<summary>
- A task that was parked behind the concurrency cap and is then retagged to a different cluster no longer runs on the cluster it was parked on
- Today the parked copy keeps its original (untagged) snapshot and is spawned locally the moment a slot frees, producing a duplicate Job on the wrong cluster (prod incident 2026-10-21)
- The fix covers both ways a task becomes foreign: tagged for another cluster, and untagged on a cluster that refuses untagged tasks
- A regression test reproduces the exact incident sequence (defer while untagged, retag, free a slot, tick twice) and asserts no Job is spawned
- A positive-control test proves the deferred path still spawns exactly once when the task is not retagged, so the regression test cannot pass vacuously
- Cap accounting and the untagged-task policy are untouched; the periodic vault reconcile pass drops a stale parked copy the same way
- The change is recorded in the changelog
</summary>

<objective>
Make the Kafka event path evict any pending deferred-respawn entry for a task when that event routes the task to another cluster, so a retagged task is never spawned from a stale pre-retag snapshot. This stops duplicate Jobs on the cluster the task was originally deferred on (prod incident 2026-10-21: task deferred at the cap while untagged, then retagged `cluster: sun`, still spawned by the nukeprod executor once a slot freed).
</objective>

<context>
Read `README.md` for project conventions. Read these before editing:

- `pkg/handler/task_event_handler.go`
  - `parseAndFilter` — calls `h.cleanupTerminalTask(task)`, then `if h.skipForeignCluster(task) { return lib.Task{}, nil, true, nil }`. The skip never touches `h.deferredRespawns`.
  - `cleanupTerminalTask` — existing precedent: it calls `h.removeDeferredEntry(task.TaskIdentifier)` so a terminal-phase event cancels a stale spawn.
  - `removeDeferredEntry(id lib.TaskIdentifier)` — takes `h.deferredMu`, deletes from `h.deferredRespawns`. Reuse it; do not add a new helper.
  - `skipForeignCluster(task lib.Task) bool` and `isForeignCluster` — true for a task tagged for another cluster AND for an untagged task when `runUntagged` is false.
  - `spawnIfNeeded` / the cap-deferral block (stores `deferredEntry{task, config, retryAfter}` with `retryAfter = now + concurrencyCapRetryDelay`, 60s) and the grace-window deferral — these store a snapshot of the task at deferral time.
  - `evalDeferredRespawns` — spawns `entry.task` from that snapshot with no cluster re-check. Leave it unchanged; the eviction at event time is the fix.
- `pkg/handler/task_event_handler_deferred_test.go` — `buildQueueTask(id, jobStartedAt)`, and inside `Describe("EvalDeferredRespawns (spec 037)")` the spec `It("admits a never-attempted task ahead of an already-attempted one")`, which is the model for the new tests (uses `fakeResolver.ResolveReturns` with `MaxConcurrentJobs: 1`, a `running` counter, `fakeSpawner.ListActiveJobsStub` returning `activeJobs(running)`, `currentDateTime.SetNow(libtimetest.ParseDateTime(...))`, `h.ConsumeMessage(ctx, buildMsg(task))`, `h.EvalDeferredRespawns(ctx)`).
- `pkg/handler/task_event_handler_test.go` — `Context("cluster routing")` shows the `handler.NewTaskEventHandler(...)` argument list for a handler with a non-empty cluster, and `activeJobs(n int)`.
- `CHANGELOG.md` — match the existing bullet style under `## Unreleased` (create the heading above `## v0.23.1` if absent).
</context>

<requirements>
1. In `pkg/handler/task_event_handler.go`, `parseAndFilter`: inside the `if h.skipForeignCluster(task) {` branch, call `h.removeDeferredEntry(task.TaskIdentifier)` before the `return lib.Task{}, nil, true, nil`. Add a one-line comment saying a pending deferred entry holds a pre-retag snapshot and must not outlive a routing change. Apply the same eviction in `reconcileTask`: inside its `if h.isForeignCluster(task) {` branch, call `h.removeDeferredEntry(task.TaskIdentifier)` before `return task, nil, false, nil`. The reconcile pass reads the vault file fresh, so it is the backstop when a reconcile-driven cap deferral stored the pre-retag snapshot after the Kafka retag event was already consumed. Do not change `skipForeignCluster`, `isForeignCluster`, `evalDeferredRespawns`, `spawnIfNeeded`, the cap accounting, `runUntagged` semantics or any other part of the reconcile path. (Known residual: `evalDeferredRespawns` releases `deferredMu` before `spawnIfNeeded` may re-defer; a retag landing in that window is caught by the next reconcile pass via this backstop.)

2. Update the `removeDeferredEntry` doc comment so it also names the foreign-cluster skip as a caller (it currently mentions only the terminal-phase event).

3. In `pkg/handler/task_event_handler_deferred_test.go`, inside `Describe("EvalDeferredRespawns (spec 037)")`, add two specs next to `It("admits a never-attempted task ahead of an already-attempted one")`, following that spec's structure:

   a. `It("does not spawn a deferred task that a later event retagged to another cluster")` (regression):
      - `fakeResolver.ResolveReturns(pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest", MaxConcurrentJobs: 1}, nil)`.
      - Set a `running := 1` counter and `fakeSpawner.ListActiveJobsStub` returning `activeJobs(running)`.
      - Build a separate handler with `handler.NewTaskEventHandler(...)` using the same argument list as the suite `BeforeEach` / the "cluster routing" context, except cluster `"nukeprod"` and runUntagged `true`; reuse `fakeSpawner`, `fakeResolver`, `fakeResultPublisher`, a fresh `pkg.NewTaskStore()`, the suite's `currentDateTime` and `fakeGitRestClient`.
      - `currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-21T08:00:00Z"))`; `ConsumeMessage` the untagged task `buildQueueTask("tid-retagged", "")` at cap -> `SpawnJobCallCount()` is 0 (deferred).
      - `ConsumeMessage` the same id again with `Frontmatter["cluster"] = "sun"` -> still 0 spawns.
      - Set `running = 0`; `SetNow` to `2026-10-21T08:01:30Z` (past the 60s retry delay), call `EvalDeferredRespawns(ctx)`; `SetNow` to `2026-10-21T08:02:30Z` and call it again. Both must `Succeed()`.
      - Assert `fakeSpawner.SpawnJobCallCount()` equals 0.

   b. `It("spawns a deferred task exactly once when no later event retags it")` (positive control): identical setup and handler with id `tid-not-retagged`, but skip the retag message; after `running = 0` and advancing to `2026-10-21T08:01:30Z`, one `EvalDeferredRespawns(ctx)` leaves `SpawnJobCallCount()` equal to 1, and the spawned task id is `tid-not-retagged`.

   c. `It("drops a deferred entry when a reconcile pass reads the task as foreign")`: defer `tid-retagged` at cap via `ConsumeMessage` (untagged), then set `fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-retagged.md"}, nil)` and `GetReturns` a file whose frontmatter carries the same `task_identifier` and `cluster: sun` (model on the `queued_at on cap deferral` Describe in the same file), call `ReconcileOnce(ctx)`, set `running = 0`, advance to `2026-10-21T08:01:30Z`, call `EvalDeferredRespawns(ctx)`, and assert `SpawnJobCallCount()` equals 0.

   d. `It("does not spawn a deferred task whose later event drops its cluster tag on a cluster that refuses untagged tasks")`: a handler with cluster `"nukeprod"` and runUntagged `false`; defer `buildQueueTask("tid-untagged", "")` with `Frontmatter["cluster"] = "nukeprod"` at cap; `ConsumeMessage` the same id with no `cluster` key; set `running = 0`, advance to `2026-10-21T08:01:30Z`, call `EvalDeferredRespawns(ctx)`, and assert `SpawnJobCallCount()` equals 0.

   Share the setup between (a)–(d) via a local closure or helper inside the `Describe` rather than copy-pasting it. Tests must drive the real `ConsumeMessage` -> `EvalDeferredRespawns` path (no direct map access).

4. In `CHANGELOG.md`, add under `## Unreleased` (create it above `## v0.23.1` if missing) exactly this bullet:
   `- fix: **a task retagged to another cluster while parked behind the concurrency cap no longer runs on the cluster it was parked on.** The deferred-respawn entry held the pre-retag snapshot and was spawned locally once a slot freed (prod 2026-10-21, nukeprod ran a \`cluster: sun\` task). A foreign-cluster event or reconcile read now evicts the pending entry.`

   In `README.md`, in the `CLUSTER` bullet, append: "Such an event also drops any respawn still pending for that task from an earlier deferral."

5. Self-check (mandatory, in this order):
   - Temporarily remove the `h.removeDeferredEntry(...)` call in `parseAndFilter`, run `ROOTDIR=/workspace go test -mod=mod ./pkg/handler/...`, and confirm specs (a) and (d) FAIL while (b) passes. Restore it, remove the `reconcileTask` call instead, and confirm (c) FAILS. If any of these does not fail, that test is vacuous: fix the test before continuing.
   - Restore both calls, re-run `ROOTDIR=/workspace go test -mod=mod ./pkg/handler/...`, and confirm everything passes.
   - Run `<verification>` and walk each requirement against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git
- Existing tests must still pass
- Do not change concurrency-cap accounting or `RUN_UNTAGGED` policy; the only reconcile change is the one eviction call in `reconcileTask`
- Use `github.com/bborbe/errors` conventions for any error wrapping (`errors.Wrapf(ctx, err, ...)`), never `fmt.Errorf`
- Minimal change: one `removeDeferredEntry` call each in `parseAndFilter` and `reconcileTask`, plus tests, the README sentence and the changelog bullet; no new config, flags, metrics or helpers
- Test files follow the repo's Ginkgo/Gomega style and carry the existing license header
</constraints>

<verification>
Run `cd /workspace && ROOTDIR=/workspace make precommit` -- must pass.
</verification>
