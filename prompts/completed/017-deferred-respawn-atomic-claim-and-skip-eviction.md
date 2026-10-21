---
status: completed
summary: Made deferred-respawn replays atomic against eviction via per-task removal epochs, guarded both deferral stores and the final spawn decision, made every parseAndFilter skip evict, and added regression/reconcile/table tests plus metrics, README and CHANGELOG updates.
execution_id: agent-task-executor-deferred-race-exec-017-deferred-respawn-atomic-claim-and-skip-eviction
dark-factory-version: v0.198.7
created: "2026-10-21T19:00:00Z"
queued: "2026-10-21T16:37:53Z"
started: "2026-10-21T16:38:47Z"
completed: "2026-10-21T16:50:16Z"
---

# Make deferred-respawn replay atomic against eviction and evict on every filter skip

<summary>
- A deferred task that is retagged, finished or filtered out while the deferred-respawn loop is already replaying it is no longer spawned or re-parked from the stale copy
- Today the loop takes the parked copy out, releases its lock, and then spawns or re-parks that copy; an eviction landing in between finds nothing to remove and is lost
- After this change every eviction cancels an in-flight replay too, and the decision to spawn is made under the same lock the eviction takes, so the two cannot interleave
- Every reason an incoming event is skipped (type mismatch, status, phase, stage, empty assignee, unresolvable agent config, foreign cluster, terminal phase) now also drops any parked copy of that task
- A regression test forces the eviction into the exact gap with a deterministic hook (no sleeps) and fails on the old code; a positive control proves an un-evicted replay still spawns exactly once
- One test per skip reason proves the parked copy is gone afterwards
- The change is recorded in the changelog
</summary>

<objective>
Close the residual race left by the foreign-cluster eviction fix: `evalDeferredRespawns` removes an entry under `deferredMu`, releases it, then calls `spawnIfNeeded` with the stale snapshot, which can spawn it or store it back into `deferredRespawns`. An eviction (`removeDeferredEntry`) arriving in that gap is a no-op, so a task retagged to another cluster can still start here. Make eviction invalidate in-flight replays and make every `parseAndFilter` skip evict, so an eviction that lands while a replay or a reconcile drive holds a task's snapshot always stops that snapshot from spawning or being re-parked. Not covered: an eviction consumed before git-rest serves the post-retag file (a stale vault read taken after the eviction gets a valid claim); the next reconcile pass's foreign read remains the backstop for that.
</objective>

<context>
Read `README.md` for project conventions. Read these before editing:

- `pkg/handler/task_event_handler.go`
  - `deferredEntry` struct, `taskEventHandler` struct (`deferredMu`, `deferredRespawns`) and `NewTaskEventHandler` (map initialisation).
  - `evalDeferredRespawns` — collects ready entries and `delete`s them under `deferredMu`, unlocks, sorts with `sortDeferredByAdmissionPriority`, then calls `h.spawnIfNeeded(ctx, entry.task, &entry.config)` per entry.
  - `spawnIfNeeded(ctx, task, config) (bool, error)` — three callers: `ConsumeMessage`, `evalDeferredRespawns`, and the reconcile pass (`spawnIfNeeded(ctx, candidate.task, candidate.config)`). It takes `lockAssigneeSpawn(config.Assignee)` and eventually calls `h.jobSpawner.SpawnJob`.
  - The two places that store into `h.deferredRespawns` from inside the spawn path: the grace-window deferral in `checkActiveCurrentJob` (`respawn_grace_window`) and the cap deferral at the end of `deferIfAtConcurrencyCap` (`deferred_concurrency_cap`).
  - `removeDeferredEntry(id)` — current callers: `cleanupTerminalTask`, the foreign-cluster branch and the terminal-phase branch of `parseAndFilter`, and `reconcileTask`.
  - `parseAndFilter` — skip branches that currently do NOT evict: `resolveConfig` `skip`, `taskTypeMismatchReason` (type mismatch), `effectiveTriggerStatuses` (`skipped_status`), the non-trigger phase (`skipped_phase`), `stageFilter` (`skipped_stage`), empty assignee (`skipped_assignee`). The empty-message / unmarshal / empty-TaskIdentifier skips have no task id to evict — leave them.
- `pkg/handler/task_event_handler_deferred_test.go` — `Describe("EvalDeferredRespawns (spec 037)")`, the specs `It("does not spawn a deferred task that a later event retagged to another cluster")` and `It("spawns a deferred task exactly once when no later event retags it")`: copy their setup (cap 1, `running` counter, `fakeSpawner.ListActiveJobsStub`, `currentDateTime.SetNow`, `ConsumeMessage`, `EvalDeferredRespawns`). Use `buildQueueTask`.
- `mocks/` — counterfeiter `FakeJobSpawner`; its `ListActiveJobsStub` (cap path; first called from `isJobActive` under the assignee spawn lock) and `IsJobActiveStub` (reached only when the task carries `current_job`) are the deterministic hook points: they run synchronously inside `spawnIfNeeded` after `evalDeferredRespawns` has released `deferredMu` and before the claim re-check. Do NOT evict from `SpawnJobStub` — it runs after the re-check. A stub must never call `ConsumeMessage` with an event that passes `parseAndFilter`: that re-enters `lockAssigneeSpawn` on the same goroutine and hangs the test.
- `ReconcileOnce` → `gatherCandidate` (calls `reconcileTask`) and `driveCandidate` (calls `spawnIfNeeded`) — same gather→drive gap: a retag eviction between them is a no-op today.
- `pkg/metrics/metrics_test.go` — the `ContainElements` list of pre-initialised labels.
- `README.md` — the sentence saying the foreign-cluster event drops any pending respawn.
- `CHANGELOG.md` — add one bullet under the existing `## Unreleased` heading in the existing style.
</context>

<requirements>
1. **Removal epoch.** Add `deferredEpochs map[lib.TaskIdentifier]uint64` to `taskEventHandler`, guarded by `deferredMu`, initialised in `NewTaskEventHandler`. `removeDeferredEntry` increments the task's epoch in the same critical section that deletes the map entry (always, even when no entry exists). Never delete from `deferredEpochs` (a reset epoch would re-validate a stale claim). Update the `removeDeferredEntry` doc comment to list all callers.

2. **Claim on collection.** Add a small `deferredClaim{id lib.TaskIdentifier; epoch uint64}` type with a doc comment. In `evalDeferredRespawns`, capture `h.deferredEpochs[taskID]` into a claim alongside each collected entry, in the same locked section that deletes it.

3. **Guarded spawn path.** Give the spawn path an optional claim: e.g. an internal `spawnIfNeededClaimed(ctx, task, config, claim *deferredClaim)` that `spawnIfNeeded` (the `ConsumeMessage` path) calls with `nil`, while `evalDeferredRespawns` and `driveCandidate` (requirement 3a) pass a claim. With a non-nil claim whose epoch no longer matches `h.deferredEpochs[claim.id]`:
   - neither deferral store (grace window, cap) may write the entry back — perform the epoch check and the store under one `deferredMu` hold;
   - no Job may be spawned — on the claimed path, re-check the epoch under `deferredMu` immediately before `h.jobSpawner.SpawnJob`, i.e. after `checkTriggerBudget` returns false (both deferral stores and `checkActiveCurrentJob` have already run), then release it with an explicit `h.deferredMu.Unlock()` before calling `SpawnJob`. Do NOT take `deferredMu` any earlier in the spawn path — `checkActiveCurrentJob` and `deferIfAtConcurrencyCap` lock it themselves and `sync.Mutex` is not reentrant — and do NOT `defer` the unlock. Do not hold it across `SpawnJob`: an eviction blocked behind an in-flight `SpawnJob` cannot stop that Job, while holding the global `deferredMu` across a rate-limited k8s create would stall the Kafka consumer. Lock order stays assignee spawn lock → `deferredMu`; state this in a comment.
   - On the cap path, also do a cheap epoch pre-check at the top of `deferIfAtConcurrencyCap`'s deferral branch, before the metrics, the `concurrency_cap` log and `stampQueueEntry`, so an invalidated claim publishes no `queued_at`; keep the authoritative check at the store. For the grace-window store, a one-line guard with a comment noting it is effectively unreachable on replay (retryAfter is already past the grace window) is enough.
   - On a claimed cap store, store only if no entry exists for the id (a concurrent fresh event may have parked a newer snapshot).
   - Count the drop with a new `TaskEventsTotal` label `deferred_claim_invalidated`, pre-initialised with `.Add(0)` in `pkg/metrics/metrics.go` next to the existing labels, and log it at `V(1)` with `event=deferred_claim_invalidated task=<id>`.

3a. **Reconcile claim.** Add a `claim deferredClaim` field to `reconcileCandidate`. In `gatherCandidate`, once `reconcileTask` has returned `ok` with a non-empty `task.TaskIdentifier`, capture `h.deferredEpochs[task.TaskIdentifier]` under `deferredMu` into the candidate; `driveCandidate` passes `&candidate.claim` to the claimed spawn path. An invalidated drive returns `reconcileOutcomeDeferred` like any other non-spawn. The claimed-store rule from requirement 3 (store only if no entry exists) now also applies to reconcile drives — accepted.

4. **Evict on every skip.** In `parseAndFilter`, call `h.removeDeferredEntry(task.TaskIdentifier)` in each skip branch listed in `<context>` that does not already evict (resolveConfig skip, type mismatch, status, non-trigger phase, stage, empty assignee). Keep the existing metrics and log lines. The status-skip eviction also fires for a stale terminal event that `cleanupTerminalTask` deliberately kept (store `current_job` ≠ event `current_job`); this is accepted because the reconcile floor re-drives a still-`in_progress` task within one interval — say so in a comment at the status branch.

5. **Tests** in `pkg/handler/task_event_handler_deferred_test.go`, inside `Describe("EvalDeferredRespawns (spec 037)")`:
   a. `It("does not spawn or re-defer a replay whose entry was evicted mid-replay")`: defer a task at cap 1 (as in the retag spec); set `running = 0`; install a `ListActiveJobsStub` (not `IsJobActiveStub` — `buildQueueTask` carries no `current_job`, so `IsJobActive` is never called on this path) that, on its first call after an `armed` flag is set immediately before `EvalDeferredRespawns` (the setup `ConsumeMessage` also calls `ListActiveJobs`), invokes `routingHandler.ConsumeMessage` with the same task retagged `cluster: sun` (`routingHandler := newRoutingHandler("nukeprod", true)`, as in the retag spec) — this lands the eviction exactly in the gap, deterministically. Assert `SpawnJobCallCount()` is 0 after that tick, and that a second `EvalDeferredRespawns` after the retry delay still spawns nothing (proves no re-defer). Confirm, by temporarily reverting your requirement-3 change locally, that this spec fails on the old code; note that in a comment above the spec.
   b. A second variant of (a) where the stub keeps the agent at cap (`running` stays 1) during the first replay tick, so the replay reaches the cap-deferral store: assert 0 spawns after that tick; then set `running = 0` and run two more `EvalDeferredRespawns` ticks, each past the 60 s retry delay; assert `SpawnJobCallCount()` is still 0. Without freeing the slot the spec also passes on the old code. Note in a comment that this spec fails on the old code.
   c. `It("spawns an un-evicted replay exactly once")` positive control with the same stub shape, but the stub evicts a different task id (consume `tid-other` retagged `cluster: sun`) instead of the replayed one: assert the stub fired, exactly 1 spawn, and `SpawnJobArgsForCall(0)` is the replayed task.
   In (a) and (b) also assert a delta of 1 on `TaskEventsTotal{result="deferred_claim_invalidated"}`.
   d. A `DescribeTable` with one `Entry` per skip reason from requirement 4 (type mismatch, status, non-trigger phase, stage, empty assignee, resolveConfig skip) plus foreign cluster and terminal phase: defer the task at cap, consume an event that triggers that skip, free the slot, tick past the retry delay, assert 0 spawns. Each Entry must first assert the task was actually parked (0 spawns and a delta of 1 on `TaskEventsTotal{result="deferred_concurrency_cap"}` after the first event). Skip-triggering second events, built from `buildQueueTask`: type mismatch — resolver Config `TaskType: "x"` for both events, first task `task_type: x`, second `task_type: y`; status — `status: hold` (NOT `completed`/`aborted`); non-trigger phase — `phase: todo` (NOT `human_review`/`done`); stage — `stage: dev` (suite handler has `stageFilter` true, branch `prod`); empty assignee — `assignee: ""`; resolveConfig skip — `assignee: unknown-agent` with a `ResolveStub` returning `pkg.ErrConfigNotFound` for that assignee only; foreign cluster — `cluster: sun`; terminal phase — `phase: human_review`. The replay uses the parked entry's config, never the resolver.
   e. Reconcile gap (requirement 3a), two specs. Setup: `fakeGitRestClient` `List`/`Get` serve one untagged task file (the shape of `foreignRetaggedTaskFile` without `cluster:`); a `ListActiveJobsStub` calls the same handler's `ConsumeMessage` with that task retagged `cluster: sun` on its first call inside `ReconcileOnce`. That first call is the drive's `isJobActive` — the gather phase makes no `ListActiveJobs` call and later calls in the pass are served from the pass cache — so the eviction lands after gather and before the claim re-check. (i) `running = 0`: assert 0 spawns after `ReconcileOnce`. (ii) `running = 1`: assert 0 spawns after `ReconcileOnce`, then set `running = 0`, advance the clock past the 60 s retry delay, run `EvalDeferredRespawns`, and assert still 0 spawns. Both fail on the old code (say so in a comment); assert a delta of 1 on `deferred_claim_invalidated` in each.
   Run `go test -race -mod=mod ./pkg/handler/...` as part of verification.

6. Add `"deferred_claim_invalidated"` to the `ContainElements` list in `pkg/metrics/metrics_test.go`. Update the `README.md` sentence so it says every filter skip drops a pending respawn. Add a `## Unreleased` CHANGELOG bullet matching the existing `fix: **bold headline.** detail` style, e.g. `- fix: **Evicted deferred respawns stay evicted.** A replay or reconcile drive whose entry was evicted mid-flight no longer spawns or re-parks the stale snapshot, and every event filter skip now evicts the task's deferred entry.`

7. Self-check: before finishing, re-run `<verification>` and confirm it passes; walk each summary bullet against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git
- Existing tests must still pass
- Do not change cap accounting, `runUntagged` / cluster routing semantics, the 60 s retry delay, or the reconcile pass beyond the claim capture and pass-through in requirement 3a
- `spawnIfNeeded`'s public behaviour for `ConsumeMessage` is unchanged (nil claim); the deferred loop and the reconcile drive pass a claim
- No `time.Sleep` in tests; interleaving is forced by the fake's stub
- Errors use `github.com/bborbe/errors` wrapping as in the surrounding code; logs use `glog.V(n)`
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit` -- must pass.
Run `go test -race -mod=mod ./pkg/handler/...` -- must pass.
</verification>
