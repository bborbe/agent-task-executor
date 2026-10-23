---
status: completed
summary: The spawn funnel now returns a spawnOutcome so the reconcile summary reports a store-held consecutive-re-drive cap as capped instead of deferred, with a regression test pinning the post-restart case.
execution_id: agent-task-executor-redrive-visible-exec-033-report-store-held-redrive-cap-as-capped
dark-factory-version: v0.198.7
created: "2026-10-23T23:21:18Z"
queued: "2026-10-23T23:21:18Z"
started: "2026-10-23T23:24:26Z"
completed: "2026-10-23T23:31:03Z"
---

# Report a store-held redrive cap as capped in the reconcile summary

<summary>
- The reconcile pass summary currently files every refusal under one of two buckets, and one of them lies
- A task refused by the consecutive-re-drive cap is reported as "deferred", the same bucket as a task waiting for a free concurrency slot
- After a pod restart the in-memory half of the cap is empty, so a task capped in the shared store lands in "deferred" with "capped" at zero
- A fleet in which every task has been retired therefore reads as a healthy queue that is merely busy
- The spawn funnel now reports which refusal it made, and the pass summary uses it
- No cap behaviour changes: the same tasks are refused, at the same threshold, for the same reason
- The public docs describe the corrected reporting
- A new test pins the case that was invisible: a cap held only in the shared store counts as "capped"
</summary>

<objective>
Make the reconcile pass summary distinguish a task retired by the consecutive-re-drive cap from a task waiting for a concurrency slot, so a fully retired fleet cannot be misread as a healthy backlog. The cap itself, its threshold and its un-cap rules stay exactly as they are.
</objective>

<context>
Read `README.md` for project conventions.

Read these before writing anything:
- `pkg/handler/task_event_handler.go` — `spawnIfNeeded`, `spawnIfNeededClaimed`, `spawnClaimedJob`, `driveCandidate`, `reconcileOutcome`, `reconcileCounters.add`
- `pkg/handler/task_event_handler_redrive.go` — `redriveStreakCappedInMemory`, `redriveStreakCapped`, `signalRedriveCapped`, and `redriveStreakStore` (note: it returns `(nil, nil)` for `ClaimModeOff` and it calls `leaseManager`, which constructs the real store on first use)
- `pkg/handler/task_event_handler_deferred.go` — the deferred-respawn replay, one of the three call sites of `spawnIfNeededClaimed`
- `pkg/handler/task_event_handler.go` `reconcileOutcome` — the local exemplar for an internal control-flow enum: typed int, `iota`, doc comment, no `String`/`Validate`/`Available*`. Follow it; do not reach for the serialized-enum convention
- `prompts/completed/024-spec-011-handler-persisted-redrive-cap.md` — this change reverses a decision recorded there: that prompt mandated reporting a store-held cap as `deferred` and required the comment requirement 5 rewrites
- `pkg/handler/claim_spawn_order_internal_test.go` — the exemplar for constructing a `taskEventHandler` in an internal test and for the stub style (the `mocks` package imports `pkg/handler`, so an internal test of `pkg/handler` must declare its own stubs)
- `docs/claim-lease-protocol.md` — the section describing the streak refs
</context>

<requirements>
1. In `pkg/handler/task_event_handler.go`, above `spawnIfNeeded`, add an unexported outcome type:

   ```go
   type spawnOutcome int

   const (
       spawnOutcomeSuppressed spawnOutcome = iota
       spawnOutcomeSpawned
       spawnOutcomeCapped
   )
   ```

   with a `spawned() bool` method. Document on the type that it exists so the reconcile pass can tell the two refusals apart. Keep `spawnOutcomeSuppressed` the zero value so an unhandled branch can never report a spawn.

2. Change `spawnIfNeededClaimed` to return `(spawnOutcome, error)`. Every existing `return false, nil` becomes `return spawnOutcomeSuppressed, nil`, and each of the three `return false, err` (the `checkActiveCurrentJob`, `isJobActive` and `deferIfAtConcurrencyCap` error paths) becomes `return spawnOutcomeSuppressed, err`. The `redriveStreakCapped` branch returns `spawnOutcomeCapped, nil`. The final `return h.spawnClaimedJob(...)` keeps its error: capture both values and return `spawnOutcomeSuppressed, err` when err is non-nil, otherwise map the bool — true → `spawnOutcomeSpawned`, false → `spawnOutcomeSuppressed`. Do not change any gate, its order, or its conditions.

3. Keep `spawnIfNeeded`'s `(bool, error)` signature — its only caller discards the bool, so the signature is left alone. Map the outcome inside it with `outcome.spawned()`.

4. In `driveCandidate`, replace the `if !spawned { return reconcileOutcomeDeferred }` mapping with a switch on the outcome: `spawnOutcomeSpawned` keeps today's `metrics.ReconcileRedrivenTotal.Inc()` + `event=reconcile_redrive` log and returns `reconcileOutcomeRedriven`; `spawnOutcomeCapped` returns `reconcileOutcomeCapped`; anything else returns `reconcileOutcomeDeferred`. The `err != nil` branch is unchanged.

5. Rewrite the comment block above the `redriveStreakCappedInMemory` call in `driveCandidate`. It currently states that a task capped solely by the shared store "is reported as `deferred` here rather than `capped`, because the authoritative check runs in the spawn funnel below and its capped reason is deliberately not carried back". That is no longer true. Keep the in-memory pre-check as the fast path that avoids a store read, and say that the funnel now carries the reason back.

6. Update the deferred-respawn replay in `pkg/handler/task_event_handler_deferred.go` to the new return type. Its behaviour must not change: it only cares whether a Job was created.

7. Update `docs/claim-lease-protocol.md`: the bullet beginning "A task capped by the shared streak but not by this executor's memory is reported as `deferred`, not `capped`, in the reconcile pass summary" now describes the old behaviour. State that such a task is reported as `capped`, and keep the sentence about `executor_redrive_capped_total` counting every cap regardless.

8. Add a plain Go test in a new internal test file `pkg/handler/task_event_handler_capped_internal_test.go`, `package handler`, following the construction and stub style of `claim_spawn_order_internal_test.go`:
   - a stub `claim.RedriveStreakStore` whose `Count` returns `maxConsecutiveRedrives` for any task and phase, and whose `Record`/`Clear` are no-ops;
   - construct the handler with `NewTaskEventHandler(...)` exactly as that test does (keep `pkg.ClaimModeShadow`), then call `h.leaseManager(ctx)` once and only after that assign the stub to `h.redriveStore`. The warm-up call is mandatory: `redriveStreakStore` returns `(nil, nil)` when `claimMode` is `ClaimModeOff`, and `createLeaseManager` overwrites `h.redriveStore` with a real store on its first call, so a stub assigned before it is silently discarded and the case reports `deferred` instead of `capped`;
   - leave the handler's in-memory streak map empty, which is the post-restart state;
   - drive one eligible candidate through `driveCandidate`, feed the returned outcome into a `reconcileCounters` via `add`, and assert `capped == 1` and `deferred == 0` — the counters are what produce the pass summary, so assert them rather than only the returned constant;
   - give each case its own handler instance and its own store stub — the stub is assigned on the instance, so cases cannot share one;
   - second case, using a stub whose `Count` returns 0 and whose `SpawnJob` succeeds: assert `redriven == 1` and `capped == 0`, so the test cannot pass on a hardcoded `capped`;
   - third case, using the 0-count stub, with `MaxConcurrentJobs: 1` on the candidate's config and the spawner's `ListActiveJobs` returning one entry for a different task id — `deferIfAtConcurrencyCap` reads `activeJobsFor`, which calls `ListActiveJobs`, not `CountActiveJobs`: assert `deferred == 1` and `capped == 0`, so the new switch is pinned not to fold a genuine deferral into the capped bucket. `stubJobSpawner` is declared at package level in `claim_spawn_order_internal_test.go`, so add an `active map[lib.TaskIdentifier]bool` field to it (returning `active` from `ListActiveJobs`) rather than redeclaring the type, which would not compile.
   Comment on the first case that it pins the regression: a cap held only in the shared store used to be counted as `deferred`.

9. Add a `## Unreleased` bullet to `CHANGELOG.md` in the repo's existing style: a `fix:` entry stating that a task retired by the shared redrive streak is now reported as `capped` in the reconcile summary instead of `deferred`.

Before you finish, re-run `<verification>` and confirm it passes; then walk each numbered requirement against the change and confirm each one is present in the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT change cap semantics: do not alter `maxConsecutiveRedrives`, the store key shape, the un-cap rules, or which tasks are refused. This is a reporting change only.
- Do NOT add a store read to the hot path for tasks the in-memory pre-check already refuses. The in-memory pre-check stays first.
- Existing tests must still pass unchanged. No existing test call needs changing: the one direct `spawnIfNeededClaimed` call discards the first return value with `_`.
- No new dependency.
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit` — must pass.
</verification>
