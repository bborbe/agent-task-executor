---
status: draft
spec: [012-claim-lease-lifecycle-wiring]
created: "2026-10-21T23:41:52Z"
branch: dark-factory/claim-lease-lifecycle-wiring
---

# Release a claim on the vault-terminal and phase-gate paths

<summary>
- A claim is released when its task's frontmatter status becomes `completed` or `aborted` on the Kafka event path.
- A claim is released when its task's phase becomes `human_review` or `done`, which is how a task ends when no Job end is observed.
- Both paths reuse the release handle established by prompt 1; they add no new release logic and no retry.
- Only the claim of the task that ended is released; another task's held claim survives.
- A stale terminal event for an older job cycle is still ignored by the existing guard, so the newer job's claim is not released.
- A failed release is logged and left to the lease loop's retry, exactly as on the Job-end paths.
- In `off` both hooks return immediately and behaviour is unchanged.
- No new metric, no new label, no change to `pkg/claim`.
</summary>

<objective>
Wire the existing release handle to the two vault-side end points — the terminal-status cleanup and the terminal-phase gate — so a claim whose Job end is never observed is still released within one lease tick of its task ending.
</objective>

<context>
Prompt 1 of this spec creates the release handle `ReleaseTaskClaim` in `pkg/handler/claim_lifecycle.go` and the harness helpers `holdClaim`, `watcher`, `snapshotClaims`, and `expectClaimDelta`. Read prompt 1 before starting.

Read these files before changing anything:

- `pkg/handler/task_event_handler.go` — `cleanupTerminalTask` (the `status != "completed" && status != "aborted"` guard and the stale-`current_job` guard that returns early), `applyPhaseGate`, `parseAndFilter` (the `cleanupTerminalTask` call and the phase-gate block), and the status/phase filters that surround the gate.
- `pkg/handler/claim_lifecycle.go` — `ReleaseTaskClaim`, created by prompt 1.
- `pkg/handler/task_event_handler_claim_test.go` — the harness, `holdClaim`, `snapshotClaims`, `expectClaimDelta`, and `consume`.
- `pkg/handler/task_event_handler_claim_release_test.go` — the prompt-1 release specs.
- `pkg/claim/lease_manager.go` — `Release` and `HeldClaims`.
- `pkg/claim/lease_manager_tick.go` — `retryPendingReleases`; this is the only retry.
- `docs/claim-lease-protocol.md` — the `released` / pending-release semantics.

Coding docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md`
</context>

<requirements>
1. Change `cleanupTerminalTask` in `pkg/handler/task_event_handler.go` to take the context:

```go
func (h *taskEventHandler) cleanupTerminalTask(ctx context.Context, task lib.Task)
```

   Update its single call site in `parseAndFilter` to `h.cleanupTerminalTask(ctx, task)`. Do not change any other caller; there is exactly one production call site.

2. Inside `cleanupTerminalTask`, call `h.ReleaseTaskClaim(ctx, task.TaskIdentifier)` AFTER the existing stale-event guard (`if storedJob := stored.Frontmatter.CurrentJob(); storedJob != "" && storedJob != task.Frontmatter.CurrentJob() { ... return }`) and BEFORE `h.taskStore.Delete(task.TaskIdentifier)`. The ordering is load-bearing: the stale guard deliberately keeps the store entry for a newer job, and releasing there would drop the claim of a job that is still running.

   The existing early return for a status that is neither `completed` nor `aborted` stays first, so a non-terminal event releases nothing.

3. In `parseAndFilter`, inside the existing phase-gate block:

```go
if phase != nil && applyPhaseGate(task, *phase) {
	h.removeDeferredEntry(task.TaskIdentifier)
	return lib.Task{}, nil, true, nil
}
```

   add `h.ReleaseTaskClaim(ctx, task.TaskIdentifier)` as the first statement of the block, before `h.removeDeferredEntry(...)`. Keep `applyPhaseGate` itself a pure function (no handler receiver, no release inside it); the release belongs to the caller that knows the gate suppressed the task.

4. Do not change the status filter, the phase filter, the stage filter, or the assignee filter. The phase gate must still fire only after the status filter has admitted the task, so a task at a terminal phase with a non-triggering status is handled by `cleanupTerminalTask` only when its status is terminal.

5. Extend `pkg/handler/task_event_handler_claim_release_test.go` (package `handler_test`, Ginkgo + Gomega) with these specs. Every counter assertion uses `snapshotClaims`/`expectClaimDelta` from the harness. Every fixture holds two claims, `task-a` and `task-b`, with `PutStub` returning `"sha-"+taskID`, and asserts the survivor.

    a. **AC1(c) terminal status releases only its claim.** Mode `shadow`. `holdClaim("task-a")` and `holdClaim("task-b")`. Consume a task event for `task-a` whose frontmatter has `status: completed`. Assert: `DeleteCallCount() == 1`; `DeleteArgsForCall(0)` has key `"claims/task/task-a"` and sha `"sha-task-a"`; `expectClaimDelta(before, after, map[string]float64{"released": 1})`; `HeldClaims()` contains `task-b` and not `task-a`.

    b. **AC1(c) `aborted` behaves the same.** Repeat (a) with `status: aborted`. The status need not pass any later filter: `cleanupTerminalTask` runs before the filters in `parseAndFilter`.

    c. **AC1(d) terminal phase releases only its claim.** Mode `shadow`. `holdClaim("task-a")` and `holdClaim("task-b")`. Consume a task event for `task-a` with `status: in_progress`, `phase: human_review`, `assignee: claude`, and `stage: prod`. Assert the same four facts as (a). `in_progress` is the default trigger status, so the event reaches the phase gate.

    d. **AC1(d) `done` behaves the same.** Repeat (c) with `phase: done`.

    e. **A failed release on the vault path is left to the tick.** Mode `shadow`. `holdClaim("task-a")`. Set `harness.client.DeleteReturnsOnCall(0, errors.New("git-rest throttled"))`. Consume the `completed` event for `task-a`. Assert `DeleteCallCount() == 1`, `released` delta 0, `error` delta 1, and `task-a` still in `HeldClaims()`. This proves the wiring did not add a retry loop of its own.

    f. **A double end event releases once.** Mode `shadow`. `holdClaim("task-a")`. Consume the `completed` event for `task-a`, then consume it again. Assert `DeleteCallCount() == 1` and `released` delta 1 (the second event finds nothing held and is a no-op).

6. Keep every prompt-1 spec green, including the two Job-end specs, the failed-spawn spec, and both AC2 specs.

7. Add a `CHANGELOG.md` entry under `## Unreleased`:
   `- feat: release the dispatch claim when a task reaches a terminal status or phase`

8. Before finishing, walk every acceptance criterion above against the change and re-run `<verification>`.
</requirements>

<constraints>
- `pkg/claim` is called, never modified. `Release` already sets `releasePending` on a failed delete and `retryPendingReleases` retries it on the next tick; the wiring must not duplicate that retry.
- `Release` for a key the manager does not hold returns nil without a counter change — a double release is a no-op, not an error, and must not be logged as one.
- The release is resolved by task id from `HeldClaims()`. No vault read and no `Config` lookup is added to either hook.
- `applyPhaseGate` stays a pure package-level function. The release is added at its call site.
- `cleanupTerminalTask` releases only after its stale-event guard; a stale terminal event for an older job cycle must not release the newer job's claim.
- No `Recover`, no lease `Run` loop, no mismatch audit, and no `spec.rerunOnLostClaim` wiring — prompt 3 owns those.
- No new metric, no new label value, no new alert. `executor_claim_total` is incremented only inside `pkg/claim`.
- In `off` no lease manager is constructed and `ReleaseTaskClaim` returns immediately.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
```
ROOTDIR=/workspace make precommit
go test -race -count=1 ./...
grep -n 'cleanupTerminalTask(' pkg/handler/task_event_handler.go
grep -n 'ReleaseTaskClaim(' pkg/handler/task_event_handler.go pkg/handler/claim_lifecycle.go
```
- `make precommit` exits 0.
- `go test -race -count=1 ./...` exits 0.
- The `cleanupTerminalTask(` grep shows the definition, the `parseAndFilter` call site, and any test call sites; all pass a context.
- The `ReleaseTaskClaim(` grep shows the implementation in `claim_lifecycle.go` and exactly two call sites in `task_event_handler.go`: the `cleanupTerminalTask` hook and the phase-gate call site.
- `! grep -q 'ClaimTotal.WithLabelValues' pkg/handler/task_event_handler.go` exits 0 (the wiring increments nothing).

Completion report (appended by dark-factory): set `"status":"success"` only when `make precommit` exits 0; otherwise `"status":"failed"`. Include the actual `make precommit` exit code in the `verification` block.
</verification>
