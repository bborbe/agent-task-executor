---
status: draft
spec: [011-claim-mode-dispatch-wiring]
created: "2026-10-21T23:30:14Z"
branch: dark-factory/claim-mode-dispatch-wiring
---

# Gate task dispatch on a lease claim

<summary>
- Every spawn decision now takes a lease claim immediately before the Job is created.
- The claim is taken after every existing gate, so no lease is held for a task a gate would have skipped.
- In `off` the new code returns immediately and behaviour is unchanged.
- In `shadow` the claim outcome is recorded and the Job is created regardless of the outcome.
- In `on` a successful claim is required before a Job is created; a conflict or an error skips this attempt without failing the dispatch loop.
- Claim failures are logged, never returned as errors, and never retried inside the wiring.
- The claim package keeps owning all counter increments; the wiring adds no metric.
- A `409` is treated as a conflict for now: skip in `on`, proceed in `shadow`; the takeover attempt is the next prompt.
</summary>

<objective>
Make a held git-rest lease a precondition of the Job in `on` and an observational signal in `shadow`, at the one funnel every spawn path passes through, while leaving `off` untouched.
</objective>

<context>
Read these files before changing anything:

- `pkg/handler/task_event_handler.go` — `spawnIfNeededClaimed`, especially the order of `skipForeignVaultTask`, `checkActiveCurrentJob`, `lockAssigneeSpawn`, `isJobActive`, `recentlySucceeded`, `redriveStreakCapped`, `deferIfAtConcurrencyCap`, `checkTriggerBudget`, `claimStillValid`, and `h.jobSpawner.SpawnJob`.
- `pkg/handler/claim_wiring.go` — created by prompt 1: `ClaimRefClientFactory`, `unmanagedJobControl`, `leaseManager`.
- `pkg/handler/task_event_handler_deferred.go` — `evalDeferredRespawns` and `driveCandidate`; both reach the same funnel.
- `pkg/claim/key.go` — `DeriveClaimKey(ctx, taskType, taskID, owner, repo)` and the components it rejects.
- `pkg/claim/lease_manager.go` — `LeaseManager.Claim`, and the counter increments inside `claimLocked`.
- `pkg/claim/ref_client.go` — `ErrLeaseConflict`, `ClaimRefClient`.
- `pkg/metrics/metrics.go` — the 13 pre-initialised `ClaimTotal` result labels.
- `pkg/handler/task_event_handler_claim_test.go` — the harness created by prompt 1 (`newClaimHarness`, `snapshotClaims`, `expectClaimDelta`).
- `pkg/handler/claim_wiring_internal_test.go` — the internal test package precedent from prompt 1.

Coding docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-context-cancellation-in-loops.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md`
</context>

<requirements>
1. Add to `pkg/handler/claim_wiring.go` (add these imports to the file: `lib "github.com/bborbe/agent"`, `github.com/golang/glog`, and `pkg "github.com/bborbe/agent-task-executor/pkg"`):

```go
// acquireSpawnClaim takes this executor's lease for the task immediately before
// the Job is created. It reports whether the spawn may proceed. Claim failures
// are recorded by the claim package and never returned: the next dispatch pass
// that would have spawned this task retries.
func (h *taskEventHandler) acquireSpawnClaim(ctx context.Context, task lib.Task) bool
```

   Behaviour, in order:
   - `h.claimMode == pkg.ClaimModeOff` → return `true` without touching the factory or the manager.
   - `h.leaseManager(ctx)` returns an error → `return h.claimFailureDecision(task, "lease_manager", err)`.
   - Derive the key with `claim.DeriveClaimKey(ctx, string(task.Frontmatter.TaskType()), string(task.TaskIdentifier), owner, repo)`. Owner and repo come from the task's **single** `repo` frontmatter key, which carries `<owner>/<name>` (verified against real tasks: `repo: bborbe/github-update-go-agent`). There is no `owner` key in the vault format — do not read one. Split on the **first** `/`: `raw, ok := task.Frontmatter.String("repo")`; `owner, repo := split once`. A derivation error, or a repo-keyed task whose `repo` key is missing / has no `/` / has an empty side → `return h.claimFailureDecision(task, "derive_key", err)`. A task type that is not repo-keyed (`DeriveClaimKey` ignores owner and repo for it) must not require the `repo` key at all.
   - `manager.Claim(ctx, key, string(task.TaskIdentifier))` returns nil → return `true`.
   - Any error, including `claim.ErrLeaseConflict` → `return h.claimFailureDecision(task, "claim", err)`.

2. Add the shared decision helper:

```go
// claimFailureDecision logs a claim failure and reports whether the spawn may
// proceed: shadow proceeds, on fails closed. The claim package has already
// incremented executor_claim_total for every request it made; this function
// must not increment anything.
func (h *taskEventHandler) claimFailureDecision(
	task lib.Task,
	stage string,
	err error,
) bool
```

   It logs at `glog.Warningf` with `event=claim_failed`, `stage`, `mode`, the task identifier and the error, then returns `h.claimMode == pkg.ClaimModeShadow`.

3. Insert the gate in `spawnIfNeededClaimed` between the `claimStillValid` block and `h.jobSpawner.SpawnJob`:

```go
// Lease claim: taken after every existing gate and immediately before the Job
// is created, under the per-assignee spawn lock taken above. Taken earlier it
// would hold a lease for a task a gate skips; taken later it could not prevent
// the duplicate it exists to prevent. Do not confuse this lease with the
// deferred-removal claim argument of this function.
if !h.acquireSpawnClaim(ctx, task) {
	return false, nil
}
```

   Do not move the call above `lockAssigneeSpawn`, and do not move it into `ConsumeMessage`, `evalDeferredRespawns`, or `driveCandidate`.

4. The wiring must not return an error from any claim failure. `spawnIfNeededClaimed` keeps returning `(false, nil)` when the claim is not held, so `ConsumeMessage` never tears the consumer loop down and a task is retried by the next dispatch pass.

5. Do not call `LeaseManager.Takeover` anywhere in this prompt. A `409` is a conflict: `on` skips, `shadow` proceeds. The next prompt adds the takeover attempt.

   <!-- NOTE (reviewer): the spec's Suggested Decomposition assigns AC3/AC4's `409` rows to prompt 2, but those rows require `takeover_too_early`, which only the takeover call increments. They are therefore completed in prompt 3, and prompt 2's `409` spec asserts the decision only. Prompt 3 owns AC3, AC6, and AC4's `409` row. -->

6. Create `pkg/handler/claim_gate_internal_test.go` (package `handler`). It must not import `mocks` — `mocks/task_event_handler.go` imports `pkg/handler`, so an internal test importing `mocks` is an import cycle. Define a small hand-written fake in this file:

```go
// recordingClaimRefClient is the hand-written ClaimRefClient fake for the
// internal test package. The counterfeiter fake lives in mocks, which cannot be
// imported from package handler without an import cycle.
type recordingClaimRefClient struct {
	put    func(ctx context.Context, key, expectedSha, cluster, taskID string, leaseUntil libtime.DateTime) (string, error)
	list   func(ctx context.Context, prefix string) ([]claim.ClaimRef, error)
	delete func(ctx context.Context, key, expectedSha string) error
	putCalls []claimPutCall
	listCalls []string
}
```

   with `Put`, `List`, `Delete` recording their arguments and delegating to the optional funcs; default `Put` returns `("sha-1", nil)` and default `List` returns `(nil, nil)`. Construct the handler as `&taskEventHandler{claimMode: mode, cluster: "self", currentDateTime: clock, claimRefClientFactory: func() claim.ClaimRefClient { return client }}`. Use `libtime.NewCurrentDateTime()` as the clock.

   Specs:
   - mode `off` → `acquireSpawnClaim` returns true, `len(client.putCalls) == 0`, and a factory that panics is never called.
   - mode `shadow`, Put 200 → true, exactly one `Put`, and the recorded `putCalls[0].expectedSha` is `""`.
   - mode `on`, Put 200 → true, exactly one `Put`.
   - mode `shadow`, Put returns a non-`409` error and List returns `(nil, nil)` → true, and the `error` series delta is 1.
   - mode `on`, same error → false, and the `error` series delta is 1.
   - mode `on`, Put returns `claim.ErrLeaseConflict`, List returns `(nil, nil)` → false. Assert the decision only; the conflict counter delta is completed by prompt 3.
   - key derivation failure: a task with `task_type: update-go` and a `repo` value with no `/` → mode `on` returns false, mode `shadow` returns true. Assert no `Put` call and no claim counter movement.

   Reuse the 13-label snapshot/delta helpers. Define them locally in this file (the `pkg/claim` helpers are `package claim_test` and not importable).

7. Extend `pkg/handler/task_event_handler_claim_test.go` (package `handler_test`) with the harness from prompt 1:
   - **AC5 `on`**: the fake client's `PutReturns("", errors.New("git-rest down"))` and `ListReturns(nil, nil)`; consume a qualifying task in mode `on`; `SpawnJobCallCount() == 0`; `error` +1, every other series +0.
   - **AC5 `shadow`**: same client in mode `shadow`; `SpawnJobCallCount() == 1`; `error` +1, every other series +0.
   - **AC4 successful claim**: `PutReturns("sha-1", nil)` in mode `on`; `SpawnJobCallCount() == 1`; `claimed` +1, every other series +0.
   - **AC7 (a) foreign cluster**: mode `shadow`, executor cluster `self`, task frontmatter `cluster: other`; `PutCallCount() == 0` and `TaskEventsTotal{result="skipped_foreign_cluster"}` +1.
   - **AC7 (b) concurrency cap**: mode `shadow`, config `MaxConcurrentJobs: 1`, `ListActiveJobsReturns` a map with one active task; `PutCallCount() == 0` and `TaskEventsTotal{result="deferred_concurrency_cap"}` +1. Follow the existing deferral fixture in `pkg/handler/task_event_handler_deferred_test.go` for the git-rest/queued_at fakes.
   - **AC7 (c) redrive streak cap**: mode `shadow`, a task with no `ref` and no `max_triggers`; consume it three times (three Jobs; the first takes the claim, and the later two conflict locally while `shadow` still spawns), snapshot `PutCallCount`, consume a fourth time; the fourth attempt creates no Job, makes no additional `Put`, and `metrics.RedriveCappedTotal` moves by 1.
   - Keep the AC2 `off` spec green.

8. Add a `CHANGELOG.md` entry under `## Unreleased`:
   `- feat: gate task dispatch on a git-rest lease claim after all existing gates (off/shadow/on; off unchanged)`

9. Before finishing, walk every acceptance criterion above against the change and re-run `<verification>`.
</requirements>

<constraints>
- `pkg/claim` is called, never modified. Its constants, fence arithmetic and tests are frozen by spec 009.
- The claim is taken inside `spawnIfNeededClaimed` between the last gate and `h.jobSpawner.SpawnJob`. That function is the single funnel all three spawn paths use; wiring anywhere else misses a path.
- The claim call runs under the existing per-assignee spawn lock (`defer h.lockAssigneeSpawn(...)`). Do not take the claim outside the lock.
- The `cluster:` filter runs before the claim and is unchanged. In `on`, a task this cluster is not allowed to run is skipped before any claim request.
- Keys come from `claim.DeriveClaimKey`; the wiring invents no key.
- One `LeaseManager` per process, constructed once, with the executor's `CLUSTER` value.
- No new metric and no new label value. `executor_claim_total` is incremented only inside `pkg/claim`.
- No retry loop around the claim request. A failed claim is reported and retried by the next dispatch pass that would have spawned the task.
- No claim release, restart recovery, lease `Run` loop, mismatch audit, or `spec.rerunOnLostClaim` wiring.
- Do not call `Takeover` in this prompt.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
```
ROOTDIR=/workspace make precommit
go test -race -count=1 ./...
grep -n 'acquireSpawnClaim' pkg/handler/task_event_handler.go pkg/handler/claim_wiring.go
! grep -q 'Takeover(' pkg/handler/claim_wiring.go
```
- `make precommit` exits 0.
- `go test -race -count=1 ./...` exits 0.
- The `acquireSpawnClaim` grep shows the definition and exactly one call site, inside `spawnIfNeededClaimed`.
- The `Takeover(` absence check exits 0 (takeover belongs to the next prompt).
- `! grep -q 'ClaimTotal.WithLabelValues' pkg/handler/claim_wiring.go pkg/handler/task_event_handler.go` exits 0 (the wiring increments nothing).
- `grep -rln '"github.com/bborbe/agent-task-executor/pkg/claim"' --include='*.go' main.go pkg/ | grep -v _test.go` lists only `pkg/factory/factory.go` and `pkg/handler/claim_wiring.go` as production importers.
</verification>
