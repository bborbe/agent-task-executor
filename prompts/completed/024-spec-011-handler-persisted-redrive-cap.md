---
status: completed
spec: [011-bug-redrive-streak-lost-across-clusters-and-restarts]
summary: Wired the persisted redrive streak into the task event handler via the shared claim-ref client, added cross-instance/restart/fail-safe specs and the changelog entry.
execution_id: agent-task-executor-redrive-streak-exec-024-spec-011-handler-persisted-redrive-cap
dark-factory-version: v0.198.7
created: "2026-10-23T09:27:00Z"
queued: "2026-10-23T09:42:55Z"
started: "2026-10-23T09:53:39Z"
completed: "2026-10-23T10:01:26Z"
branch: dark-factory/bug-redrive-streak-lost-across-clusters-and-restarts
---

<summary>
- The executor now checks and records the consecutive-spawn cap against the shared git-rest streak store whenever claim mode is on or shadow, so every cluster and every restarted pod sees the same streak.
- The cap refuses a spawn when the larger of the stored count and the in-memory count reaches 3, and skips the store read entirely when memory alone already reaches 3.
- A store read that fails logs a warning and falls back to the in-memory count, so an unreachable git-rest never lets a task through that this pod already capped and never retires one it has not.
- A store write that fails logs a warning and still increments the in-memory count; the Job that was already created is untouched.
- The in-memory streak is maintained exactly as before in every mode and is the safety floor.
- Claim mode off constructs no store and sends no request, so its behaviour is unchanged.
- The lease manager and the streak store are built together from the same one-time claim-ref client, so the factory still runs exactly once.
- Terminal cleanup clears the task's stored streak refs as well as the in-memory one.
- The misleading "in-memory by design" comment is replaced by the shared-store design, the fail-safe floor and the explicit un-cap rule; the frontmatter-clobber explanation stays.
- The changelog carries the fix under `## Unreleased`.
</summary>

<objective>
Wire the persisted redrive streak into the task event handler: construct one store from the same claim-ref client the lease manager uses, make the cap check `max(store count, in-memory count) >= 3`, record a successful spawn in the store, clear the store on terminal state, and make every store failure fall back to memory with a warning. Rewrite the `maxConsecutiveRedrives` rationale and add the changelog entry. Ship handler specs for cross-instance capping, restart survival, the fail-safe paths, claim-mode-off and single-construction.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for the container agent's execution rules (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, external test packages, coverage ≥80% for new code, BSD license header). This repo has no project-level `CLAUDE.md`.

Read these files fully before changing anything:
- `/workspace/pkg/handler/task_event_handler.go` — the `maxConsecutiveRedrives` doc comment and constant (~line 69-106), the `taskEventHandler` struct, `NewTaskEventHandler`, `cleanupTerminalTask` (calls `clearRedriveStreak`), `spawnIfNeeded` (calls `redriveStreakCapped` after the active-Job and recent-success checks), `finishSpawn` (calls `recordRedriveSpawn` last), `driveCandidate` (calls `redriveStreakCapped` for the pass summary), and the `redriveStreak` / `redriveStreakCapped` / `taskPhaseString` / `recordRedriveSpawn` / `clearRedriveStreak` block near the end.
- `/workspace/pkg/handler/claim_wiring.go` — `ClaimRefClientFactory`, `leaseManager(ctx)` (one-time `sync.Once`), `createLeaseManager(ctx)` (calls the factory once and builds the lease manager), `acquireSpawnClaim`, `claimFailureDecision`.
- `/workspace/pkg/handler/claim_lifecycle.go` — `ClaimLifecycleEventHandler` and `ClaimLeaseManager` (this interface is NOT widened).
- `/workspace/pkg/handler/task_event_handler_claim_test.go` — `claimHarness`, `newClaimHarness(mode)`, `consume(task)`, `qualifyingClaimTask()`, `conflictOnCreate()`, `snapshotClaims()`, `expectClaimDelta`, and the AC6 spec with `Expect(harness.client.PutCallCount()).To(Equal(2))`.
- `/workspace/pkg/handler/task_event_handler_claim_release_test.go` — `shaPut`, `terminalStatusTask`, and how terminal events reach `cleanupTerminalTask`.
- `/workspace/pkg/handler/task_view_test.go` — `captureStderr(fn func()) string`, the shared glog stderr capture helper. Reuse it; do not write another.
- `/workspace/pkg/handler/task_reconcile_test.go` — "still re-drives a task whose Job failed, up to the redrive cap" and "resets the streak when the task advances to a new phase". Do NOT edit either spec; they pin claim-mode-off behaviour.
- `/workspace/pkg/handler/claim_wiring_internal_test.go` — `TestLeaseManagerConstructsOnce`; it must keep passing unmodified.
- `/workspace/pkg/claim/redrive_streak_store.go` (created by prompt 1) — `RedriveStreakStore` with `Count(ctx, taskID, phase) (int, error)`, `Record(ctx, taskID, phase) error`, `Clear(ctx, taskID) error`, and `NewRedriveStreakStore(refClient, currentDateTime)`.
- `/workspace/pkg/metrics/metrics.go` — `RedriveCappedTotal`; its name and labels are unchanged.
- `/workspace/CHANGELOG.md` — the preamble and the top of the version list. There is currently no `## Unreleased` section.

Facts already verified, do not re-derive:
- `pkg.ClaimMode.TakesClaim()` is true for `shadow` and `on`, false for `off` and for a zero-value mode.
- `h.currentDateTime` is a `libtime.CurrentDateTimeGetter`; `h.claimMode` is a `pkg.ClaimMode`; `h.claimRefClientFactory` is a `ClaimRefClientFactory`.
- `mocks.FakeClaimRefClient` has `ListCalls`, `PutCalls`, `DeleteCalls`, `ListArgsForCall`, `PutArgsForCall`, `DeleteArgsForCall`, `ListCallCount`, `PutCallCount`, `DeleteCallCount`. `PutCalls` takes `func(context.Context, string, string, string, string, libtime.DateTime) (string, error)`.
- `claim.ErrLeaseConflict` is the 409 sentinel; `claim.ClaimRef` is `{Ref, Sha, Cluster, TaskID, LeaseUntil}` with `Ref` = `refs/...`.
- `captureStderr` toggles `logtostderr`; a `glog.V(1)` line additionally needs `flag.Set("v", "1")` around the captured call.
- `metrics.RedriveCappedTotal` is the only cap signal; `event=redrive_capped task=%s streak=%d cap=%d phase=%q` is the log format and must stay byte-identical.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md` — interface + private struct + `New*` constructor.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `bborbe/errors` wrapping.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2, counterfeiter mocks, external `_test` package.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new and modified code.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` placement and the required `fix:` prefix.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-precommit.md` — golines 100-char limit, funlen 80, license headers.
</context>

<requirements>
1. In `/workspace/pkg/handler/task_event_handler.go`, add a field to the `taskEventHandler` struct, directly after `redriveStreaks map[lib.TaskIdentifier]redriveStreak`:
   ```go
   // redriveStore is the shared, persisted half of the redrive streak. It is
   // built with the lease manager from the same claim-ref client and stays nil
   // when claim mode takes no claim.
   redriveStore claim.RedriveStreakStore
   ```

2. In `/workspace/pkg/handler/claim_wiring.go`, in `createLeaseManager`, build the store from `refClient` but assign `h.redriveStore` only after `claim.NewLeaseManager` has succeeded (so a failed construction leaves the field nil and never half-wires the handler):
   ```go
   h.redriveStore = claim.NewRedriveStreakStore(refClient, h.currentDateTime)
   ```
   This runs inside the existing `sync.Once`, so the lease manager and the store share one client instance and the factory is called exactly once. Do not build a second client, URL or secret, and do not change `TestLeaseManagerConstructsOnce`.

3. Do NOT widen the production `ClaimLifecycleEventHandler` interface in `claim_lifecycle.go`. The store is reached by tests through the unexported `redriveStreakStore(ctx)` accessor in requirement 4, from an internal test (package `handler`).

4. Add this helper to `/workspace/pkg/handler/task_event_handler.go` (near the streak functions):
   ```go
   // redriveStreakStore returns this process's streak store, or nil when claim
   // mode takes no claim. It triggers the one-time lease-manager construction, so
   // the store and the manager share one claim-ref client. An unparsed mode errors
   // through leaseManager, which makes the caller fall back to the in-memory floor.
   func (h *taskEventHandler) redriveStreakStore(
       ctx context.Context,
   ) (claim.RedriveStreakStore, error) {
       if h.claimMode == pkg.ClaimModeOff {
           return nil, nil
       }
       if _, err := h.leaseManager(ctx); err != nil {
           return nil, err
       }
       return h.redriveStore, nil
   }
   ```

5. Replace `redriveStreakCapped` with three functions. Keep `taskPhaseString` unchanged. Carry the existing `redriveStreakCapped` rationale (why `glog.V(1)` for the cap line, and why the comparison is against the task's current phase) into the new functions' doc comments, and add one sentence to `recordRedriveSpawn`'s doc comment that `Record`'s network calls run under the per-assignee spawn lock, bounded by the 30 s per-request cap.
   ```go
   // inMemoryRedriveCount returns the streak this process has recorded for id at
   // phase, or 0 when the recorded streak belongs to another phase.
   func (h *taskEventHandler) inMemoryRedriveCount(id lib.TaskIdentifier, phase string) int {
       h.redriveMu.Lock()
       defer h.redriveMu.Unlock()
       streak, ok := h.redriveStreaks[id]
       if !ok || streak.phase != phase {
           return 0
       }
       return streak.count
   }

   // signalRedriveCapped is the only place that increments
   // executor_redrive_capped_total or logs event=redrive_capped.
   func (h *taskEventHandler) signalRedriveCapped(
       id lib.TaskIdentifier,
       streak int,
       phase string,
   ) {
       metrics.RedriveCappedTotal.Inc()
       glog.V(1).Infof(
           "event=redrive_capped task=%s streak=%d cap=%d phase=%q",
           id, streak, maxConsecutiveRedrives, phase,
       )
   }

   // redriveStreakCappedInMemory is the cheap pre-check the reconcile pass uses
   // for its summary. It never reads the store; the authoritative check runs in
   // the spawn funnel.
   func (h *taskEventHandler) redriveStreakCappedInMemory(task lib.Task) bool {
       id := task.TaskIdentifier
       phase := taskPhaseString(task)
       count := h.inMemoryRedriveCount(id, phase)
       if count < maxConsecutiveRedrives {
           return false
       }
       h.signalRedriveCapped(id, count, phase)
       return true
   }

   // redriveStreakCapped reports whether the spawn must be refused because the
   // task has been spawned maxConsecutiveRedrives times at this phase. Memory is
   // checked first; when it already reaches the cap the store is not read. A
   // failed store read logs event=redrive_streak_read_failed and uses memory
   // alone. The refusal keeps today's metric and log line, with streak showing
   // the larger count actually used.
   func (h *taskEventHandler) redriveStreakCapped(ctx context.Context, task lib.Task) bool {
       id := task.TaskIdentifier
       phase := taskPhaseString(task)
       count := h.inMemoryRedriveCount(id, phase)
       if count >= maxConsecutiveRedrives {
           h.signalRedriveCapped(id, count, phase)
           return true
       }
       store, err := h.redriveStreakStore(ctx)
       if err != nil {
           glog.Warningf(
               "event=redrive_streak_read_failed task=%s phase=%q error=%v",
               id, phase, err,
           )
       } else if store != nil {
           stored, storeErr := store.Count(ctx, string(id), phase)
           switch {
           case storeErr != nil:
               glog.Warningf(
                   "event=redrive_streak_read_failed task=%s phase=%q error=%v",
                   id, phase, storeErr,
               )
           case stored > count:
               count = stored
           }
       }
       if count < maxConsecutiveRedrives {
           return false
       }
       h.signalRedriveCapped(id, count, phase)
       return true
   }
   ```

6. Update the two call sites:
   - In `spawnIfNeeded`: `if h.redriveStreakCapped(ctx, task) {`
   - In `driveCandidate`: `if h.redriveStreakCappedInMemory(candidate.task) {`
   The full check stays inside the single spawn funnel the Kafka consumer, deferred-respawn loop and reconcile floor all pass through. A task capped only by the store is therefore reported as `deferred` in the reconcile pass summary rather than `capped`, and this must be documented in `driveCandidate`'s doc comment (the capped reason is deliberately not carried back); the cap still refuses and signals from the funnel. This keeps the store read to one List per spawn decision, which is the budget the spec states.

7. Replace `recordRedriveSpawn` with:
   ```go
   // recordRedriveSpawn extends this task's in-memory streak with the spawn that
   // just happened, resetting it when the task has moved phase, then records the
   // spawn in the shared store. The in-memory update happens first and always: a
   // store failure must not roll back the Job or stop the floor from counting.
   //
   // Called only after SpawnJob returned successfully, so a suppressed, deferred
   // or capped spawn does not count — the streak measures spawns, not attempts.
   func (h *taskEventHandler) recordRedriveSpawn(ctx context.Context, task lib.Task) {
       id := task.TaskIdentifier
       phase := taskPhaseString(task)
       h.redriveMu.Lock()
       if prev, ok := h.redriveStreaks[id]; ok && prev.phase == phase {
           h.redriveStreaks[id] = redriveStreak{count: prev.count + 1, phase: phase}
       } else {
           h.redriveStreaks[id] = redriveStreak{count: 1, phase: phase}
       }
       h.redriveMu.Unlock()

       store, err := h.redriveStreakStore(ctx)
       if err != nil {
           glog.Warningf(
               "event=redrive_streak_write_failed task=%s phase=%q error=%v",
               id, phase, err,
           )
           return
       }
       if store == nil {
           return
       }
       if recordErr := store.Record(ctx, string(id), phase); recordErr != nil {
           glog.Warningf(
               "event=redrive_streak_write_failed task=%s phase=%q error=%v",
               id, phase, recordErr,
           )
       }
   }
   ```
   Update `finishSpawn`'s call to `h.recordRedriveSpawn(ctx, task)`. Keep it after `recordTriggerSpawn` and after the spawn notification, exactly where it is: a spawn whose trigger-count write fails returns before the streak is recorded (spec Non-goal).

8. Replace `clearRedriveStreak` with:
   ```go
   // clearRedriveStreak ends this task's streak in memory and in the shared store.
   // Called when the task reaches a terminal state, where a further spawn is an
   // operator decision rather than a re-drive of the loop this cap bounds. A store
   // failure is logged and never blocks the terminal handling.
   func (h *taskEventHandler) clearRedriveStreak(ctx context.Context, id lib.TaskIdentifier) {
       h.redriveMu.Lock()
       delete(h.redriveStreaks, id)
       h.redriveMu.Unlock()

       store, err := h.redriveStreakStore(ctx)
       if err != nil {
           glog.Warningf("event=redrive_streak_write_failed task=%s error=%v", id, err)
           return
       }
       if store == nil {
           return
       }
       if clearErr := store.Clear(ctx, string(id)); clearErr != nil {
           glog.Warningf("event=redrive_streak_write_failed task=%s error=%v", id, clearErr)
       }
   }
   ```
   Update `cleanupTerminalTask`'s call to `h.clearRedriveStreak(ctx, task.TaskIdentifier)`.

9. Rewrite the `maxConsecutiveRedrives` doc comment. Replace the paragraphs from `// The reset is the phase.` through the end of the comment (immediately before `const maxConsecutiveRedrives = 3`) with prose that:
   - keeps the frontmatter-clobber explanation in substance (the word `clobbered` must remain);
   - removes the sentence `The streak is in-memory, and that is load-bearing` and every occurrence of `deliberately not persisted`;
   - states that the streak is shared through `claims/redrive/<taskid>/<phase>/<n>` in the same git-rest instance as the dispatch claims, so every cluster and every restarted pod reads the same count;
   - states that the in-memory map remains as a floor, that a failed read falls back to it, that a failed write still increments it, and that a failed read never retires a task memory has not capped;
   - states the explicit un-cap rule: a capped task spawns again only after an operator deletes its refs under `claims/redrive/<taskid>/`, or the task moves to another phase and is actually spawned there, or it reaches a terminal state.
   Also change the `redriveStreak` type comment so it no longer says the streak is deliberately not persisted (say the task file is not used, for the clobber reason).
   Do not change the `event=redrive_capped` format, the metric, or the cap value 3.

10. Create `/workspace/pkg/handler/task_event_handler_redrive_test.go` (`package handler_test`, Ginkgo, BSD header) containing a shared in-memory claim-ref transport and the specs below. Add a harness builder that does NOT modify `newClaimHarness`:
    ```go
    func newRedriveHarness(mode pkg.ClaimMode, client *mocks.FakeClaimRefClient) *claimHarness
    ```
    It mirrors `newClaimHarness` but takes the client, uses `libtime.NewCurrentDateTime()`, and its factory increments that harness's `factoryCalls` and returns `client`. Both `spawner.IsJobActiveReturns(false, nil)` and an empty `ListActiveJobs` must be set, and the view must be ready.
    The transport:
    ```go
    type sharedClaimRefTransport struct {
        mu      sync.Mutex
        refs    map[string]claim.ClaimRef // key WITHOUT the "refs/" prefix
        listErr error
        putErr  error
    }
    ```
    `attach(client)` installs `ListCalls`/`PutCalls`/`DeleteCalls` stubs that implement CAS against `refs`: `List` filters by prefix (`key == prefix || strings.HasPrefix(key, prefix+"/")`, or `strings.HasPrefix(key, prefix)` when the prefix ends in `/`) and sorts by `Ref`; `Put` returns `claim.ErrLeaseConflict` when `expected_sha == ""` and the key exists, or when a non-empty `expected_sha` does not match, otherwise stores the ref and returns a fresh sha; `Delete` returns `claim.ErrLeaseConflict` unless the key exists and its sha matches. `List` returns each `claim.ClaimRef` with `Ref = "refs/" + key`. `listErr`/`putErr` are returned before any CAS work. Add `seed(key, sha, cluster, taskID, leaseUntil string)`.
    Specs:
    - AC7 cross-instance: two harnesses A and B share one client and one transport, both `ClaimModeShadow`, one task id. Consume on A, then B, then A; combined `SpawnJobCallCount()` is 3. With `flag.Set("v", "1")` around the 4th attempt on B, `captureStderr` contains the exact line `fmt.Sprintf("event=redrive_capped task=%s streak=3 cap=3 phase=%q", id, "execution")`, and `flag.Set("v", "0")` is restored afterwards; the 5th attempt on A spawns nothing; combined count stays 3.
    - AC7 restart: a fresh harness C over a transport pre-seeded with `claims/redrive/<id>/execution/1..3` makes one consume and `SpawnJobCallCount()` is 0.
    - AC8(a): set `listErr` before three consumes on one shadow harness (memory fallback lets all three spawn). Capture `ListCallCount()`, consume a 4th time; the spawn is refused and `ListCallCount()` is unchanged.
    - AC8(b): `listErr` set, no memory streak; one consume spawns once, `captureStderr` contains `event=redrive_streak_read_failed task=<id>`, and `DeleteCallCount()` is 0.
    - AC8(c): `putErr` set; one consume spawns once and `captureStderr` contains `event=redrive_streak_write_failed task=<id>`; four consumes total spawn exactly 3 times (the 4th is refused by memory).
    - AC8(d): a task whose `TaskIdentifier` contains `/` (e.g. `"bad/id"`); four consumes spawn 3 times, all three transport call counts are 0, and `captureStderr` contains `event=redrive_streak_write_failed task=bad/id`; the 4th is refused by memory.
    - AC9: `newClaimHarness(pkg.ClaimModeOff)`; four consumes produce 3 spawns and `harness.factoryCalls == 0`, with `PutCallCount`, `ListCallCount` and `DeleteCallCount` all 0.
    - AC10 is NOT in this file. Add a plain Go test `TestRedriveStreakStoreSharesLeaseManagerClient(t *testing.T)` to `/workspace/pkg/handler/claim_wiring_internal_test.go` (package `handler`; internal tests cannot import `mocks` — import cycle — and are not Ginkgo). Build `h := &taskEventHandler{claimMode: pkg.ClaimModeShadow, cluster: "self", claimRefClientFactory: func() claim.ClaimRefClient { calls++; return client }, claimJobControl: noopJobControl{}, currentDateTime: libtime.NewCurrentDateTime()}` with `client := &recordingClaimRefClient{}` from `claim_gate_internal_test.go`. Call `h.redriveStreakStore(ctx)` then `h.ClaimLeaseManager(ctx)`, then both again in the opposite order; assert `calls == 1`, both non-nil, the two stores identical and the two managers identical. Then call `store.Count(ctx, "tid-ac10", "execution")` and `manager.Claim(ctx, "claims/task/tid-ac10", "tid-ac10")`, and assert `len(client.listCalls) == 1` and `len(client.putCalls) == 1` — proving both hold the same client instance.

11. In `/workspace/pkg/handler/task_event_handler_claim_test.go`, re-scope ONLY the absolute PUT count in the AC6 "on spawns after taking over an expired lease" spec. Replace `Expect(harness.client.PutCallCount()).To(Equal(2))` with a count of PUT calls whose key starts with `claims/task/` or `claims/repo/` (add a helper in the same file that loops `PutArgsForCall`; import `strings`). Keep the `PutArgsForCall(1)` assertions exactly as they are. Do NOT edit any other spec in that file, and in particular do not touch "AC7(c): a redrive-capped task makes no additional claim request".

12. Add a `## Unreleased` section to `/workspace/CHANGELOG.md` immediately after the preamble and before `## v0.30.6`, with one `fix:` bullet describing this change. The bullet must contain the task id `1788bf3c`, name `claims/redrive/<taskid>/<phase>/<n>` and mention that claim mode `off` is unchanged. Follow `changelog-guide.md` (one logical change, `fix:` prefix, no prompt filenames, no verification narration).

13. Do not add a config knob, env var, opt-out, reaper, new metric, or a change to the cap value, the reset rule or the terminal clear. Do not change dispatch claim keys, lease constants, or fence/takeover behaviour.

14. Before finishing, re-run every `<verification>` command and confirm each passes; walk each acceptance criterion listed above against the change.
</requirements>

<constraints>
- The cap value 3, the phase reset and the terminal clear are unchanged; the cap check stays inside the single spawn funnel, and a spawn is recorded only after the Job create succeeded (spawns, not attempts).
- Do NOT store the streak in task frontmatter — agent pods rewrite frontmatter from a pre-spawn snapshot and would clobber it.
- Do NOT add a config knob, env var or opt-out for the persisted streak; it engages whenever claim mode takes a claim.
- Do NOT add a reaper for streak refs of tasks that never reach a terminal state.
- Do NOT add new metrics. `executor_redrive_capped_total` stays the alerting signal; store failures are warning log lines.
- The `event=redrive_capped` log line format and `executor_redrive_capped_total` name/labels are unchanged.
- Wiring: the streak store receives the SAME claim-ref client instance the lease manager receives, built once inside the existing one-time lease-manager construction; `TestLeaseManagerConstructsOnce` must still hold.
- Streak traffic must never increment any `executor_claim_total{result}` series; the store uses the raw client, not the lease manager.
- All other existing tests in `pkg/claim` and `pkg/handler` pass without weakened assertions.
- Repo conventions: Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, counterfeiter mocks for new interfaces, glog `V(n)` gating, injected clock (no wall-clock reads in tests asserting `lease_until`), license headers, golines 100-char limit.
- New and modified code needs ≥80% statement coverage; every fail-safe branch above must have a spec.
- Do NOT commit — dark-factory handles git.
</constraints>

<verification>
Run `go test ./pkg/claim/... ./pkg/handler/...` — must exit 0.

Confirm the comment rewrite:
`! grep -q 'The streak is in-memory, and that is load-bearing' pkg/handler/task_event_handler.go` — must exit 0
`! grep -q 'deliberately not persisted' pkg/handler/task_event_handler.go` — must exit 0
`! grep -q 'empties the streak' pkg/handler/task_event_handler.go` — must exit 0
`grep -c 'claims/redrive/' pkg/handler/task_event_handler.go` — at least 1
`grep -c 'clobbered' pkg/handler/task_event_handler.go` — at least 1

Confirm the changelog:
`awk '/^## Unreleased/{f=1;next} /^## /{f=0} f' CHANGELOG.md | grep -c 1788bf3c` — at least 1

Run `ROOTDIR=/workspace make precommit` once at the end — must exit 0.
</verification>
