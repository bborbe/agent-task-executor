---
status: completed
spec: [022-clear-ghost-current-job]
execution_id: agent-task-executor-ghost-current-job-exec-061-spec-022-ghost-guard-hardening
dark-factory-version: v0.198.17
created: "2026-11-08T17:45:00Z"
queued: "2026-11-08T18:56:57Z"
started: "2026-11-08T19:28:37Z"
completed: "2026-11-08T19:53:03Z"
---

<summary>
- The reconcile step that clears a stale job name already exists; this change closes the gaps a review found.
- The step's active-Job read is narrower than dispatch's. It must use the live, task-id-scoped read that dispatch's own `current_job` gate uses.
- The step must not clear a field whose Job started within the respawn grace period.
- Two safety properties have no test: that the publish happens under the per-task lock, and that the cluster reads receive the right arguments. Removing either still passes the suite today.
- Claim mode `shadow` is untested.
- A code comment claims publish order fixes apply order. It does not, and the comment must say so.
</summary>

<objective>
The ghost `current_job` step reads Job activity exactly as dispatch's `current_job` gate does, skips records inside the respawn grace period, and has tests that fail if the lock or the read arguments are broken.
</objective>

<context>
Read `CLAUDE.md` if present, and `README.md`, for project conventions.

Read these in full before changing anything:
- `specs/in-progress/022-clear-ghost-current-job.md`, the amended contract. In particular: Desired Behavior 3 (step 1b and step 3), Desired Behavior 4, decision-table rows c, f and n–r, and the Failure Modes row "Apply-order inversion".
- `pkg/handler/ghost_current_job.go`, the step to change.
- `pkg/handler/ghost_current_job_test.go` and `pkg/handler/ghost_current_job_internal_test.go`, the existing specs to extend.
- `pkg/handler/job_lookup_denied.go`: `withinRespawnGraceWindow` (around line 128). `defaultRespawnGracePeriod` (300 s, aliasing `claim.SuccessGracePeriod`) is defined in `pkg/handler/task_event_handler.go` (around line 53). Mirror its time arithmetic, but NOT its side effects: no metric increment and no `storeGraceWindowDeferral`.
- `pkg/handler/task_event_handler_current_job.go`: `checkActiveCurrentJob`, whose `IsJobActive(ctx, namespace, task.TaskIdentifier)` read is the one this step must match.
- `pkg/handler/task_event_handler.go`: `lockClaimRelease` and `markSpawnPending`.

Guides:
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-concurrency-patterns.md`
</context>

<requirements>
1. **Active-Job read.** In `ghostCurrentJobGuard`, replace `h.isJobActive(ctx, task, config)` with `h.jobSpawner.IsJobActive(ctx, namespace, task.TaskIdentifier)`, using the namespace already resolved by `jobNamespaceForTask`.
   - An error keeps the field with `guard=read_failed`, wrapped as `errors.Wrapf(ctx, err, "check active job for task %s", task.TaskIdentifier)`. A true result keeps it with `guard=job_active`.
   - Update the doc comments. In `ghostCurrentJobGuard`, "(usually served from the pass cache)" becomes a live, task-id-scoped `IsJobActive` list. In `clearGhostCurrentJobs`, "by isJobActive" becomes `IsJobActive`. Do not describe the read as cached.

2. **Grace window.** Add the label constant `ghostGuardGraceWindow = "grace_window"`.
   - Immediately after the local spawn-state check, and before Config resolution, keep the field with `grace_window` when all of these hold: `task.Frontmatter.JobStartedAt()` parses without error, the value is non-zero, and `h.currentDateTime.Now().Time().Sub(jobStartedAt) < defaultRespawnGracePeriod`.
   - A parse error or a zero value counts as elapsed, matching dispatch.
   - Implement it as a small pure helper in `ghost_current_job.go`. It emits no log line and touches no metric and no deferral store.

3. **Comment correction.** In `publishGhostClear`'s doc comment, replace the claim that "the spawn's claim and spawn notification follow the published clear". The new text says they are *published* after the clear, but commands are keyed by a random `RequestID`, so the controller may apply them in either order. That window is the spec's accepted "Apply-order inversion" residual. Keep the rest of the lock argument.

4. **Existing specs.** Update rows c and f, and any other spec that stubs `ListActiveJobs` to drive this step, to drive `IsJobActive` instead. The harness already defaults to `IsJobActiveReturns(false, nil)`; keep it. Row c1 becomes `IsJobActiveReturns(true, nil)` and row f2 becomes `IsJobActiveReturns(false, <err>)`. Keep every existing `It` and do not merge c1/c2 or f1/f2. The focused run selects 26 specs today, and the 4 new specs bring it to the 30 the verification requires.

5. **New external specs** in `ghost_current_job_test.go`, under the existing `Describe("ghost current_job", …)`:
   - **Row o:** `job_started_at` set to `time.Now().Add(-60*time.Second).UTC().Format(time.RFC3339)`. The harness uses the real `libtime.NewCurrentDateTime()`, so there is no fake clock; follow `currentJobTask` in `task_event_handler_current_job_internal_test.go`. Expect 0 publishes, the kept line `guard=grace_window`, `IsJobActiveCallCount() == 0` and `JobExistsCallCount() == 0`.
   - **Row p:** claim mode `shadow`. Expect 0 publishes, `guard=claim_unprobeable`, and a claim client `ReadCallCount() == 0`.
   - **Row r:** the base record, with the resolver returning a Config whose `JobNamespace` is `"agent-sandbox"`. It must differ from the executor namespace `"dev"`: `ResolveJobNamespace` rejects an equal value, and an empty one falls back to `"dev"`, so the assertion could not catch a wrong namespace. Expect exactly 1 publish. `IsJobActiveArgsForCall(0)` returns `libk8s.Namespace("agent-sandbox")` and `"ghost-task-1"`. `JobExistsArgsForCall(0)` returns `libk8s.Namespace("agent-sandbox")` and `"ghost-job-1"`.

   The base record must carry no `job_started_at`, or one older than the grace period, so the existing rows still reach the cluster reads.

6. **New internal spec** in `ghost_current_job_internal_test.go`, for **row q**:
   - The test takes `h.lockClaimRelease("ghost-task-1")` and runs `h.ReconcileOnce(ctx)` in a goroutine.
   - It asserts with `Consistently` (200 ms) that `PublishClearCurrentJob` is not called.
   - It then calls `h.markSpawnPending("ghost-task-1")` and releases the lock.
   - With `Eventually`, it waits for the goroutine to finish, then asserts 0 publishes and the kept line `guard=store_entry`.
   - Run the whole sequence inside the existing internal log-capture helper's fn, so the kept line is captured. The goroutine sends `ReconcileOnce`'s error on a channel and makes no `Expect` itself (or uses `defer GinkgoRecover()`). If the recording publisher's call slice is read while the goroutine runs, guard it with a `sync.Mutex` and read it through a locked accessor, so `ENABLE_RACE=true` stays clean.

   This spec must fail if `publishGhostClear` does not take the lock. Verify that by temporarily replacing the lock with a no-op, observing the failure, and reverting. Say in the completion report that you did so.

7. **CHANGELOG.** In the existing `## Unreleased` bullet about the ghost `current_job` step, extend the sentence so it also states that the step never clears while the Job is inside the 300 s respawn grace window after `job_started_at`. The phrase "grace window" must appear in that bullet. Keep it grammatical, and add no new bullet and no new section.
</requirements>

<constraints>
- Do NOT change `pkg/handler/task_event_handler_current_job.go`, `pkg/handler/task_event_handler_claim_probe.go`, `pkg/handler/job_lookup_denied.go` or `pkg/result_publisher.go`.
- Do NOT change `ReconcileOnce`, or any file other than `ghost_current_job.go`, the two `ghost_current_job*_test.go` files and `CHANGELOG.md`.
- No new flags, env vars or metrics.
- Keep the guard order from spec Desired Behavior 3: local spawn state, grace window, Config/namespace, `IsJobActive`, `JobExists`, claim probe, then the locked re-check and publish.
- Wrap errors with `github.com/bborbe/errors` (`errors.Wrapf(ctx, …)`), never `fmt.Errorf`.
- Do NOT commit.
</constraints>

<verification>
- `ROOTDIR=/workspace make precommit` exits 0.
- Run `go test ./pkg/handler/ -v -args -ginkgo.focus='ghost current_job' > /tmp/ghost.out 2>&1; grep -qE 'Ran (3[0-9]|[4-9][0-9]) of' /tmp/ghost.out && grep -q 'SUCCESS!' /tmp/ghost.out && echo GHOST_OK`. It must print `GHOST_OK`: at least 30 specs selected, all passing.
- `grep -n 'h.isJobActive' pkg/handler/ghost_current_job.go` prints nothing.
- `grep -n 'grace_window' pkg/handler/ghost_current_job.go` prints at least 1 line.
- `awk '/^## /{sec=$0} /grace window/{print sec; exit}' CHANGELOG.md` prints `## Unreleased`.
</verification>
