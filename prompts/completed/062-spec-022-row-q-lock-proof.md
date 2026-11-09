---
status: completed
spec: [022-clear-ghost-current-job]
summary: 'Hardened the ghost current_job specs: recorded per-task lock state at publish time (row q2), made row q wait until the pass reached the claim probe, pinned row r call counts, wrapped the JobExists error with job and task context, and confirmed both mutations fail as required.'
execution_id: agent-task-executor-ghost-current-job-exec-062-spec-022-row-q-lock-proof
dark-factory-version: v0.198.17
created: "2026-11-08T20:15:00Z"
queued: "2026-11-08T20:09:49Z"
started: "2026-11-08T20:09:53Z"
completed: "2026-11-08T20:17:04Z"
---

<summary>
- The test meant to prove that the stale-job clear is published while the per-task lock is held does not prove it.
- Publishing just after the lock is released still passes that test today.
- On a slow machine the test can also pass without the reconcile pass ever reaching the lock.
- This change makes the test fail in both cases.
- A new check proves the clear is published while the lock is held, and fails if it is published just after release.
- The namespace test also fails if the cluster is read more than once per pass.
- One returned error from the cluster read now carries the job and task name.
</summary>

<objective>
Rows q and q2 together fail whenever `PublishClearCurrentJob` is called without the per-task claim lock held, and row q fails whenever the pass under test never reaches the lock. The `JobExists` read error carries context, like the `IsJobActive` one. Row r fails if any call other than the ghost step's single call reaches the cluster reads.
</objective>

<context>
Read `CLAUDE.md` if present, and `README.md`.

Read in full:
- `specs/in-progress/022-clear-ghost-current-job.md`: decision-table rows q and r, and Desired Behavior 4.
- `pkg/handler/ghost_current_job.go`: `publishGhostClear` and `ghostCurrentJobGuard`.
- `pkg/handler/ghost_current_job_internal_test.go`: the existing row q spec (around lines 196–234), `ghostRecordingPublisher` and `clearCalls()`.
- `pkg/handler/ghost_current_job_test.go`: the existing row r spec.
- `pkg/handler/task_event_handler.go`: `lockClaimRelease`, `claimLocks` and `claimLocksMu`.

Guides:
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-concurrency-patterns.md`
</context>

<requirements>
1. **Record lock state at publish time.**
   - In `ghostRecordingPublisher.PublishClearCurrentJob` (internal test file), record for each call whether the per-task claim lock for `task.TaskIdentifier` is held at that moment.
   - Read it without blocking: under `h.claimLocksMu`, look up `h.claimLocks[id]`. If the mutex exists, call `TryLock()`. A false result means "held". A true result means "not held", so immediately `Unlock()`. A missing map entry means "not held".
   - The publisher needs a reference to the handler. Add an `h *taskEventHandler` field and set `publisher.h = h` in `newGhostInternalHandler`, right after the `raw.(*taskEventHandler)` assertion. Compute the lock state before taking `p.mu`, and expose it through a copying accessor (for example `lockStates() []bool`) read under `p.mu`, like `clearCalls()`.
   - Comment the lock-state helper: `TryLock` returning false means the lock is held by some goroutine, not necessarily the publisher. The flag is only meaningful in an uncontended spec (row q2); never assert it in row q, where the test itself holds the lock.

2. **Positive row (add as row q2, a separate `It`; do not merge it into the existing "clears when nothing is pending" spec, because the verification count assumes a new spec).** The base ghost record with all guards clear produces exactly one recorded publish, and its recorded lock state is "held".

3. **Row q: prove the pass reached the lock.**
   - The fake claim client's read callback closes a channel `reachedProbe` through a `sync.Once`, so a second `Read` call cannot panic.
   - The test waits on `reachedProbe`, with an `Eventually` bound of 5 s, before starting the `Consistently` window. So the window starts only once the pass has passed the claim probe and is about to take the lock.
   - Keep the rest of row q:
     - the test holds `lockClaimRelease("ghost-task-1")`;
     - `Consistently` asserts no publish;
     - then `markSpawnPending` and release;
     - then 0 publishes and `guard=store_entry`.

4. **Row r: pin call counts.** Add `IsJobActiveCallCount() == 1` and `JobExistsCallCount() == 1` before the `ArgsForCall(0)` assertions.

5. **Wrap the `JobExists` error.** In `ghostCurrentJobGuard`, wrap it as `errors.Wrapf(ctx, err, "check named job %s for task %s", jobName, task.TaskIdentifier)`, matching the wrapped `IsJobActive` error above it. The guard label stays `read_failed`. In row f1, also assert that `out` contains `check named job ghost-job-1 for task ghost-task-1`.

6. **Mutation proofs.** Run each of the two mutations below, observe the stated failure, revert, and state in the completion report that you did so. Git is unavailable in the container. Before mutating, run `cp pkg/handler/ghost_current_job.go /tmp/ghost_current_job.go.bak`; restore with `cp /tmp/ghost_current_job.go.bak pkg/handler/ghost_current_job.go`. Run the verification only after both reverts.
   - (i) change `publishGhostClear` to lock, re-check, unlock, and only then publish. Row q2 must fail.
   - (ii) remove the lock entirely (`unlock := func() {}`). Row q or row q2 must fail.
</requirements>

<constraints>
- Change only `pkg/handler/ghost_current_job.go` (requirement 5 only), `pkg/handler/ghost_current_job_internal_test.go` and `pkg/handler/ghost_current_job_test.go`.
- No production behavior change beyond the error wrap. No new flags, env vars or metrics.
- No `Expect` inside a goroutine without `defer GinkgoRecover()`. Guard every piece of state shared with the goroutine with a mutex. The suite must stay clean under `-race`.
- No CHANGELOG change: this is a test hardening of an unreleased change.
- Do NOT commit.
</constraints>

<verification>
- `ROOTDIR=/workspace make precommit` exits 0.
- Run `go test -race ./pkg/handler/ -v -args -ginkgo.focus='ghost current_job' > /tmp/ghost.out 2>&1; grep -qE 'Ran (3[1-9]|[4-9][0-9]) of' /tmp/ghost.out && grep -q 'SUCCESS!' /tmp/ghost.out && echo GHOST_OK`. It must print `GHOST_OK`: at least 31 specs, all passing, with the race detector on.
</verification>
