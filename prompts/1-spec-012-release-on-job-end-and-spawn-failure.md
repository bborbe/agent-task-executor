---
status: draft
spec: [012-claim-lease-lifecycle-wiring]
created: "2026-10-21T23:41:52Z"
branch: dark-factory/claim-lease-lifecycle-wiring
---

# Release a claim when its Job ends or its spawn fails

<summary>
- A claim taken before a Job is created is now released when that Job reaches a terminal state, whether it succeeded or failed.
- The release is keyed by task id against the claims this executor actually holds, so an end event for one task never releases another task's claim.
- The release on a succeeded Job runs even when the task store no longer has the task, which is the case after an executor restart.
- A claim whose Job was never created is released as soon as the spawn that motivated it fails.
- A failed release is logged and left held: the lease loop's existing retry is the only retry, and this wiring adds none.
- Releasing a claim this executor does not hold is a no-op and changes no counter.
- In `off` nothing changes: no lease manager is constructed and every new call returns immediately.
- The Job watcher receives the release handle through a small interface so `pkg` keeps no dependency on the handler package.
- No new metric, no new label, no change to `pkg/claim`.
</summary>

<objective>
Establish the single claim-release handle and wire it to the two Job-watcher terminal branches and to the failed-spawn branch, so every claim that motivated a Job is released when that Job ends or is never created, while `off` stays byte-for-byte unchanged.
</objective>

<context>
Spec 011 (`specs/in-progress/011-claim-mode-dispatch-wiring.md`) lands first. Read `prompts/completed/021-spec-011-claim-mode-config.md` and the code it produced (`pkg/handler/claim_wiring.go`: `leaseManager`, `acquireSpawnClaim`, `claimFailureDecision`) to see the exact seam this prompt builds on: `pkg.ClaimMode`, `ClaimRefClientFactory`, the `taskEventHandler` claim fields, and the test harness `newClaimHarness` in `pkg/handler/task_event_handler_claim_test.go`.

Read these files before changing anything:

- `pkg/job_watcher.go` — `JobWatcher`, `NewJobWatcher`, `jobWatcher`, `HandleJob`. The two terminal branches are the `IsJobFailed` branch and the `IsJobSucceeded` branch; `handleSucceeded` and `handleTerminal` are NOT hook points.
- `pkg/handler/task_event_handler.go` — `NewTaskEventHandler`, the `taskEventHandler` struct, `spawnIfNeededClaimed`, and the `SpawnJob` error branch inside it.
- `pkg/handler/claim_wiring.go` — created by spec 011 prompt 1: `ClaimRefClientFactory`, `unmanagedJobControl`, `leaseManager`.
- `pkg/claim/lease_manager.go` — `LeaseManager.Release` (sets `releasePending` on a failed delete and counts `error`; a key the manager does not hold returns nil with no counter change), `LeaseManager.HeldClaims` (`HeldClaim{Key, TaskID, Sha, LeaseUntil}`; `TaskID` is a `string`).
- `pkg/claim/lease_manager_tick.go` — `retryPendingReleases`; this is the retry the wiring must not duplicate.
- `pkg/factory/factory.go` — `CreateJobWatcher`, `CreateConsumer`.
- `main.go` — `application.Run`, the `taskStore`/`jobWatcher`/`zombieSweeper` block, and the `factory.CreateConsumer` call.
- `mocks/claim_ref_client.go` — `DeleteCallCount`, `DeleteArgsForCall(i) (context.Context, string, string)`, `DeleteReturnsOnCall(i, error)`, `PutStub` / `PutReturns`.
- `pkg/claim/lease_manager_test_helpers_test.go` — the counter-snapshot pattern (`snapshotClaims`, `expectClaimDelta`) that the handler harness mirrors.
- `docs/claim-lease-protocol.md` — the `released` / pending-release semantics.

Coding docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-factory-pattern.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md`
</context>

<requirements>
1. Create `pkg/claim_releaser.go` (package `pkg`) with exactly this contract:

```go
// ClaimReleaser releases this executor's held claim for a task, if any. The
// task event handler implements it and the Job watcher is injected with it, so
// a terminal Job can release the claim taken before the Job was created.
type ClaimReleaser interface {
	ReleaseTaskClaim(ctx context.Context, taskID lib.TaskIdentifier)
}
```

   Imports: `context` and `lib "github.com/bborbe/agent"`. Do not add methods. Do not put this interface in `pkg/handler` — `pkg` must not import the handler package.

2. In `pkg/job_watcher.go`:
   - Add `claimReleaser ClaimReleaser` to the `jobWatcher` struct.
   - Append `claimReleaser ClaimReleaser` as the last parameter of `NewJobWatcher` and assign it.
   - Add this helper next to `HandleJob`:

```go
// releaseClaim releases this executor's held claim for taskID. A nil releaser
// (claim mode off) is a no-op. ReleaseTaskClaim logs its own failures; the
// lease loop's retryPendingReleases retries a failed delete on its next tick.
func (w *jobWatcher) releaseClaim(ctx context.Context, taskID lib.TaskIdentifier) {
	if w.claimReleaser == nil {
		return
	}
	w.claimReleaser.ReleaseTaskClaim(ctx, taskID)
}
```

   - In the `IsJobFailed(job)` branch, call `w.releaseClaim(ctx, taskID)` after `reason := w.failureReason(job)` and before `w.handleTerminal(ctx, taskID, job, reason, true)`. Do not place it inside `handleTerminal` or `logMissingTask`.
   - In the `IsJobSucceeded(job)` branch, call `w.releaseClaim(ctx, taskID)` immediately after the `if job.Status.CompletionTime != nil { w.taskStore.MarkSucceeded(...) }` block and BEFORE `if task, ok := w.taskStore.Load(taskID); ok { w.handleSucceeded(...) }`. This position is load-bearing: the release must run even when the task entry was already evicted or was never stored after a restart. Do not place it inside `handleSucceeded`.

3. Create `pkg/handler/claim_lifecycle.go` (package `handler`) with the lifecycle seam and the release handle. Its imports are `context`, `lib "github.com/bborbe/agent"`, `github.com/golang/glog`, `github.com/bborbe/agent-task-executor/pkg/claim`, and `pkg "github.com/bborbe/agent-task-executor/pkg"`:

```go
// ClaimLifecycleEventHandler is the TaskEventHandler plus the claim lifecycle
// seam main and the Job watcher use. Every claim method is a no-op in claim
// mode off.
type ClaimLifecycleEventHandler interface {
	TaskEventHandler
	pkg.ClaimReleaser
	// ClaimLeaseManager returns this executor's lease manager, or nil in claim
	// mode off, where no manager is constructed.
	ClaimLeaseManager(ctx context.Context) (claim.LeaseManager, error)
}
```

```go
// ReleaseTaskClaim releases every claim this executor holds for taskID. It is
// the single release handle every end point calls. A claim the manager no
// longer holds is a no-op, so two end points firing for one claim release once.
// A failed delete is logged and left to the lease loop's retryPendingReleases;
// this function never retries and never returns an error.
func (h *taskEventHandler) ReleaseTaskClaim(ctx context.Context, taskID lib.TaskIdentifier) {
	if h.claimMode == pkg.ClaimModeOff {
		return
	}
	manager, err := h.leaseManager(ctx)
	if err != nil {
		glog.Warningf(
			"event=claim_release_failed task=%s stage=lease_manager err=%v",
			taskID,
			err,
		)
		return
	}
	for _, held := range manager.HeldClaims() {
		if held.TaskID != string(taskID) {
			continue
		}
		if releaseErr := manager.Release(ctx, held.Key); releaseErr != nil {
			glog.Warningf(
				"event=claim_release_failed task=%s key=%s err=%v",
				taskID,
				held.Key,
				releaseErr,
			)
		}
	}
}
```

```go
// ClaimLeaseManager returns this executor's lease manager, or nil in claim mode
// off, where no manager is constructed.
func (h *taskEventHandler) ClaimLeaseManager(
	ctx context.Context,
) (claim.LeaseManager, error) {
	if h.claimMode == pkg.ClaimModeOff {
		return nil, nil
	}
	return h.leaseManager(ctx)
}
```

   Behavioural requirements on `ReleaseTaskClaim`:
   - `off` returns before touching the factory or the manager.
   - It iterates `HeldClaims()` and releases only entries whose `TaskID` equals the given task id. It never releases a claim for another task.
   - A `Release` error is logged at `glog.Warningf` and the loop continues to the next matching entry. It never retries and never loops over a failed key.
   - It increments no counter itself; `pkg/claim` owns every `executor_claim_total` increment.
   - A task id with no held claim sends no `Delete` and changes no counter.

4. Change the return type of `NewTaskEventHandler` from `TaskEventHandler` to `ClaimLifecycleEventHandler`. Its parameter list is unchanged in this prompt. Change the second return value of `pkg/factory.CreateConsumer` from `handler.TaskEventHandler` to `handler.ClaimLifecycleEventHandler`. Do not change `CreateConsumer`'s parameters.

5. In `spawnIfNeededClaimed` (`pkg/handler/task_event_handler.go`), in the `h.jobSpawner.SpawnJob` error branch, call `h.ReleaseTaskClaim(ctx, task.TaskIdentifier)` after the existing `metrics.TaskEventsTotal.WithLabelValues("error").Inc()` and before the `return false, errors.Wrapf(...)`. A successful claim whose Job was never created must not stay held. In `shadow` with a conflicting claim this is a no-op because the manager holds nothing for the task.

6. In `pkg/factory/factory.go`, append `claimReleaser pkg.ClaimReleaser` as the last parameter of `CreateJobWatcher` and pass it to `pkg.NewJobWatcher`. `pkg/factory` already imports `pkg`.

7. In `main.go`:
   - Move the `jobWatcher := factory.CreateJobWatcher(...)` and `zombieSweeper := factory.CreateZombieSweeper(...)` block from its current position to AFTER the `consumer, taskEventHandler := factory.CreateConsumer(...)` block. The handler must exist before the watcher is built; `currentDateTimeGetter`, `resultPublisher`, `taskStore`, and `eventHandlerConfig` are all still in scope there.
   - Pass `taskEventHandler` as the final argument of `factory.CreateJobWatcher`: `factory.CreateJobWatcher(kubeClient, a.Namespace, taskStore, resultPublisher, taskEventHandler)`.
   - Do not change any other startup validation or ordering.

8. Update every other `NewJobWatcher` call site to pass `nil` as the new final argument. The call sites are:
   - `pkg/job_watcher_internal_test.go`
   - `pkg/job_watcher_test.go`
   - `pkg/handler/task_event_handler_respawn_race_test.go`
   - `pkg/envtest/job_watcher_envtest_test.go`

9. Extend the spec-011 harness in `pkg/handler/task_event_handler_claim_test.go` (package `handler_test`). Add the imports this file now needs: `batchv1 "k8s.io/api/batch/v1"`, `corev1 "k8s.io/api/core/v1"`, `metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"`, `"k8s.io/client-go/kubernetes/fake"`, `"github.com/bborbe/agent-task-executor/pkg/claim"`, and `"github.com/bborbe/agent-task-executor/pkg"`:
   - `claimHarness` already carries `eventHandler handler.TaskEventHandler` (`pkg/handler/task_event_handler_claim_test.go:31`, assigned at `:51`, used by `consume` at `:80`). Rename that field to `handler` and retype it `handler.ClaimLifecycleEventHandler`, updating both usages. Also add `taskStore *pkg.TaskStore`, `publisher *mocks.FakeResultPublisher` and `gitRestClient *mocks.FakeGitRestClient` by extracting the values currently constructed inline in the `NewTaskEventHandler(...)` call, and populate them in `newClaimHarness`. `spawner` and `client` already exist — do not add them again.
   - Add this helper:

```go
// holdClaim takes a claim for taskID directly through the handler's lease
// manager, without going through the spawn path, and returns the held view.
func (h *claimHarness) holdClaim(taskID string) claim.HeldClaim {
	ctx := context.Background()
	manager, err := h.handler.ClaimLeaseManager(ctx)
	Expect(err).NotTo(HaveOccurred())
	held, err := manager.Claim(ctx, "claims/task/"+taskID, taskID)
	Expect(err).NotTo(HaveOccurred())
	return *held
}
```

   - Add this helper so the tests can drive a terminal Job through the real watcher:

```go
// watcher returns a JobWatcher that releases through this harness's handler.
func (h *claimHarness) watcher() pkg.JobWatcher {
	return pkg.NewJobWatcher(fake.NewClientset(), "test-ns", h.taskStore, h.publisher, h.handler)
}
```

   - Add a helper that builds a terminal Job for a task id:

```go
// terminalJob returns a Job carrying taskID and the given terminal condition.
// It MUST set Labels["agent.benjamin-borbe.de/task-id"] = taskID: HandleJob
// returns early without that label (pkg/job_watcher.go:142), so a helper that
// omits it makes every spec below pass vacuously.
func terminalJob(name, taskID string, condition batchv1.JobCondition) *batchv1.Job
```

   For a succeeded Job, set `Status.CompletionTime` to a non-nil `metav1.Time`. Also add these two condition builders in the same file so the specs below compile:

```go
func succeededCondition() batchv1.JobCondition {
	return batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}
}

func failedCondition(message string) batchv1.JobCondition {
	return batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: message}
}
```

10. Create `pkg/handler/task_event_handler_claim_release_test.go` (package `handler_test`, Ginkgo + Gomega) with these specs. Use `snapshotClaims`/`expectClaimDelta` from the harness for every counter assertion.

    a. **AC1(a) succeeded Job releases only its claim.** Mode `shadow`. Set `harness.client.PutStub` to return `"sha-"+taskID` for every call. `holdClaim("task-a")` and `holdClaim("task-b")`. Call `harness.watcher().HandleJob(ctx, terminalJob("job-a", "task-a", succeededCondition))` with a non-nil `CompletionTime`. Assert: `DeleteCallCount() == 1`; `DeleteArgsForCall(0)` has key `"claims/task/task-a"` and sha `"sha-task-a"`; `expectClaimDelta(before, after, map[string]float64{"released": 1})`; `HeldClaims()` contains `task-b` and not `task-a`.

    b. **AC1(a) succeeded hook runs with no task-store entry.** Mode `shadow`. `holdClaim("task-a")` and do NOT store a task. Call `HandleJob` with a succeeded Job for `task-a`. Assert one `Delete` with `sha-task-a` and `released` +1. This spec fails if the hook is placed inside the `w.taskStore.Load` guard.

    c. **AC1(b) failed Job releases only its claim.** Same fixture with `terminalJob("job-a", "task-a", failedCondition("OOMKilled"))`. Assert one `Delete` with `sha-task-a`, `released` +1, and `task-b` still held.

    d. **AC1(e) failed spawn releases the claim it just took.** Mode `on`. `harness.spawner.SpawnJobReturns("", errors.New("job create failed"))`. Consume a qualifying task with `harness.consume(task)`. Assert: exactly one `Put` happened (the claim was taken); `DeleteCallCount() == 1`; `DeleteArgsForCall(0)` sha equals the sha the fake `Put` returned; `released` +1. Expect `ConsumeMessage` to return an error and assert only that it occurred.

    e. **AC2 a failed release is retried by the next tick, not by the wiring.** Mode `shadow`. `holdClaim("task-a")`. `harness.client.DeleteReturnsOnCall(0, errors.New("git-rest throttled"))`. Call `HandleJob` with a succeeded Job. Assert: `DeleteCallCount() == 1`; `released` delta 0 and `error` delta 1; `HeldClaims()` still contains `task-a`. Then call `manager.Tick(ctx)` on `harness.handler.ClaimLeaseManager(ctx)`. Assert: `DeleteCallCount() == 2`; `released` delta 1; `HeldClaims()` no longer contains `task-a`. Do not advance the clock: the claim's lease is 300 s out and the renew interval is 60 s, so the tick performs no fence and no renew.

    f. **AC2 releasing an unheld key sends no Delete and changes no counter.** Mode `on`. Do not hold any claim. Call `harness.handler.ReleaseTaskClaim(ctx, "unknown-task")`. Assert `DeleteCallCount() == 0` and every one of the 13 `executor_claim_total` series has delta 0.

11. Add a `CHANGELOG.md` entry under `## Unreleased`:
    `- feat: release the dispatch claim when its Job ends or its spawn fails`

12. Before finishing, walk AC1(a), AC1(b), AC1(e) and AC2 against the change and re-run `<verification>`. AC1(c)/(d) belong to prompt 2 and AC3/AC4 to prompt 3 — do not implement them here.
</requirements>

<constraints>
- `pkg/claim` is called, never modified. `Release` already sets `releasePending` on a failed delete and `retryPendingReleases` retries it on the next tick; the wiring must not duplicate that retry.
- `Release` for a key the manager does not hold returns nil without a counter change — a double release is a no-op, not an error, and must not be logged as one.
- The Job-watcher hooks live in `HandleJob` at its two terminal branches, NOT inside `handleSucceeded` or `handleTerminal`. The succeeded hook must sit outside `HandleJob`'s `if task, ok := w.taskStore.Load(taskID); ok` guard.
- The claim is resolved by task id from `HeldClaims()`. The hook needs no vault read and no `Config`.
- `Recover` is not called in this prompt. The lease `Run` loop is not started in this prompt. The mismatch audit is not added in this prompt. `spec.rerunOnLostClaim` is not wired.
- No `CLAIM_MODE` parsing, no claim gate at dispatch, and no takeover — spec 011 owns those.
- No new metric, no new label value, no new alert. `executor_claim_total` is incremented only inside `pkg/claim`.
- No release on a timer independent of an end point: a claim whose Job is still running must stay held.
- In `off` no lease manager is constructed and every new call returns immediately.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
```
ROOTDIR=/workspace make precommit
go test -race -count=1 ./...
grep -rn 'ReleaseTaskClaim' pkg/ main.go
grep -rn 'NewJobWatcher(' --include='*.go' .
grep -rn 'claimReleaser' pkg/job_watcher.go
```
- `make precommit` exits 0.
- `go test -race -count=1 ./...` exits 0.
- The `ReleaseTaskClaim` grep shows the interface declaration in `pkg/claim_releaser.go`, the implementation in `pkg/handler/claim_lifecycle.go`, the Job-watcher call site, and the failed-spawn call site.
- The `NewJobWatcher(` grep shows the definition and every call site; no call site still passes the old argument count.
- `! grep -q 'ClaimTotal.WithLabelValues' pkg/handler/claim_lifecycle.go pkg/job_watcher.go` exits 0 (the wiring increments nothing).
- `grep -c 'w.releaseClaim(ctx, taskID)' pkg/job_watcher.go` prints `2` (one per terminal branch).
- `grep -c 'for _, held := range manager.HeldClaims()' pkg/handler/claim_lifecycle.go` prints `1`, and `! grep -nE 'retry' pkg/handler/claim_lifecycle.go` exits 0 — the only loop around `Release` is the `HeldClaims` match loop and there is no retry loop.

Completion report (appended by dark-factory): set `"status":"success"` only when `make precommit` exits 0; otherwise `"status":"failed"`. Include the actual `make precommit` exit code in the `verification` block.
</verification>
