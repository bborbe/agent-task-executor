---
status: draft
spec: [012-claim-lease-lifecycle-wiring]
created: "2026-10-21T23:41:52Z"
branch: dark-factory/claim-lease-lifecycle-wiring
---

# Recover claims at startup, run the lease loop, and audit mismatches

<summary>
- A restarted executor recovers the claims it held before any loop starts: it adopts those whose Job still runs and releases those whose Job is gone.
- The lease loop runs as one more service run func in `shadow` and `on`, and is absent in `off`.
- The factory produces the lifecycle run funcs; an empty slice in `off`, one lease-loop entry in `shadow` and `on`.
- The service run list is extracted into an inspectable method so a test can assert the claim funcs are passed to `service.Run`.
- Each completed reconcile pass audits held claims against running Jobs and counts both mismatch directions.
- In `off` no lease manager is constructed and no audit runs.
- The placeholder Job control that failed loud is replaced by a real one backed by the executor's namespace, so recovery, fencing and the audit can actually list and delete Jobs.
- No new metric: the audit increments the existing `executor_claim_mismatch_total` only.
- `pkg/claim` is unchanged.
</summary>

<objective>
Close the lifecycle: recover held claims before the service loops start, start the lease loop in `shadow`/`on`, and run the held-claim / running-Job audit once per reconcile pass, while `off` constructs nothing and starts nothing.
</objective>

<context>
Prompts 1 and 2 of this spec create the release handle and wire it to all five end points. Read them before starting.

Read these files before changing anything:

- `pkg/handler/claim_wiring.go` — created by spec 011 prompt 1: `ClaimRefClientFactory`, `unmanagedJobControl`, and `leaseManager(ctx)`. `unmanagedJobControl` is the placeholder this prompt replaces.
- `pkg/handler/claim_lifecycle.go` — created by prompt 1: `ClaimLifecycleEventHandler`, `ReleaseTaskClaim`, `ClaimLeaseManager`.
- `pkg/handler/task_event_handler.go` — `NewTaskEventHandler`, the `taskEventHandler` struct, and `ReconcileOnce` (the drive loop and the `publishVaultTasks(seen)` call at its end).
- `pkg/handler/claim_wiring_internal_test.go` — the spec-011 internal test block that asserts `unmanagedJobControl` returns "not wired"; remove that block.
- `pkg/claim/lease_manager.go` — `LeaseManager.Recover`, `HeldClaims`, and `NewLeaseManager(cluster, refClient, jobControl, currentDateTime, waiter)`.
- `pkg/claim/audit.go` — `AuditMismatch(ctx, runningTaskIDs map[string]bool)`; it increments `metrics.ClaimMismatchTotal` and logs `claim_mismatch case=... task=...`.
- `pkg/claim/job_control.go` — the `JobControl` interface (`IsJobRunning`, `DeleteJob`).
- `pkg/claim/lease_manager_tick.go` — `Run` (ticks, waits `TickInterval`, never calls `Recover`).
- `pkg/spawner/job_spawner.go` — `IsJobActive` / `ListActiveJobs` terminal rules (conditions first, then the counters) and the label key `agent.benjamin-borbe.de/task-id`.
- `pkg/factory/factory.go` — `CreateConsumer` and `CreateJobWatcher`.
- `main.go` — `application.Run`, the `service.Run` call list, and `createHTTPServer`.
- `main_internal_test.go` — the `package main` specs discovered by `main_test.go`.
- `pkg/handler/task_event_handler_claim_test.go` — the harness from prompt 1 (`newClaimHarness`, `holdClaim`, `snapshotClaims`, `expectClaimDelta`).
- `pkg/claim/lease_manager_test_helpers_test.go` — the fake-clock pattern (`libtime.NewCurrentDateTime()` + `SetNow`) and `claimResults`.
- `docs/claim-lease-protocol.md` — restart recovery and the audit semantics.

Coding docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-factory-pattern.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-prometheus-metrics-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-concurrency-patterns.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md`
</context>

<requirements>
1. In `pkg/handler/claim_lifecycle.go`, add the Job-control interface next to `ClaimLifecycleEventHandler`:

```go
// ClaimJobControl is the Job view the claim lifecycle needs: the two methods
// claim.JobControl requires plus the whole-namespace running set the audit
// compares against.
type ClaimJobControl interface {
	claim.JobControl
	RunningTaskIDs(ctx context.Context) (map[string]bool, error)
}
```

2. Create `pkg/factory/claim_job_control.go` (package `factory`) with a real implementation. Its imports are `context`, `"github.com/bborbe/errors"`, `pkg "github.com/bborbe/agent-task-executor/pkg"`, `"github.com/bborbe/agent-task-executor/pkg/handler"`, `libk8s "github.com/bborbe/k8s"`, `batchv1 "k8s.io/api/batch/v1"`, `apierrors "k8s.io/apimachinery/pkg/api/errors"`, `metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"`, and `"k8s.io/client-go/kubernetes"`.

```go
// claimTaskIDLabelKey must match spawner's task-id label key.
const claimTaskIDLabelKey = "agent.benjamin-borbe.de/task-id"

// kubeClaimJobControl is the claim.JobControl backed by the executor's namespace.
type kubeClaimJobControl struct {
	kubeClient kubernetes.Interface
	namespace  libk8s.Namespace
}

// CreateClaimJobControl returns the Job control the lease manager uses for
// restart recovery, fencing and the mismatch audit.
func CreateClaimJobControl(
	kubeClient kubernetes.Interface,
	namespace libk8s.Namespace,
) handler.ClaimJobControl {
	return &kubeClaimJobControl{kubeClient: kubeClient, namespace: namespace}
}
```

   Method behaviour:
   - `IsJobRunning(ctx, taskID)` lists Jobs in the namespace with `LabelSelector: claimTaskIDLabelKey + "=" + taskID` and returns `true` for the first Job that is not terminal. A terminal Job is one where `pkg.IsJobFailed(job) || pkg.IsJobSucceeded(job)`, or `job.Status.Succeeded > 0`, or (`job.Status.Failed > 0 && job.Status.Active == 0`). This mirrors `spawner.IsJobActive` exactly. A list error is wrapped with `errors.Wrapf(ctx, err, "list jobs for task %s", taskID)`.
   - `DeleteJob(ctx, taskID)` lists the same selector and deletes each Job with `metav1.DeleteOptions{PropagationPolicy: &metav1.DeletePropagationBackground}`. A `NotFound` error is ignored (`apierrors.IsNotFound`); any other error is wrapped. Deleting no Jobs is not an error.
   - `RunningTaskIDs(ctx)` lists Jobs with the existence selector `LabelSelector: claimTaskIDLabelKey`, skips terminal Jobs, skips Jobs whose task-id label is empty, and returns `map[string]bool` of the rest. A list error is wrapped with `errors.Wrapf(ctx, err, "list running jobs")`.

3. In `pkg/handler/task_event_handler.go`, append `claimJobControl ClaimJobControl` as the last parameter of `NewTaskEventHandler` and store it on `taskEventHandler`. In `pkg/handler/claim_wiring.go`:
   - Replace `unmanagedJobControl{}` in the `claim.NewLeaseManager(...)` call with `h.claimJobControl`.
   - Delete the `unmanagedJobControl` type and its two methods.
   - In `leaseManager(ctx)`, before constructing the manager, add a fail-loud guard:

```go
if h.claimJobControl == nil {
	return nil, errors.Errorf(ctx, "claim job control not wired for mode %s", h.claimMode)
}
```

4. In `pkg/factory/factory.go`:
   - In `CreateConsumer`, build `claimJobControl := CreateClaimJobControl(kubeClient, namespace)` and pass it as the final argument of `handler.NewTaskEventHandler`.
   - Do not change `CreateConsumer`'s parameters: `kubeClient` and `namespace` are already available.

5. Add the lifecycle run-func constructor to `pkg/factory/factory.go`:

```go
// CreateClaimLifecycleRunFuncs returns the claim lease loop as a service run
// func. In claim mode off it returns an empty slice and never starts a loop.
func CreateClaimLifecycleRunFuncs(
	claimMode pkg.ClaimMode,
	leaseLoop run.Func,
) []run.Func {
	if claimMode == pkg.ClaimModeOff || leaseLoop == nil {
		return []run.Func{}
	}
	return []run.Func{leaseLoop}
}
```

   `factory` already imports `github.com/bborbe/run` and `pkg`.

6. In `pkg/handler/claim_lifecycle.go`, add the audit input helper:

```go
// claimAuditInputs returns the lease manager and the running Job task ids for
// one audit. ok is false in claim mode off, when the manager cannot be built,
// or when the running set cannot be listed; each failure is logged and the
// audit is skipped for this pass.
func (h *taskEventHandler) claimAuditInputs(
	ctx context.Context,
) (claim.LeaseManager, map[string]bool, bool) {
	if h.claimMode == pkg.ClaimModeOff {
		return nil, nil, false
	}
	manager, err := h.leaseManager(ctx)
	if err != nil {
		glog.Warningf("event=claim_audit_skipped reason=lease_manager err=%v", err)
		return nil, nil, false
	}
	running, err := h.claimJobControl.RunningTaskIDs(ctx)
	if err != nil {
		glog.Warningf("event=claim_audit_skipped reason=list_running err=%v", err)
		return nil, nil, false
	}
	return manager, running, true
}
```

7. In `ReconcileOnce` (`pkg/handler/task_event_handler.go`), after the drive loop and immediately BEFORE `h.publishVaultTasks(seen)`, add the audit. There must be exactly one `AuditMismatch` call site and it must be textually inside `ReconcileOnce`:

```go
// Claim audit: once per completed pass, not once per candidate. In claim mode
// off claimAuditInputs reports false and no audit runs.
if manager, running, ok := h.claimAuditInputs(passCtx); ok {
	manager.AuditMismatch(passCtx, running)
}
```

   An aborted pass returns before this point, so an aborted pass performs no audit. Do not move the audit into the candidate loop.

8. In `main.go`, after `factory.CreateConsumer` and before `service.Run`:
   - Obtain the manager: `claimManager, err := taskEventHandler.ClaimLeaseManager(ctx)`; on error `return errors.Wrapf(ctx, err, "create claim lease manager")`.
   - Call `Recover` exactly once, before the run list starts. A recovery failure must not prevent the executor from starting (the failure-mode row says the executor starts with no held claims); log it:

```go
if claimManager != nil {
	if recoverErr := claimManager.Recover(ctx); recoverErr != nil {
		glog.Warningf("event=claim_recover_failed err=%v", recoverErr)
	}
}
```

   - Build the loop func:

```go
var claimLoop run.Func
if claimManager != nil {
	claimLoop = claimManager.Run
}
claimRunFuncs := factory.CreateClaimLifecycleRunFuncs(claimMode, claimLoop)
```

   - Extract the `service.Run` argument list into a method:

```go
// runFuncs appends the claim lifecycle run funcs to the base service run list.
// In claim mode off the factory returns an empty slice, so the list is exactly
// the base list.
func (a *application) runFuncs(base []run.Func, claimRunFuncs []run.Func) []run.Func {
	return append(base, claimRunFuncs...)
}
```

   - Call `return service.Run(ctx, a.runFuncs([]run.Func{ /* the existing eight entries, unchanged */ }, claimRunFuncs)...)`. Do not change the eight base entries.

9. Update every `handler.NewTaskEventHandler(` call site for the new final parameter. Several of these files contain more than one call site; update them all:
   - `pkg/factory/factory.go`: pass the constructed `claimJobControl`.
   - `pkg/handler/task_event_handler_test.go`, `pkg/handler/task_event_handler_deferred_test.go`, `pkg/handler/task_event_handler_respawn_race_test.go`, `pkg/handler/task_reconcile_test.go`: append `nil`.
   - `pkg/handler/task_event_handler_claim_test.go`: the harness passes a `*fakeClaimJobControl` (see requirement 10).

10. Extend the harness in `pkg/handler/task_event_handler_claim_test.go` (package `handler_test`):
    - Add `jobControl *fakeClaimJobControl` to `claimHarness`, construct it in `newClaimHarness`, and pass it to `handler.NewTaskEventHandler` as the final argument.
    - Define the fake in this file:

```go
// fakeClaimJobControl is the test double for handler.ClaimJobControl.
type fakeClaimJobControl struct {
	mu      sync.Mutex
	running map[string]bool
	deleted []string
}
```

    with `SetRunning(map[string]bool)`, `IsJobRunning` (answers from the set), `DeleteJob` (records the task id), `RunningTaskIDs` (returns a copy of the set), and `DeleteCallCount() int`.

11. Create `pkg/factory/factory_suite_test.go` (package `factory_test`) with a standard Ginkgo `RunSpecs` suite, then create `pkg/factory/claim_lifecycle_test.go` (package `factory_test`) with:
    - mode `off` with a non-nil loop func → `len(CreateClaimLifecycleRunFuncs(pkg.ClaimModeOff, fn)) == 0`
    - mode `shadow` → `len == 1` and calling the entry invokes `fn`
    - mode `on` → `len == 1`

12. Create `pkg/factory/claim_job_control_test.go` (package `factory_test`) using `k8s.io/client-go/kubernetes/fake`:
    - a running Job (no terminal condition, `Status.Active > 0`) for task `t1` → `IsJobRunning` true and `RunningTaskIDs` contains `t1`
    - a succeeded Job (JobComplete condition) for task `t2` → `IsJobRunning` false and `RunningTaskIDs` does not contain `t2`
    - `DeleteJob` removes the Jobs for the task; a second `DeleteJob` on the same task returns nil
    - a Job with no task-id label is ignored by `RunningTaskIDs`

13. Create `pkg/handler/task_event_handler_claim_lifecycle_test.go` (package `handler_test`, Ginkgo + Gomega):

    a. **AC3 restart recovery.** Mode `shadow`. `harness.client.ListReturns` two `claim.ClaimRef` values with `Cluster: "self"` and `LeaseUntil` five minutes in the future (`time.Now().UTC().Add(5*time.Minute).Format(time.RFC3339)`): one `{Ref: "refs/claims/task/running", Sha: "sha-running", TaskID: "running"}` and one `{Ref: "refs/claims/task/gone", Sha: "sha-gone", TaskID: "gone"}`. `harness.jobControl.SetRunning(map[string]bool{"running": true})`. Get the manager with `harness.handler.ClaimLeaseManager(ctx)` and call `Recover(ctx)`. Assert: `expectClaimDelta(before, after, map[string]float64{"adopted": 1, "released": 1})`; `DeleteCallCount() == 1`; `DeleteArgsForCall(0)` has key `"claims/task/gone"` and sha `"sha-gone"`; `HeldClaims()` contains `running` and not `gone`.

    b. **AC4 both mismatch directions in one pass.** Mode `shadow`. Set `harness.gitRestClient.IsReadyReturns(true, nil)` and `harness.gitRestClient.ListReturns([]string{}, nil)`. `harness.jobControl.SetRunning(map[string]bool{"running": true})`. `harness.holdClaim("held")`. Snapshot `testutil.ToFloat64(metrics.ClaimMismatchTotal)`, run `harness.handler.ReconcileOnce(ctx)` inside the log-capture helper below, and assert the delta is exactly `2`. Assert the captured output contains `claim_mismatch case=job_without_claim task=running` and `claim_mismatch case=claim_without_job task=held`.

    c. **AC4 matching sets add nothing.** Same pass fixture with `harness.jobControl.SetRunning(map[string]bool{"held": true})` and `harness.holdClaim("held")`; assert the delta is `0`.

    d. **AC4 no audit in `off`.** Mode `off`; same pass fixture with a running task and no claims; assert the `ClaimMismatchTotal` delta is `0`.

    Use this helper for the log assertions. It captures glog's stderr output; restore the flag and `os.Stderr` afterwards:

```go
func captureGlog(fn func()) string {
	Expect(flag.Set("logtostderr", "true")).To(Succeed())
	defer func() { _ = flag.Set("logtostderr", "false") }()
	old := os.Stderr
	r, w, err := os.Pipe()
	Expect(err).NotTo(HaveOccurred())
	os.Stderr = w
	defer func() { os.Stderr = old }()
	fn()
	glog.Flush()
	Expect(w.Close()).To(Succeed())
	out, err := io.ReadAll(r)
	Expect(err).NotTo(HaveOccurred())
	return string(out)
}
```

14. Add to `main_internal_test.go` (package `main`, no new `RunSpecs`):

```go
var _ = Describe("application.runFuncs", func() {
	It("appends the claim lifecycle run funcs to the base list", func() {
		base := []run.Func{func(context.Context) error { return nil }}
		claimRan := false
		claimFunc := run.Func(func(context.Context) error {
			claimRan = true
			return nil
		})
		a := &application{}
		list := a.runFuncs(base, []run.Func{claimFunc})
		Expect(list).To(HaveLen(2))
		for _, fn := range list {
			Expect(fn(context.Background())).To(Succeed())
		}
		Expect(claimRan).To(BeTrue())
		Expect(a.runFuncs(base, nil)).To(HaveLen(1))
	})
})
```

    Add the `github.com/bborbe/run` import to that file.

15. Remove the `unmanagedJobControl` specs from `pkg/handler/claim_wiring_internal_test.go` (the block asserting each method returns an error containing `not wired`). Keep the other specs in that file.

16. Add a `CHANGELOG.md` entry under `## Unreleased`:
    `- feat: recover held claims at startup, run the claim lease loop, and audit claim/Job mismatches per reconcile pass`

17. Before finishing, walk every acceptance criterion above against the change and re-run `<verification>`.
</requirements>

<constraints>
- `pkg/claim` is called, never modified. `Release`, `Recover`, `Run`, `Tick` and `AuditMismatch` are used as they are.
- `Recover` is called once, before the service run list starts, and never from a loop.
- The lifecycle run funcs are produced by a `pkg/factory` constructor returning `[]run.Func`; `main.go` appends them through `a.runFuncs`.
- The audit runs inside `ReconcileOnce`, once per completed pass, not once per candidate. In `off` no audit runs.
- In `off` no lease manager is constructed, so `ClaimLeaseManager` returns nil and `CreateClaimLifecycleRunFuncs` returns an empty slice.
- No new metric, no new label value, no new alert. The audit increments the existing `executor_claim_mismatch_total` only. Do not modify `pkg/metrics/metrics.go`.
- No `CLAIM_MODE` parsing, no claim gate at dispatch, and no takeover — spec 011 owns those. No `spec.rerunOnLostClaim` wiring.
- Do not add a retry loop around `Recover`, `Run` or `AuditMismatch`.
- Do not change the eight existing `service.Run` entries.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
```
ROOTDIR=/workspace make precommit
go test -race -count=1 ./...
grep -rn 'Recover\|AuditMismatch' main.go pkg/ | grep -v _test.go
grep -rn 'AuditMismatch(' pkg/handler/ | grep -v _test.go
git diff --stat pkg/metrics/
grep -rn 'unmanagedJobControl' pkg/ main.go
```
- `make precommit` exits 0.
- `go test -race -count=1 ./...` exits 0.
- The `Recover\|AuditMismatch` grep shows exactly one `claimManager.Recover(` call in `main.go`, before `service.Run`, and exactly one `AuditMismatch(` call in `pkg/handler/task_event_handler.go` inside `ReconcileOnce`. The `pkg/claim` definition sites also match the broad grep; that is expected.
- `git diff --stat pkg/metrics/` prints nothing (no new metric registration).
- `grep -rn 'unmanagedJobControl' pkg/ main.go` prints nothing.
- `grep -c 'AuditMismatch(' pkg/handler/task_event_handler.go` prints `1`.
- `grep -n 'a.runFuncs(' main.go` shows the `service.Run` call uses the method.

Completion report (appended by dark-factory): set `"status":"success"` only when `make precommit` exits 0; otherwise `"status":"failed"`. Include the actual `make precommit` exit code in the `verification` block.
</verification>
