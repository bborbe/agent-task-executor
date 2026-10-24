---
status: draft
spec: [016-bug-claim-only-until-quota]
created: "2026-10-24T19:34:00Z"
branch: dark-factory/bug-claim-only-until-quota
---

<summary>
- Admission for a task is now bounded by the smaller of the declared concurrency cap and the pod quota that scopes the agent's priority class.
- A cluster that cannot start the work never takes the claim for it, so a peer cluster can take the task immediately.
- The declared cap still binds when it is the smaller of the two, and an unlimited declared cap is still bounded by the quota.
- A quota read failure falls back to the declared cap, logs exactly one warning, and never blocks a spawn on its own.
- Both the admit and the deferral log lines now name the running count, the effective cap, the declared maximum and the quota, so the binding bound is readable from one line.
- Deferral behaviour is unchanged: same queue, same retry delay, same `queued_at` stamp, same two existing counters.
- No new metric, no change to the claim lease constants, keys, takeover, or admission ordering.
- The quota reader attaches through the existing handler-option seam, so the handler constructor and the generated JobSpawner fake are untouched.
- The production wiring in the factory attaches the real reader, so the bound is live once the release ships (subject to the separate RBAC grant).
</summary>

<objective>
Make `deferIfAtConcurrencyCap` admit only what the cluster can actually start: the effective capacity is the smaller positive value of `MaxConcurrentJobs` and the pod quota of the agent's priority class, read through the `PodQuotaLimiter` added in prompt 1. Because admission is already decided before the claim is taken, a deferred task holds no lease and the peer pool can take it.
</objective>

<context>
Read `CLAUDE.md` for project conventions.

Read these files before changing anything:
- `pkg/handler/task_event_handler.go` — `spawnIfNeededClaimed` (the per-assignee spawn lock is taken before `deferIfAtConcurrencyCap`, so the lock already spans count, quota read and spawn), `deferIfAtConcurrencyCap`, and the `taskEventHandler` struct.
- `pkg/handler/task_event_handler_options.go` — the `HandlerOption` seam (`WithClaimJobControl`, `WithVaultAliases`). The quota limiter attaches here.
- `pkg/handler/job_namespace.go` — `jobNamespaceForTask`, already used by `deferIfAtConcurrencyCap`.
- `pkg/handler/job_cache.go` — `activeJobsFor` and the per-assignee count the cap compares against.
- `pkg/handler/task_event_handler_test.go` — the package-level Ginkgo suite, `activeJobs(n)`, `testTaskVault`, and the existing `MaxConcurrentJobs` specs ("defers instead of spawning when the assignee is at MaxConcurrentJobs", "spawns when the assignee is below MaxConcurrentJobs", "spawns without applying a cap when MaxConcurrentJobs is unset").
- `pkg/handler/task_event_handler_claim_test.go` — `newClaimHarness`, `claimHarness.consume`, `dispatchPutCount`, `qualifyingClaimTask`, `claimBoolPtr`. Reuse this harness so "the claim was never taken" is asserted against a real claim-mode handler rather than a claims-off one.
- `pkg/handler/task_event_handler_capped_internal_test.go` — the plain-Go internal test style used in this package (specs declared in an internal test package are not discovered by the external Ginkgo suite).
- `pkg/handler/task_event_handler_pin_outside_pool_test.go` — `capturePinReportLog`, the glog capture helper. Reuse it as-is (it is generic despite its name); it sets `v=2`, which is above the `V(1)` decision lines.
- `pkg/spawner/pod_quota_limiter.go` — the `PodQuotaLimiter` interface added by prompt 1; `mocks/pod_quota_limiter.go` — its generated fake (`PodQuotaReturns`, `PodQuotaCallCount`).
- `pkg/factory/factory.go` — `CreateConsumer`, the only production construction site of `NewTaskEventHandler` (verified: no other non-test call site exists).

Verified facts:
- `pkg.AgentConfiguration.PriorityClassName` is the field carrying the agent's priority class.
- `pkg.AgentConfiguration.MaxConcurrentJobs` is `0` for "no declared cap".
- `handler.NewTaskEventHandler` keeps its current positional signature and variadic `opts ...HandlerOption`; do not change it.
</context>

<requirements>
1. Add a field to `taskEventHandler` in `pkg/handler/task_event_handler.go`:

```go
// podQuotaLimiter is the optional pod-capacity reader. Nil means no quota bound,
// so every existing test and every deployment without the limiter keeps today's
// behaviour. It is read only under the per-assignee spawn lock.
podQuotaLimiter spawner.PodQuotaLimiter
```

2. Add the option to `pkg/handler/task_event_handler_options.go` (import `"github.com/bborbe/agent-task-executor/pkg/spawner"`):

```go
// WithPodQuotaLimiter attaches the reader that bounds admission by the pod quota
// of the agent's priority class. An absent limiter means no quota bound.
func WithPodQuotaLimiter(limiter spawner.PodQuotaLimiter) HandlerOption {
	return func(h *taskEventHandler) {
		h.podQuotaLimiter = limiter
	}
}
```

3. Create `pkg/handler/concurrency_capacity.go` with two unexported helpers, both with doc comments explaining the rule:

```go
// effectiveConcurrencyCap returns the admission bound: the smaller positive
// value of the declared cap and the pod quota. Zero means uncapped. A zero
// declared cap does NOT disable the quota bound; a zero quota does not disable
// the declared cap.
func effectiveConcurrencyCap(maxConcurrentJobs int, podQuota int) int

// podQuotaFor returns the pod quota that bounds admission for config, or 0 when
// no limiter is attached, the Config names no priority class, or the read fails.
// A failed read logs exactly one warning and must never block the spawn on its
// own.
func (h *taskEventHandler) podQuotaFor(
	ctx context.Context,
	namespace libk8s.Namespace,
	config *pkg.AgentConfiguration,
) int
```

   `podQuotaFor` calls `h.podQuotaLimiter.PodQuota(ctx, namespace, config.PriorityClassName)` only when the limiter is non-nil and `config.PriorityClassName != ""`; on error it logs exactly one warning and returns 0:

```
event=pod_quota_read_failed namespace=%s assignee=%s priority_class=%s err=%v
```

   A negative returned limit is treated as 0. Do not add any other log line on this path.

4. Rewrite `deferIfAtConcurrencyCap` in `pkg/handler/task_event_handler.go` to use the effective capacity, preserving the existing order and side effects:
   - Return `(false, nil)` when `config == nil`.
   - Return `(false, nil)` when `h.podQuotaLimiter == nil && config.MaxConcurrentJobs <= 0` — this preserves today's early return exactly for the uncapped, limiter-less case (the existing spec "spawns without applying a cap when MaxConcurrentJobs is unset" asserts exactly one `ListActiveJobs` call, so no list may happen on this path).
   - Resolve `namespace` with `h.jobNamespaceForTask(ctx, config, task.TaskIdentifier)`; on error keep the existing `metrics.TaskEventsTotal.WithLabelValues("error").Inc()` and `errors.Wrapf` behaviour.
   - `quota := h.podQuotaFor(ctx, namespace, config)`; `cap := effectiveConcurrencyCap(config.MaxConcurrentJobs, quota)`; return `(false, nil)` when `cap <= 0`.
   - `activeSet, err := h.activeJobsFor(ctx, namespace, config.Assignee)`; keep the existing error handling; `running := len(activeSet)`.
   - Admit when `running < cap`, logging at `V(1)` with exactly these fields in this order:

```
event=concurrency_admit task=%s assignee=%s running=%d cap=%d max=%d quota=%d
```

   - Otherwise keep every existing deferral side effect unchanged (claim-invalidation pre-check, `metrics.TaskEventsTotal.WithLabelValues("deferred_concurrency_cap").Inc()`, `metrics.DeferredConcurrencyCapTotal.WithLabelValues(config.Assignee).Inc()`, `stampQueueEntry`, the `deferredRespawns` store and its claim/epoch guards) and log at `V(1)` with exactly these fields in this order:

```
event=concurrency_cap task=%s assignee=%s running=%d cap=%d max=%d quota=%d retry_after=%s
```

   `max` is always `config.MaxConcurrentJobs` (the declared value, including 0) and `quota` is always the raw value `podQuotaFor` returned (including 0). Do not reorder the deferral side effects.

5. Update the doc comment on `deferIfAtConcurrencyCap` and the paragraph in `lockAssigneeSpawn`'s comment that says over-cap Jobs are "rejected by the agent's ResourceQuota": state that admission is now bounded by the smaller of the declared cap and the quota, and that the quota is read inside this same per-assignee lock. Do not change the lock itself.

6. Wire the production reader in `pkg/factory/factory.go` inside `CreateConsumer`, after the existing `handler.WithVaultAliases(vaultAliases)` option:

```go
handler.WithPodQuotaLimiter(
	spawner.NewPodQuotaLimiter(kubeClient, currentDateTimeGetter),
),
```

   This is the only non-test construction site; do not change `CreateConsumer`'s signature.

7. Permit the new dependency in the claim test harness: change `newClaimHarness(mode pkg.ClaimMode) *claimHarness` to `newClaimHarness(mode pkg.ClaimMode, opts ...handler.HandlerOption)` and append `opts...` after `handler.WithClaimJobControl(noopJobControl{})` in the `NewTaskEventHandler` call. Every existing call site keeps compiling unchanged. This is the only permitted edit to an existing test file; do not relax any existing assertion.

8. Add `pkg/handler/concurrency_capacity_internal_test.go` (`package handler`, plain Go table test, no Ginkgo) covering `effectiveConcurrencyCap`: `(0,0)→0`, `(0,3)→3`, `(5,0)→5`, `(5,8)→5`, `(5,3)→3`, `(-1,3)→3`, `(5,-1)→5`.

9. Add `pkg/handler/task_event_handler_quota_admission_test.go` (`package handler_test`, Ginkgo). Build each fixture with `newClaimHarness(pkg.ClaimModeOn, handler.WithPodQuotaLimiter(limiter))` where `limiter := new(mocks.FakePodQuotaLimiter)`; override the resolver with `pkg.AgentConfiguration{Assignee: "claude", Image: "img", MaxConcurrentJobs: <max>, PriorityClassName: "github-pr-review-agent", RerunOnLostClaim: claimBoolPtr(true)}`; set `h.client.PutReturns("sha-1", nil)` and `h.client.ListReturns(nil, nil)` so a wrongly-admitted task really would take a claim. Drive every fixture through `h.consume(qualifyingClaimTask())` and set the active set with `h.spawner.ListActiveJobsReturns(activeJobs(n), nil)`. Cover:
   - AC1: `max=5`, quota `3`, active `3` — `consume` returns nil; `SpawnJobCallCount()==0`; `dispatchPutCount(h)==0`; `TaskEventsTotal{result="deferred_concurrency_cap"}` rises by 1; `DeferredConcurrencyCapTotal{assignee="claude"}` rises by 1.
   - AC2: quota `8`, `max=5` — active `5` defers (no spawn, no dispatch PUT); active `4` spawns (`SpawnJobCallCount()==1`, `dispatchPutCount(h)==1`).
   - AC3: limiter returns `(0, nil)`, `max=5` — active `4` spawns; active `5` defers.
   - AC4: `max=0`, quota `3` — active `3` defers; active `2` spawns.
   - AC5: limiter returns `(0, stderrors.New("forbidden"))`, `max=5`, active `4` — the spawn proceeds and the captured output contains exactly one `event=pod_quota_read_failed` line (`strings.Count`). Wrap AC2's admit fixture (successful read, `(8, nil)`) in `capturePinReportLog` too and assert `strings.Count(out, "event=pod_quota_read_failed") == 0`.
   - AC8: wrap the AC1 fixture in `capturePinReportLog` and assert the output matches `event=concurrency_cap .*running=3 cap=3 max=5 quota=3`. Also assert AC2's admit fixture matches `event=concurrency_admit .*running=4 cap=5 max=5 quota=8`.
   Metric assertions are deltas via `testutil.ToFloat64`, never absolute values (the counters are package-global).
10. In the completion report, quote verbatim (a) the `event=concurrency_cap ... running=3 cap=3 max=5 quota=3` line captured by the AC8 spec and (b) the `event=pod_quota_read_failed` warning captured by the AC5 spec.
11. Before finishing, re-run `<verification>` and walk each acceptance criterion of this prompt against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- `NewTaskEventHandler` keeps its signature. The quota reader attaches only through `HandlerOption`; do not add a positional parameter and do not add a method to `JobSpawner`.
- The generated counterfeiter fake for `JobSpawner` (`mocks/job_spawner.go`) is untouched. Do not edit any generated file by hand.
- Do NOT add a Prometheus metric. Reuse `TaskEventsTotal{result="deferred_concurrency_cap"}` and `DeferredConcurrencyCapTotal{assignee}`. `ClaimTotal` labels stay a closed set; keys, task ids and priority class names are never label values.
- Do NOT change lease constants (`LeaseDuration`, `RenewInterval`, `FenceMargin`, `TakeoverGrace`, `JoblessGrace`, `JoblessCheckInterval`), claim keys, takeover, per-PR keying, `ResourceQuota` objects or manifests, any live `maxConcurrentJobs` value, the deferred-respawn loop, `queued_at`, admission ordering, offset replay, the reconcile pass budget, or the Kafka view.
- Do NOT add a release-on-refused-spawn path. The claim is not taken because admission already said no; the existing claim-acquisition site is unchanged.
- Existing tests keep their assertions unchanged. The only permitted edits to existing test files are the variadic `opts` on `newClaimHarness` and any stub that must gain a zero-returning method; no existing expectation may be relaxed.
- The per-assignee spawn lock must still span the active count, the quota read and the spawn. Do not move the quota read outside it.
- Error wrapping uses `github.com/bborbe/errors`; never `fmt.Errorf`; never `context.Background()` inside `pkg/`.
- `task_event_handler.go` is already near 2,000 lines. Put the new helpers in `pkg/handler/concurrency_capacity.go` and the new specs in new test files; do not grow the existing file beyond the `deferIfAtConcurrencyCap` edit and the struct field.
</constraints>

<verification>
```
ROOTDIR=/workspace make test
ROOTDIR=/workspace make precommit
go test -mod=mod ./pkg/handler/ -run TestHandler -count=1
```
All three must exit 0.
</verification>
