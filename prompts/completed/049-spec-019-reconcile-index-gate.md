---
status: completed
spec: [019-bug-deferred-respawn-ignores-vault-terminal-status]
summary: Added a gather-stage index-terminal gate with a per-pass shared vault-index cache, suppressed-id metrics/logging, and a self-contained reconcile test suite; precommit exits 0
execution_id: agent-task-executor-divergent-view-exec-049-spec-019-reconcile-index-gate
dark-factory-version: v0.198.7
created: "2026-10-30T00:25:18Z"
queued: "2026-10-29T23:48:40Z"
started: "2026-10-29T23:48:41Z"
completed: "2026-10-29T23:56:54Z"
branch: dark-factory/bug-deferred-respawn-ignores-vault-terminal-status
---

# Reconcile index-terminal gate

<summary>
- A task the vault index reports as finished can no longer become a reconcile candidate
- The reconcile pass reuses the index read it already performs, so a pass still costs one read
- The gate sits beside the existing status and phase gates, after the eligibility filter admits the task
- A task whose index entry is merely different from the view is still driven — only terminality suppresses
- A missing index client or a failed read suppresses nothing, exactly as before
- The advisory divergence check keeps reporting the same lines and the same counter units
- A pass that never becomes ready makes no index call at all
- The new behaviour is proven by a self-contained test suite that also fails on the pre-change commit

</summary>

<objective>
Stop the reconcile candidate gather from admitting a task whose vault-index state is terminal, so a task the vault has already finished can never be re-driven — while keeping the existing one-index-read-per-pass budget and the advisory divergence report intact.
</objective>

<context>
Read `pkg/handler/task_event_handler.go`:
- `terminalStatuses`, `IsTerminalStatus` and `applyPhaseGate` — the fixed terminal sets this change reuses. Do not widen or move them.
- `ReconcileOnce` — the pass. Note the three pass-scoped caches already installed on `passCtx` (`withJobCache`, `withRedriveStreakCache`, `withSuccessCache`) before the readiness check, and the `checkViewDivergence` call after the gather.
- `gatherCandidates` / `gatherCandidate` — the filter phase; `gatherCandidate` calls `reconcileTask`, then branches to `restoreParkedAssignee` for an empty assignee, then builds the candidate.
- `reconcileTask` — the eight existing gates. The new gate is **not** one of them: it runs in the gather, after `reconcileTask` returns `ok == true`.
- `task_event_handler_reconcile_refusal.go` — `refuseReconcile` logs only when a task's gate changes, so the new per-pass suppression line must NOT go through it.

Read `pkg/handler/view_divergence.go`:
- `maxDivergenceLogLines` (20) — the cap the new line reuses; do not add a second constant.
- `checkViewDivergence` — the advisory report. Its event names, counter units (`ok` per pass, `mismatch`/`absent` per id) and read-error handling must not change.
- `compareViewToIndex` — the duplicate-id rule: a duplicate id keeps the last item.

Read `pkg/handler/job_cache.go` — the pass-scoped context-cache pattern (`withJobCache`, `jobCacheFrom`) the new index cache follows. The cache must live on the pass context, never on the handler.

Read `pkg/claim/vault_api_index_client.go` — `VaultAPIIndexClient`, `TaskIndex`, `TaskIndexItem` (fields `ID`, `Status`, `Phase`). The client is read-only; do not add a filter, a write, or a second request.

Read `pkg/metrics/metrics.go` — the `ReconcileRefusedTotal` const block and `init()`.

Read `pkg/handler/task_event_handler_test.go` (the harness: `testTaskVault`, `NewTaskEventHandler` call shape), `pkg/handler/view_divergence_test.go` (`newIndexServer`, `indexBody`, `ownedTask`, `captureStderr`), and `pkg/handler/task_reconcile_test.go` (`eligibleTask`, the reconcile harness).

Read `docs/vault-api-task-index.md` § *Divergence check* — the behaviour this change makes no longer purely advisory (the doc is updated by a later prompt; do not edit it here).

Read these coding guides from the coding plugin at `/home/node/.claude/plugins/marketplaces/coding/docs/`:
- `go-patterns.md` — interface → constructor → struct, `errors.Wrapf`
- `go-glog-guide.md` — V-level convention, `event=` prefix
- `go-testing-guide.md` — Ginkgo/Gomega v2 external suite shape
- `go-context-cancellation-in-loops.md` — the non-blocking select pattern
</context>

<requirements>
1. Add `pkg/handler/vault_index_cache.go` with:
   - an unexported context key type `vaultIndexCacheKey struct{}`;
   - `type passVaultIndexCache struct { once sync.Once; index *claim.TaskIndex; err error; terminal map[string]struct{} }`;
   - `func withVaultIndexCache(ctx context.Context) context.Context` returning a context carrying a fresh cache;
   - `func vaultIndexCacheFrom(ctx context.Context) *passVaultIndexCache` returning nil outside a cache scope;
   - `func (h *taskEventHandler) vaultIndexFor(ctx context.Context) (*claim.TaskIndex, map[string]struct{}, error)`:
     - when `h.vaultIndexClient == nil`, return `(nil, nil, nil)` and make **no** call;
     - with a cache in `ctx`, perform `h.vaultIndexClient.Read(ctx)` exactly once under `once.Do`, store the index and error, and build the terminal-id set only when `err == nil && index != nil`;
     - with no cache in `ctx`, read directly (this keeps existing direct-call unit tests working);
   - `func indexTerminalIDs(index *claim.TaskIndex) map[string]struct{}`: an id is terminal when `IsTerminalStatus(domain.TaskStatus(item.Status))` **or** `IsTerminal(domain.TaskPhase(item.Phase))`; iterate `index.Items` in order so a duplicate id keeps the **last** item's verdict (delete the id on a non-terminal last item), matching `compareViewToIndex`.
   Imports: `context`, `sync`, `"github.com/bborbe/vault-cli/pkg/domain"`, `"github.com/bborbe/agent-task-executor/pkg/claim"`.
2. Add `pkg/handler/index_terminal_suppression.go` with:
   - `type indexTerminalSuppression struct { ids []string }`;
   - `func (s *indexTerminalSuppression) add(id string)` appending the id **and** incrementing `metrics.ReconcileRefusedTotal.WithLabelValues(metrics.ReconcileRefusedGateIndexTerminal)` — the counter is uncapped, one increment per suppressed id;
   - `func (s *indexTerminalSuppression) report()` sorting `ids` ascending and emitting at most `maxDivergenceLogLines` lines at `glog.V(2)` with the exact format `event=index_terminal_suppressed task=%s` — one line per id, never one line listing many;
   - `func (h *taskEventHandler) indexGateSuppresses(ctx context.Context, task lib.Task, suppressions *indexTerminalSuppression) bool` which calls `vaultIndexFor`, returns false on error or when the id is absent from the terminal set (fail open), and otherwise calls `suppressions.add(string(task.TaskIdentifier))` and returns true.
   Imports: `context`, `sort`, `lib "github.com/bborbe/agent"`, `"github.com/golang/glog"`, `"github.com/bborbe/agent-task-executor/pkg/metrics"`.
3. In `pkg/metrics/metrics.go`, add the exported constant `ReconcileRefusedGateIndexTerminal = "index_terminal"` at the end of the existing gate const block, with a doc comment saying it is the gather-stage gate that refuses a task whose vault-index state is terminal, that it is applied on the reconcile gather and on the deferred-respawn path, and that it is not one of the eight existing gates `reconcileTask` applies. Do **not** add it to `init()` here and do **not** rewrite the existing "eight gates" / "all eight series" comments — a later prompt owns the registration and the comment correction.
4. In `ReconcileOnce`, install the cache on the pass context next to the existing pass caches: `passCtx = withVaultIndexCache(passCtx)`. Place it with `withJobCache` / `withRedriveStreakCache` / `withSuccessCache`, before the readiness check. Do not read the index eagerly.
5. In `gatherCandidates`, create `suppressions := &indexTerminalSuppression{}`, `defer suppressions.report()`, and pass it to `gatherCandidate`. In `gatherCandidate`, immediately after `reconcileTask` returns `ok == true` (and **before** the `task.Frontmatter.Assignee() == ""` restore branch), call `h.indexGateSuppresses(ctx, task, suppressions)`; when true return `reconcileCandidate{}, reconcileOutcomeSkipped`. Do not route this refusal through `refuseReconcile` and do not touch the `refusedGates` map.
   Keep the growth of `task_event_handler.go` minimal: it is 1944 lines against revive's `file-length-limit: 2000` (`.golangci.yml`). Put every explanatory comment and helper body in the two new files; this file gains only the cache line, the collector pair, the signature parameters and the three-line gate call.
6. In `checkViewDivergence`, replace the direct `h.vaultIndexClient.Read(ctx)` call with `index, _, err := h.vaultIndexFor(ctx)`. Keep the nil-client early return, keep `err != nil || index == nil` mapping to `ViewDivergenceTotal{result="read_error"}` plus the `event=view_divergence_read_error` line, and keep the `ok` / `mismatch` / `absent` increments and the summary line byte-identical. Add a short comment that the read is shared with the candidate gate through the pass cache.
7. Add `pkg/handler/index_terminal_reconcile_test.go` in `package handler_test` — a Ginkgo suite that is **self-contained**: it must compile and run against `v0.49.1` after being copied alone into a `v0.49.1` worktree, so it may reference only identifiers that already exist at `v0.49.1` (`testTaskVault`, `captureStderr`, `handler.NewTaskEventHandler`, `handler.WithVaultAPIIndexClient`, `mocks.*`, `claim.TaskIndex` / `claim.TaskIndexItem`, `pkg.AgentConfiguration`, `agentv1.Trigger`, `domain.TaskStatus*` / `domain.TaskPhase*`). It must not reference `ReconcileRefusedGateIndexTerminal`, the new cache helpers or any other new identifier. Prefix every package-level helper name with `reconcileGate` so it cannot collide with `ownedTask`, `indexItem`, `buildHandler`, `newDivergenceHandler` or the sibling suite from prompt 2.
   Define a local counting stub **type** named `reconcileGateIndexStub` implementing `claim.VaultAPIIndexClient` with `items []claim.TaskIndexItem`, `err error` and `reads int`. The type name carries the prefix so it cannot collide with prompt 2's stub in the same `handler_test` package.
   The suite covers:
   a. **Three eligible ids (AC 2).** Three view tasks, each `Vault: testTaskVault`, view `status: in_progress`, view `phase: execution`, `assignee: claude`, `stage: prod`, no `current_job`; a Config whose `Trigger.Statuses` is `{in_progress, completed, aborted}` and `Trigger.Phases` is `{execution}`; index reports `completed` for the first, `aborted` for the second and `in_progress / execution` for the third. `ReconcileOnce` spawns exactly one Job and it is the third. Collect the spawned ids from `fakeSpawner.SpawnJobArgsForCall(i)` and assert `Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1), "spawned ids: %v", ids)`.
   b. **Divergence still reports (AC 9).** One id whose view is `in_progress / execution` and index is `in_progress / ai_review` (non-terminal, divergent). Capture stderr with `captureStderr`; assert `strings.Count(out, "event=view_divergence task=") == 1` and a `testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("mismatch"))` delta of 1.
   c. **No over-suppression (AC 10).** View `in_progress / ai_review`, index `in_progress / execution`; the task still spawns once.
   d. **Fail-open, no client (AC 6, reconcile half).** No `WithVaultAPIIndexClient` option; an otherwise eligible task spawns once.
   e. **Fail-open, failed read (AC 7).** Index stub returns an error; the task spawns once and `ViewDivergenceTotal{result="read_error"}` increases by 1.
   f. **Read budget, shared (AC 15a).** One id the gate suppresses (`view in_progress / execution`, index `completed / human_review`) in a pass where the divergence check also runs; assert the stub's `reads == 1`.
   g. **Read budget, not ready (AC 15b).** `fakeView.ReadyReturns(false)` with an index stub installed; `ReconcileOnce`; assert `reads == 0`.
   Use the existing reconcile harness shape (`mocks.FakeJobSpawner` with `IsJobActiveReturns(false, nil)` and `SpawnJobReturns("job-1", nil)`, `mocks.FakeConfigResolver`, `mocks.FakeResultPublisher`, `mocks.FakeTaskView` with `ReadyReturns(true)`).
8. Before finishing, re-run `<verification>` and confirm it passes; walk each requirement above against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass, including `pkg/handler/view_divergence_test.go` and `pkg/handler/view_divergence_metrics_internal_test.go`; the shared read must not change any divergence event name, count or unit.
- The gate can only suppress, never spawn. A corrupt or hostile index cannot start work.
- Fail open: a missing index client, a failed read, or a nil index suppresses nothing and drives the pass exactly as today.
- One index read per pass, per path. The reconcile pass shares one read between the candidate gate and the divergence check. No consumer adds a second read within one pass.
- The terminal set is the existing fixed one (`terminalStatuses`, `terminalPhases`). No Config, task field or index value may widen it.
- The Kafka event spawn path is unchanged and stays ungated.
- `maxDivergenceLogLines` (20) is reused as the cap; no new constant.
- The index request stays unfiltered and read-only: no `cluster=` filter, no vault write, the existing client and auth header pair, the existing 32 MiB body bound.
- Do not touch the cross-cluster suppression guard's `claim_probe_failed` path, `MergeFrontmatter`, or how the vault writes/validates task frontmatter.
- Do not repair the stale view. `divergent=` is not this change's success metric.
- No config knob, flag or opt-out for this gate.
- Repo conventions are frozen: Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, glog, counterfeiter mocks, license headers on new files.
- New test files must be self-contained: no helper in a fourth file, because the pre-change proof copies exactly the new suite files into a `v0.49.1` worktree.
</constraints>

<verification>
- `ROOTDIR=/workspace make precommit` — must exit 0.
- `go test ./pkg/handler/ -count=1` — must exit 0.
- `go test ./pkg/metrics/ -count=1` — must exit 0 (the existing eight-gate registration spec must stay green: this prompt adds the constant but does not pre-initialise it).
</verification>
