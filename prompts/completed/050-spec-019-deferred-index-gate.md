---
status: completed
spec: [019-bug-deferred-respawn-ignores-vault-terminal-status]
summary: Gated the startup seed and deferred evaluation on terminal vault status so a finished task is never re-spawned, with fail-open reads and a per-pass index cache; added self-contained external and internal regression tests.
execution_id: agent-task-executor-divergent-view-exec-050-spec-019-deferred-index-gate
dark-factory-version: v0.198.7
created: "2026-10-30T00:25:18Z"
queued: "2026-10-29T23:48:40Z"
started: "2026-10-29T23:56:55Z"
completed: "2026-10-30T00:05:52Z"
branch: dark-factory/bug-deferred-respawn-ignores-vault-terminal-status
---

# Deferred seed and evaluation index-terminal gates

<summary>
- The startup seed no longer parks a task whose view status is already terminal
- A due deferred entry whose vault-index state is terminal is discarded without spawning
- A task seeded before the vault moved on is caught at evaluation time
- The deferred evaluation performs one index read per pass and none when nothing is due
- A missing index client or a failed read suppresses nothing; the entry spawns as today
- The deferred path stays silent on a read failure, because it runs no divergence check
- Every suppression on this path counts and is reported through the shared per-pass line
- The behaviour is proven by a self-contained suite and an internal seed-gate test that both fail on the pre-change commit

</summary>

<objective>
Close the deferred-respawn duplicate: a task the vault has finished must never be re-spawned by the startup seed or by the deferred evaluation, while an unreadable index keeps today's fail-open behaviour and the deferred loop adds no more than one read per evaluation pass.
</objective>

<context>
Read `pkg/handler/task_event_handler_deferred.go`:
- `seedDeferredRespawnsFromStore` — it skips an empty `current_job` and a terminal **phase**; the terminal-**status** refusal is missing.
- `evalDeferredRespawns` — it collects due entries under `deferredMu`, sorts them, then calls `spawnIfNeededClaimed` for each. This is the path that produced `respawn_after_grace_window` for a vault-`completed` task.
- `clearQueuedAtStampIfNotDeferred` — the correct clear for an entry that ends without a spawn.
- `removeDeferredEntry`, `claimStillValid` — do not change the claim/epoch protocol.

Read `pkg/handler/task_event_handler.go`:
- `terminalStatuses`, `IsTerminalStatus`, `IsTerminal` (`pkg/handler/task_event_handler_phases.go`) — the fixed sets this change reuses.
- `spawnIfNeededClaimed` — the funnel. It applies no status or phase gate; that is why this gate must run in the caller.

This prompt builds on the previous prompt's shared plumbing, which must already be present in the tree:
- `pkg/handler/vault_index_cache.go` — `withVaultIndexCache(ctx)` and `vaultIndexFor(ctx)`.
- `pkg/handler/index_terminal_suppression.go` — `indexTerminalSuppression`, `indexGateSuppresses`, and the `metrics.ReconcileRefusedGateIndexTerminal` constant.
If any of those is absent, stop and report that prompt 1 has not landed; do not duplicate them here.

Read `pkg/handler/task_event_handler_deferred_test.go` — `buildGraceTask`, the `insideGrace` / `graceExpiredR` / `graceExpiredMax` clock constants, the startup-seed spec that runs `RunDeferredRespawnLoop` in a short-lived context, and `EvalDeferredRespawns` driven directly.

Read `pkg/handler/claim_spawn_order_internal_test.go`, `pkg/handler/view_readiness_internal_test.go` and `pkg/handler/task_event_handler_reconcile_refusal_internal_test.go` — the package-`handler` test stubs `stubJobSpawner`, `stubResultPublisher`, `stubResolver`, `notReadyView`, and the parameterised `refusalResolver`. The internal seed-gate file may reuse these because they already exist at `v0.49.1`.

Read `docs/vault-api-task-index.md` § *Divergence check* — the deferred loop runs no divergence check; do not add one.

Read these coding guides from the coding plugin at `/home/node/.claude/plugins/marketplaces/coding/docs/`:
- `go-patterns.md` — `errors.Wrapf`, constructor shape
- `go-glog-guide.md` — V(2) is the deployed level; `event=` prefix
- `go-testing-guide.md` — Ginkgo/Gomega v2 external suite shape
- `go-context-cancellation-in-loops.md` — the existing non-blocking select in the deferred loop must stay
</context>

<requirements>
1. In `seedDeferredRespawnsFromStore`, add a terminal-**status** refusal beside the existing terminal-phase refusal: when `IsTerminalStatus(task.Frontmatter.Status())` is true, `continue` without parking the task. The task must end up absent from `h.deferredRespawns`, not merely never spawned. Keep the existing empty-`current_job` and terminal-phase refusals and their order.
2. In `evalDeferredRespawns`:
   - install a fresh per-evaluation cache with `ctx = withVaultIndexCache(ctx)` at the top of the function, so one evaluation pass costs at most one read and each tick gets a new one;
   - create `suppressions := &indexTerminalSuppression{}` and `defer suppressions.report()`;
   - in the loop over `ready`, **before** `spawnIfNeededClaimed`, call `h.indexGateSuppresses(ctx, entry.task, suppressions)`; when true, call `h.clearQueuedAtStampIfNotDeferred(entry.task.TaskIdentifier)` and `continue` — the entry is discarded, never re-parked and never spawned.
   Do not add a divergence check to this path. Do not change the existing error handling for `triggerRefusalWriteError` / `spawnJobError`, the claim-epoch checks, the admission sort, or the `respawn_after_grace_window` log/metric.
3. Add `pkg/handler/index_terminal_deferred_test.go` in `package handler_test`, a Ginkgo suite that is **self-contained** and must compile against `v0.49.1` when copied alone into a worktree: reference only identifiers that exist at `v0.49.1` (`testTaskVault`, `handler.NewTaskEventHandler`, `handler.WithVaultAPIIndexClient`, `mocks.*`, `claim.TaskIndex` / `claim.TaskIndexItem`, `pkg.AgentConfiguration`, `agentv1.Trigger`, `domain.*`, `libtimetest.ParseDateTime`). It must not reference the new cache/suppression identifiers. Prefix every package-level helper name with `deferredGate` so it cannot collide with the reconcile suite's `reconcileGate*` helpers.
   Define a local counting stub **type** named `deferredGateIndexStub` implementing `claim.VaultAPIIndexClient` (`items`, `err`, `reads`). The type name carries the prefix so it cannot collide with prompt 1's `reconcileGateIndexStub` in the same `handler_test` package.
   The suite covers:
   a. **Three eligible ids through the startup seed (AC 3).** Three tasks in `taskStore`, each `Vault: testTaskVault`, view `status: in_progress`, view `phase: execution`, `assignee: claude`, `stage: prod`, `current_job` set and `job_started_at` older than the respawn grace window; a Config whose `Trigger.Statuses` is `{in_progress, completed, aborted}` and `Trigger.Phases` is `{execution}`; index reports `completed` for the first, `aborted` for the second, `in_progress / execution` for the third. Build a fresh handler over that store with the index client, set the clock past the grace window, run `RunDeferredRespawnLoop` in a goroutine with a cancellable context, `Eventually(func() int { return fakeSpawner.SpawnJobCallCount() }).Should(BeNumerically(">=", 1))`, cancel, wait for the loop goroutine to return, and only then assert `SpawnJobCallCount() == 1` and that the single spawned id is the third. Never put `== 1` inside `Eventually`: on the pre-change commit the same evaluation spawns all three, and a `== 1` predicate cancelled mid-loop would let the pre-change run pass. Collect ids from `fakeSpawner.SpawnJobArgsForCall(i)` and assert `Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1), "spawned ids: %v", ids)`.
   b. **Fail-open, no client (AC 6, deferred half).** No index client; one due entry (park it with `ConsumeMessage` inside the grace window, advance the clock past it, call `EvalDeferredRespawns`) spawns once.
   c. **Fail-open, failed read (AC 8).** Index stub returns an error; the same due-entry setup spawns once, and `testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues("read_error"))` has a delta of 0 across that pass. Assert no `event=view_divergence_read_error` line is emitted by capturing stderr around `EvalDeferredRespawns` (mirror `capturePinReportLog` in `pkg/handler/task_event_handler_pin_outside_pool_test.go`; `captureStderr` in `pkg/handler/task_view_test.go` does not set V(2) but the absence assertion does not need it).
   d. **Read budget, due entries (AC 15c).** One `EvalDeferredRespawns` pass over three due entries performs exactly one read (`reads == 1`). Park the three entries with `ConsumeMessage` inside the grace window, advance the clock past it, then call `EvalDeferredRespawns` once.
   e. **Read budget, nothing due (AC 15d).** One `EvalDeferredRespawns` pass on a fresh handler with no entries performs zero reads (`reads == 0`).
   The three-id seed spec must drive the real seed: the initial `evalDeferredRespawns` inside `RunDeferredRespawnLoop` runs synchronously after the seed, so cancelling after the spawn is observed is safe (see the existing startup-seed spec).
4. Add `pkg/handler/index_terminal_seed_internal_test.go` in `package handler` as a **plain Go test** (`func TestIndexTerminalSeedGate(t *testing.T)`), following the header comment convention of `pkg/handler/view_divergence_metrics_internal_test.go`.
   <!-- NOTE: the spec's AC 4 says "internal Ginkgo spec in package handler". This repo runs exactly one `RunSpecs`, in `package handler_test`; a second `RunSpecs` in the same test binary makes Ginkgo fail, and an internal-package `Describe` is never discovered (stated in the header of pkg/handler/view_divergence_metrics_internal_test.go). Implement AC 4 as a plain Go test in package handler; the behavioural assertions are identical. -->
   The file declares its own vault constant (for example `const indexTerminalSeedVault = "openclaw"`) because `testTaskVault` lives in `package handler_test` and is not visible here.
   The parameterised resolver `refusalResolver` in `pkg/handler/task_event_handler_reconcile_refusal_internal_test.go` (same package, present at `v0.49.1`) returns one Config for every lookup; reuse it to return a Config listing `in_progress`, `completed` and `aborted`, or define a local equivalent in this file.
   Build the real handler with `NewTaskEventHandler` (the stub spawner, publisher and view already exist in this package) and take the concrete type with the idiom the referenced internal tests use: `handler := NewTaskEventHandler(...)`, then `h, ok := handler.(*taskEventHandler)` with `t.Fatalf` when `!ok`. `seedDeferredRespawnsFromStore` and `deferredRespawns` are not on the returned `ClaimLifecycleEventHandler` interface, so calling them on the interface value does not compile. Then:
   - store one task in `taskStore` with `Vault: indexTerminalSeedVault`, view `status: completed`, view `phase: execution` (non-terminal and in `defaultTriggerPhases`), `current_job` set, `job_started_at` older than the grace window, `assignee: claude`, `stage: prod`;
   - install **no** index client;
   - call `h.seedDeferredRespawnsFromStore(context.Background())`, then assert `len(h.deferredRespawns) == 0` and that there is no entry for the id;
   - call `h.EvalDeferredRespawns(ctx)` twice and assert `stubJobSpawner.spawned == 0` after both.
   The map assertion is load-bearing: a status check added inside `spawnIfNeededClaimed` would also yield zero spawns but would leave the task parked and would gate the Kafka path, which the constraints forbid.
5. Before finishing, re-run `<verification>` and confirm it passes; walk each requirement above against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass, including every deferred-respawn spec in `pkg/handler/task_event_handler_deferred_test.go` and `pkg/handler/task_reconcile_test.go`.
- Fail open: a missing index client, a failed read, or a nil index suppresses nothing; the entry spawns exactly as today. The deferred path is silent on a read failure because it runs no divergence check.
- One index read per evaluation pass, and zero when no entry is due. The pass budget is per pass, not global.
- The terminal set is the existing fixed one; no Config, task field or index value may widen it.
- The Kafka event spawn path is unchanged and stays ungated, so a task that genuinely re-opens still starts.
- Do not touch the cross-cluster suppression guard's `claim_probe_failed` path, `MergeFrontmatter`, or how the vault writes/validates task frontmatter.
- Do not repair the stale view. `divergent=` is not this change's success metric.
- No config knob, flag or opt-out for this gate.
- `maxDivergenceLogLines` (20) is reused as the cap; no new constant.
- The index request stays unfiltered and read-only.
- Repo conventions are frozen: Ginkgo/Gomega v2 (external suites), `github.com/bborbe/errors` wrapping, glog, counterfeiter mocks, license headers on new files.
- The two external test files from this and the previous prompt plus the internal seed-gate file are the exact three files copied into a `v0.49.1` worktree for the pre-change proof, so each must be self-contained: no helper in a fourth file.
</constraints>

<verification>
- `ROOTDIR=/workspace make precommit` — must exit 0.
- `go test ./pkg/handler/ -count=1` — must exit 0.
- `go test ./pkg/handler/ -count=1 -run TestIndexTerminalSeedGate` — must exit 0.
</verification>
