---
status: completed
spec: [019-bug-deferred-respawn-ignores-vault-terminal-status]
summary: Registered the index_terminal refusal gate at zero, corrected its documentation, and added an internal test proving the 20-line log cap with an uncapped counter.
execution_id: agent-task-executor-divergent-view-exec-051-spec-019-observability
dark-factory-version: v0.198.7
created: "2026-10-30T00:25:18Z"
queued: "2026-10-29T23:48:40Z"
started: "2026-10-30T00:05:53Z"
completed: "2026-10-30T00:11:50Z"
branch: dark-factory/bug-deferred-respawn-ignores-vault-terminal-status
---

# Index-terminal observability: registration, cap, sort, counter

<summary>
- The new refusal gate is registered at zero before any refusal, like every other gate
- The metrics registration spec now proves nine gates, not eight
- One pass suppressing many ids emits at most twenty sorted lines, one per id
- The counter behind that line is never capped: every suppressed id counts
- A second identical pass emits the same twenty lines again
- The stale "eight gates" wording is gone and the help text names both paths
- The cap reuses the existing twenty-name budget, with no new constant

</summary>

<objective>
Make the index-terminal refusal observable and honest: register `executor_reconcile_refused_total{gate="index_terminal"}` at zero before any refusal, correct the gate documentation, and prove the per-pass log cap/sort and the uncapped counter with an internal test.
</objective>

<context>
This prompt builds on the previous two, which must already be in the tree:
- `pkg/handler/index_terminal_suppression.go` — `indexTerminalSuppression.add` increments `metrics.ReconcileRefusedTotal{gate="index_terminal"}` per id; `report` sorts ascending and emits at most `maxDivergenceLogLines` V(2) `event=index_terminal_suppressed task=<id>` lines, one per id.
- `pkg/handler/vault_index_cache.go` and the gather/deferred gates that call it.
If any of those is absent, stop and report that prompts 1 and 2 have not landed; do not re-implement them here.

Read `pkg/metrics/metrics.go`:
- the gate const block (`ReconcileRefusedGateForeignCluster` … `ReconcileRefusedGateNoRouting`, plus `ReconcileRefusedGateIndexTerminal` added by prompt 1);
- `ReconcileRefusedTotal` and its doc comment;
- `init()` — the loop that pre-initialises every gate series at zero.

Read `pkg/metrics/metrics_test.go` — the spec `"pre-initialises exactly the eight reconcile-refusal gates at zero"` (`gatherLabels(mfs, "executor_reconcile_refused_total", "gate")` plus the per-gate zero loop). This is the non-vacuous pre-initialisation proof: `gatherLabels` reads the default gatherer without creating a series, so a `testutil.ToFloat64(....WithLabelValues(...)) == 0` assertion does not substitute for it.

Read `pkg/handler/task_event_handler_reconcile_refusal_internal_test.go` — `captureRefusalLog` (V(2) + `logtostderr`, `os.Stderr` pipe, `glog.Flush`), and the plain-Go internal-test convention.

Read `pkg/handler/view_readiness_internal_test.go` — `notReadyView`, `stubResolver`, and the `NewTaskEventHandler` call shape; the new internal test needs a **ready** view stub, which it defines itself.

Read `pkg/handler/task_reconcile_test.go` — the reconcile harness shape and `eligibleTask`.

Read `docs/vault-api-task-index.md` § *Metric contract* — the closed-label, pre-initialised-counter rule (a later prompt documents the new gate there; do not edit docs here).

Read these coding guides from the coding plugin at `/home/node/.claude/plugins/marketplaces/coding/docs/`:
- `go-prometheus-metrics-guide.md` — closed label domains and pre-initialisation
- `go-glog-guide.md` — V(2) is the deployed level
- `go-testing-guide.md` — test shape
</context>

<requirements>
1. In `pkg/metrics/metrics.go`:
   - add `ReconcileRefusedGateIndexTerminal` to the `init()` gate loop, so its series exists at zero before any refusal;
   - rewrite the gate const block comment so it no longer closes the set at eight: state that the eight existing gates are applied in order by `reconcileTask` and that `index_terminal` runs at the gather stage after the filter returns the task eligible, and again in the deferred-respawn evaluation. The exact substring `eight gates` must not appear anywhere in the file — write `the eight existing gates` or `those eight`, never the bare phrase;
   - rewrite the `ReconcileRefusedTotal` doc comment so it states the counter covers both the reconcile floor and the deferred-respawn path, that the historical sentence reads `six of the then-existing eight returned silently` (never the bare phrase `eight gates`), and that `all nine series` are pre-initialised at zero. The exact substring `all eight series` must not appear anywhere in the file, and the exact substring `and the deferred-respawn path` must appear on exactly one line and only in this doc comment — nowhere in `ReconcileRefusedGateIndexTerminal`'s comment;
   - update the counter's `Help` string to cover both paths without repeating the phrase above, for example `"Total number of spawn refusals by the eligibility gate that refused them, on every spawn path that applies the gate."`.
   Do not rename any existing gate constant, do not reorder them, and do not change any other counter.
2. In `pkg/metrics/metrics_test.go`, extend the existing registration spec:
   - change its description to say nine gates;
   - add `"index_terminal"` to the `ConsistOf(...)` list of `gatherLabels(mfs, "executor_reconcile_refused_total", "gate")`;
   - add `metrics.ReconcileRefusedGateIndexTerminal` to the per-gate zero loop.
   This spec is the non-vacuous proof: `gatherLabels` reads the default gatherer without creating any series, so it fails until `init()` pre-initialises the ninth.
3. Add `pkg/handler/index_terminal_log_internal_test.go` in `package handler` as a **plain Go test** (`func TestIndexTerminalSuppressionLogAndCounter(t *testing.T)`), following the header convention of `pkg/handler/view_divergence_metrics_internal_test.go`.
   The file declares its own vault constant `indexTerminalLogVault` (value `"openclaw"`) because `testTaskVault` is in `package handler_test`.
   Define, in this file, uniquely named helpers so they cannot collide with the existing package-`handler` test stubs:
   - `type indexTerminalLogView struct { TaskView; tasks []lib.Task }` overriding `Ready() bool` (true) and `Snapshot() []lib.Task` (the fixture tasks);
   - `type indexTerminalLogIndexClient struct { index *claim.TaskIndex }` implementing `Read(context.Context) (*claim.TaskIndex, error)`.
   Build the handler with `NewTaskEventHandler(&stubJobSpawner{}, "dev", "prod", "self", …)` — spawner, namespace, branch, cluster, as in `view_readiness_internal_test.go`; then `stageFilter` false, `runUntagged` true, `stubResolver{}`, `stubResultPublisher{}`, a fresh `pkg.NewTaskStore()`, `libtime.NewCurrentDateTime()`, the ready view, `indexTerminalLogVault`, the default timeouts, `pkg.ClaimModeOff`, `nil`, and `WithVaultAPIIndexClient(client)`. `stubJobSpawner` has pointer receivers, so the literal `stubJobSpawner{}` does not satisfy `spawner.JobSpawner`.
   Fixture: 25 tasks with ids `gate-id-00` … `gate-id-24` (lexicographic order equals numeric order), each `Vault: indexTerminalLogVault`, `status: in_progress`, `phase: execution`, `assignee: claude`; the index reports all 25 as `status: completed`, `phase: human_review`.
   Then:
   - read `before := testutil.ToFloat64(metrics.ReconcileRefusedTotal.WithLabelValues(metrics.ReconcileRefusedGateIndexTerminal))`;
   - run one `ReconcileOnce` inside `captureRefusalLog` (fail the test if it returns an error);
   - assert `strings.Count(out, "event=index_terminal_suppressed task=") == 20`;
   - assert none of `gate-id-20` … `gate-id-24` appears in `out` (the cap names the 20 lexicographically lowest ids);
   - assert the counter delta is exactly 25 (uncapped);
   - run a second identical `ReconcileOnce` inside `captureRefusalLog` and assert `strings.Count(out, "event=index_terminal_suppressed task=") == 20` again.
   The pass also emits `event=view_divergence` lines for the same ids; count only the suppression prefix. Do not assert on the total line count of the capture.
4. Before finishing, re-run `<verification>` and confirm it passes; walk each requirement above against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
- The label is `gate`; the label value is exactly `index_terminal`; no task id, cluster, vault or assignee is ever a label value.
- The counter is never capped: one increment per suppressed id, on both the reconcile and the deferred path.
- The log line is capped at `maxDivergenceLogLines` (20) per pass, sorted ascending by id, at V(2); no new constant for the cap.
- The suppression line is not routed through `refuseReconcile` and does not touch the per-task refusal-transition state.
- Do not change the eight existing gates' names, order or behaviour; a task an existing gate refuses is counted under that gate, never as `index_terminal`.
- The divergence check's reporting is unchanged: same event names, same counts, same units.
- No config knob, flag or opt-out for this gate.
- Do not edit `docs/vault-api-task-index.md` or `CHANGELOG.md` here; a later prompt owns them.
- Repo conventions are frozen: Ginkgo/Gomega v2 for external suites, plain Go tests for internal packages, `github.com/bborbe/errors` wrapping, glog, counterfeiter mocks, license headers on new files.
</constraints>

<verification>
- `ROOTDIR=/workspace make precommit` — must exit 0.
- `go test ./pkg/metrics/ -count=1` — must exit 0.
- `go test ./pkg/handler/ -count=1` — must exit 0.
- `! grep -q 'eight gates' pkg/metrics/metrics.go`
- `! grep -q 'all eight series' pkg/metrics/metrics.go`
- `test "$(grep -c 'and the deferred-respawn path' pkg/metrics/metrics.go)" -eq 1`
- `test "$(grep -c 'index_terminal' pkg/metrics/metrics_test.go)" -ge 1`
</verification>
