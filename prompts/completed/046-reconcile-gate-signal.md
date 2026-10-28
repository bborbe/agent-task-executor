---
status: completed
summary: Added executor_reconcile_refused_total with a closed eight-gate label domain, routed all reconcile-floor refusals through a log-once-on-gate-change helper, and covered it with table and transition tests.
execution_id: agent-task-executor-reconcile-gate-signal-exec-046-reconcile-gate-signal
dark-factory-version: v0.198.7
created: "2026-10-28T09:58:15Z"
queued: "2026-10-28T09:58:15Z"
started: "2026-10-28T10:04:34Z"
completed: "2026-10-28T10:12:56Z"
---

# reconcile gate signal

<summary>
- Every refusal by the reconcile eligibility filter becomes countable
- A refusal names which of the eight gates refused the record
- Each task's first refusal at a gate is logged once, not on every pass
- The per-pass log volume stays flat: one line per task per gate change
- The eight gate names form a closed set, seeded at zero so a query is honest
- Existing refusal behaviour is unchanged: same inputs still refuse, same inputs still spawn
- The two gates that already log keep their existing lines exactly as they are

</summary>

<objective>
Make a record refused by the reconcile eligibility filter attributable, so an operator can tell "refused by gate X" from "not in the candidate set at all" — today six of the eight gates return silently and the refusal is indistinguishable from absence.
</objective>

<context>
Read `docs/agent-crd-specification.md` — the `spec.trigger.phases` / `spec.trigger.statuses` contract this filter reads. This repo has no `CLAUDE.md`.

Read `pkg/handler/task_event_handler.go`:
- `reconcileTask` (line 1651) — the eight gates. Six return `(task, nil, false, nil)` with no signal; two log (`resolveConfig`/`resolveConfigForReconcile` and `logPinOutsidePool`).
- `gatherCandidate` (line 1517) — the only caller. Its `!ok` branch is silent by design and must stay silent.
- `logPinOutsidePool` (~line 1956) — the V(2) convention this change follows, including the comment explaining why V(2) and not V(3).
- the `deferredMu` / `deferredRespawns` / `deferredEpochs` fields (~line 286) — the mutex-plus-map shape the new refusal state follows.

Read `pkg/metrics/metrics.go`:
- `ViewDivergenceTotal` (~line 139) — the exemplar for this change: a closed label domain, pre-initialised at 0 in `init()` (~line 275), with the comment explaining why a closed domain makes the seeding honest.
- `RedriveAbortedTotal` (~line 103) — the comment about which counters are and are not pre-initialised.

Read `docs/vault-api-task-index.md` § "Metric contract" — the closed-label counter rule this change follows. Read `docs/claim-lease-protocol.md` § "Claim state machine" for the same label-hygiene rule stated for the claim counters ("Keys and task ids are never label values"; all result series pre-initialised to 0).

Read these coding guides from the coding plugin at `/home/node/.claude/plugins/marketplaces/coding/docs/`, as the completed sibling `prompts/completed/044-spec-017-view-divergence-metrics.md` did:
- `go-prometheus-metrics-guide.md` — counter pre-initialisation for a closed label domain
- `go-glog-guide.md` — the V-level convention and the `event=` prefix
- `changelog-guide.md` — the changelog entry shape

Read `pkg/handler/view_divergence_metrics_internal_test.go` — the closest precedent for the test this change adds: plain-Go internal test, table held outside the function, `testutil` child-series reads, an `assertDelta` helper.
</context>

<requirements>
1. Add a counter `ReconcileRefusedTotal` to `pkg/metrics/metrics.go`, name `executor_reconcile_refused_total` (the reconcile-counter family is `executor_reconcile_*` and `executor_view_divergence_total`; do not use the `agent_executor_` prefix), single label `gate`, help text stating that it counts reconcile-floor refusals by the gate that refused them.
2. Define the gate label domain as exactly these eight values, in this order, as exported constants in the metrics package:
   `foreign_cluster`, `unresolved_config`, `terminal_status`, `status_not_triggered`, `phase_not_triggered`, `terminal_phase`, `stage_mismatch`, `no_routing`.
   Pre-initialise all eight series at 0 in `init()`, following `ViewDivergenceTotal`.
3. Add a new file `pkg/handler/task_event_handler_reconcile_refusal.go` holding:
   - refusal state on the handler: a mutex plus `map[lib.TaskIdentifier]string` recording the last gate that refused each task, with methods to record-and-report-change and to clear an id. Follow the handler's `deferredMu`/`deferredRespawns` shape, not a new standalone type.
   - `func (h *taskEventHandler) refuseReconcile(task lib.Task, gate string) (lib.Task, *pkg.AgentConfiguration, bool, error)` which increments `ReconcileRefusedTotal` for the gate on **every** call, logs one line at `glog.V(2)` **only when the task's gate differs from its last recorded one**, stores the gate, and returns `(task, nil, false, nil)`.
   - the log line format: `event=reconcile_refused task=%s gate=%s task_type=%s phase=%s status=%s stage=%s cluster=%s`.
   - a clear method for an id's recorded gate, called when the task becomes a candidate. Put it on the concrete type only — do not add it to the `TaskEventHandler` interface and do not hand-edit `mocks/`.
   Imports: `lib "github.com/bborbe/agent"` and `pkg "github.com/bborbe/agent-task-executor/pkg"`.
4. In `reconcileTask`, replace each of the eight refusal returns (six silent, two that already log) with `return h.refuseReconcile(task, <gate constant>)` at the matching gate, and clear the recorded gate on the success path (`return task, config, true, nil`).
   The foreign-cluster branch must keep its `h.removeDeferredEntry(task.TaskIdentifier)` call immediately before the new return — it is the backstop for a pre-retag deferred snapshot, and dropping it reopens that bug.
   Do not move, merge or reorder the gates. Do not change any guard expression.
   Keep this file's growth to the single clear call: it is at 1993 lines against revive's `file-length-limit: 2000` (`.golangci.yml`), so put any explanation in the new file, not here.
5. Leave `resolveConfig`'s `glog.Warningf` unknown-assignee line, `resolveConfigForReconcile`'s V(2) `task_type` line and `logPinOutsidePool`'s V(2) line exactly as they are — they are gate 2's and gate 7's existing partial signal.
6. Add `pkg/handler/task_event_handler_reconcile_refusal_internal_test.go` as plain Go `testing` table tests — NOT Ginkgo: a Ginkgo spec in an internal test package is never discovered (see the header comment of `pkg/handler/view_divergence_metrics_internal_test.go`), so `make precommit` would pass with the tests never run. One case per gate, each a constructed `lib.Task` plus a `*pkg.AgentConfiguration` that reaches exactly that gate, asserting that gate's series increments by one and no other gate's series moves:
   - `foreign_cluster`: `cluster` naming another cluster;
   - `unresolved_config`: an assignee that resolves to no Config;
   - `terminal_status`: `status: completed`;
   - `status_not_triggered`: `status: hold`;
   - `phase_not_triggered`: `phase: todo`;
   - `terminal_phase`: `phase: human_review` with a Config whose `trigger.phases` lists `human_review` (this gate is otherwise unreachable — see `<constraints>`);
   - `stage_mismatch`: `stage` differing from the handler's branch, no cluster pin;
   - `no_routing`: empty assignee and no `task_type`.
   Build the handler with `NewTaskEventHandler(...)`, as `TestConsumeMessageDoesNotSpawnBeforeTheViewIsReady` does in `pkg/handler/view_readiness_internal_test.go`: it takes `jobSpawner`, `namespace`, `branch`, `cluster`, `stageFilter`, `runUntagged` and the `pkg.ConfigResolver` as its first seven positional arguments (then the publisher, store, clock, view, vault, timeouts, claim mode and factory, plus variadic `HandlerOption`s) and initialises `deferredRespawns`, `deferredEpochs`, `queuedAtStamps` and `spawnLocks`. It returns `ClaimLifecycleEventHandler`, so assert `h, ok := handler.(*taskEventHandler)` first, as that test does at line 74. A bare `&taskEventHandler{...}` literal leaves `deferredEpochs` nil (`removeDeferredEntry` panics on it) and `resolver` nil (every gate from `unresolved_config` onward panics).
   The resolver is how each case's `*pkg.AgentConfiguration` reaches its gate: use the `stubResolver` shape from `view_readiness_internal_test.go` (embed `pkg.ConfigResolver`, override `Resolve` **and** `ResolveByTaskType` — the existing stub overrides only `Resolve`, so the task_type-fallback cases panic on the embedded nil interface) to return that case's Config, `pkg.ErrConfigNotFound`, or an empty result as the gate requires.
   Hold the case table outside the test function so it stays inside funlen (80 lines / 50 statements, `.golangci.yml`), as `pkg/handler/view_divergence_metrics_internal_test.go` does.
   Assert counter movement with `testutil.ToFloat64` on the **child series** — `testutil.ToFloat64(metrics.ReconcileRefusedTotal.WithLabelValues(gate))`, never the bare vec: a pre-initialised vec panics `ToFloat64` (see the comment in `view_divergence_metrics_internal_test.go`).
   Also assert the transition rule: two consecutive refusals at the same gate increment the counter twice but log once; a refusal at a different gate logs again; clearing the id then refusing at the same gate logs again. Capture the log output by mirroring the mechanism in `capturePinReportLog` (`pkg/handler/task_event_handler_pin_outside_pool_test.go`: flag `v=2` + `logtostderr`, `os.Stderr` pipe, `glog.Flush`) — that helper lives in `package handler_test` and asserts with Gomega, so it cannot be called from here, and a plain buffer assertion sees nothing.
7. Add a spec to `pkg/metrics/metrics_test.go` asserting that `gatherLabels(mfs, "executor_reconcile_refused_total", "gate")` is exactly the eight gate values and each reads 0, matching the view-divergence spec (`executor_view_divergence_total`, ~line 168). This is the closed-domain boundary assertion; the handler test proves movement, this one proves the seeded set.
8. Add a `## Unreleased` section at the top of `CHANGELOG.md`, immediately under the intro lines and above `## v0.46.3`; no such section exists today. One bullet in the file's existing voice (`- feat: **bold summary.** prose`) describing the new closed-label counter, the eight gate values, and the log-once-on-change rule. Do not rename an existing version heading or touch a released section.

Before finishing, re-run `<verification>` and confirm it passes; walk each of the eight gates and the transition rule against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
- Behaviour must not change: every input that refused before must still refuse, and every input that was a candidate must still be a candidate. In particular the foreign-cluster branch keeps `removeDeferredEntry`.
- The label set is closed and exactly eight values; never add a label, never put a task id, cluster, stage or assignee in a label value.
- `parseAndFilter` and `skipAndEvict` are out of scope — the Kafka path's skips keep their existing `TaskEventsTotal` labels; do not add `refuseReconcile` there.
- `terminal_phase` is unreachable in the deployed fleet today: no Config in the deployed fleet currently lists a terminal phase in `trigger.phases` (operator-measured on nuke dev 2026-10-28), and `phase_not_triggered` fires first. The unit test is the only way to exercise it; do not change any Config, any CRD, or any deployment to make it reachable.
- The refusal-state map is bounded by the tasks that have been refused and have not since become candidates — not by view size, because a task refused and then removed from the view keeps its entry. Do not add pruning machinery, but do clear an id on the success path.
- Do not touch `gatherCandidate`'s `!ok` branch comment or behaviour.
- No `docs/` change is required — no existing doc enumerates reconcile-floor metrics.
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit` — must pass.
</verification>
