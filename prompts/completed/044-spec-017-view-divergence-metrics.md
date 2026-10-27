---
status: completed
spec: [017-view-divergence-detector]
summary: Added the closed-label executor_view_divergence_total counter, wired per-pass and per-id increments, and documented and tested the contract.
execution_id: agent-task-executor-view-divergence-exec-044-spec-017-view-divergence-metrics
dark-factory-version: v0.198.7
created: "2026-10-27T00:00:00Z"
queued: "2026-10-26T23:15:33Z"
started: "2026-10-27T19:53:40Z"
completed: "2026-10-27T19:59:27Z"
---

# Count the divergence check by result, with a fixed increment unit

<summary>
- The divergence check is counted in a new metric with a closed label set
- A successful read counts once per pass, even when it finds divergence
- A failed read counts once per pass and never as an empty result
- Each divergent id and each absent id counts once
- The counter is pre-initialised so a zero series is visible before the first pass
- Task ids never become label values
- The existing vault-ownership labels are not reused, so two different failures cannot merge
- The changelog records the detector
</summary>

<objective>
Give the divergence check a metric whose increment unit is part of its contract, so an operator can distinguish "the check ran and agreed", "the check found divergence", and "the check could not read" without inferring any of them from silence.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for execution rules.

Then read these, in this order:

- `pkg/metrics/metrics.go` — `ReconcilePassesTotal` (the counter whose `task_vault_mismatch` label must **not** be reused), `ClaimTotal` (the "closed result set, no task ids as label values" precedent), `RedriveAbortedTotal` (the bounded, pre-initialised side of the counter rule), and `RedriveCappedTotal` (the unbounded, deliberately-not-pre-initialised side).
- `pkg/metrics/metrics_test.go` — `gatherLabels(mfs, name, labelName)` and the "pre-initializes all N series" pattern.
- `pkg/handler/view_divergence.go` — the file added by prompt 2: `checkViewDivergence`, `compareViewToIndex`, `viewDivergenceReport`.
- `pkg/handler/redrive_streak_cache_internal_test.go` — the plain-`testing` internal-test convention, and `newStreakCacheTestHandler` as the model for building a `*taskEventHandler` in-package.
- `pkg/handler/task_rerun_metric_internal_test.go` — the closest precedent for this prompt: an internal metric test that imports `pkg/metrics` and `testutil`, with its case table outside the test function to stay inside the `funlen` budget.
- `pkg/handler/claim_spawn_order_internal_test.go` — `stubJobSpawner`, the local-stub shape to copy.
- `docs/vault-api-task-index.md` — the contract doc created by prompt 1; this prompt adds its `## Metric contract` section.
- `CHANGELOG.md` — the top is `## v0.44.0`; there is no `## Unreleased` section today.

Coding docs:
`/home/node/.claude/plugins/marketplaces/coding/docs/go-prometheus-metrics-guide.md` (the `counter-pre-initialization` rule),
`/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md`,
`/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`.
</context>

<requirements>
1. **Declare the counter in `pkg/metrics/metrics.go`**, beside `ReconcilePassesTotal`:
   ```go
   // ViewDivergenceTotal counts reconcile-pass view/vault divergence checks by
   // result. The result set is closed at compile time: `ok` and `read_error`
   // increment once per pass that reaches the check, `mismatch` and `absent`
   // increment once per task id. An alert written against one reading is wrong
   // under the other, so the unit is part of the contract. Task ids are never
   // label values.
   //
   // A new name is required rather than reusing
   // executor_reconcile_passes_total{result="task_vault_mismatch"}: that label
   // describes vault-ownership classification over the in-memory snapshot, not
   // a per-id file comparison, and merging the two would let a dashboard read
   // one failure as the other.
   var ViewDivergenceTotal = promauto.NewCounterVec(
       prometheus.CounterOpts{
           Name: "executor_view_divergence_total",
           Help: "Total number of view/vault divergence checks by result (ok, mismatch, absent, read_error).",
       },
       []string{"result"},
   )
   ```
   Pre-initialise all four series in `init()` with `.Add(0)`:
   ```go
   for _, result := range []string{"ok", "mismatch", "absent", "read_error"} {
       ViewDivergenceTotal.WithLabelValues(result).Add(0)
   }
   ```
   The label domain is closed at compile time, so pre-initialisation is the bounded side of the rule: `rate(...) == 0` must be able to fire on a check that never ran.

2. **Increment in `pkg/handler/view_divergence.go`.** Import `github.com/bborbe/agent-task-executor/pkg/metrics`.
   - Read error: `metrics.ViewDivergenceTotal.WithLabelValues("read_error").Inc()` before the `event=view_divergence_read_error` log line, then return.
   - Successful read: `metrics.ViewDivergenceTotal.WithLabelValues("ok").Inc()` **once**, immediately after `Read` returns without error and before the findings loop. `ok` means the read succeeded, not "no divergence". A pass that finds three divergences still increments `ok` once.
   - Per finding: `absent` finding → `WithLabelValues("absent").Inc()` once; divergent finding → `WithLabelValues("mismatch").Inc()` once.
   - Nil `h.vaultIndexClient` → return before any increment. A deployment with no vault-api URL moves no series.

3. **Tests.**
   - `pkg/metrics/metrics_test.go`: add a spec asserting `gatherLabels(mfs, "executor_view_divergence_total", "result")` satisfies `ConsistOf("ok", "mismatch", "absent", "read_error")` — exactly four, no extras — and that each gathered value is `0` before any check runs. Follow the existing "pre-initializes all N series" spec's shape. Because the series are pre-initialised, read the child series with `testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues(...))`; `ToFloat64` on the bare collector panics on a vec with more than one metric.
   - `pkg/handler/view_divergence_metrics_internal_test.go` (`package handler`, plain `testing`, not Ginkgo — a deliberate exception to the spec's Ginkgo rule: `checkViewDivergence` is unexported, and every internal test file in this package carries a header comment stating why the package-external suite does not discover its specs. Copy that header comment.) Define a local stub:
     ```go
     type stubVaultIndexClient struct {
         index *claim.TaskIndex
         err   error
     }

     func (s *stubVaultIndexClient) Read(context.Context) (*claim.TaskIndex, error) {
         return s.index, s.err
     }
     ```
     Do not import `mocks`; it imports `pkg/handler`, so an internal test cannot use it. Build a `taskEventHandler` with `vaultIndexClient` set directly (the field is in-package) or via `WithVaultAPIIndexClient`. Call `h.checkViewDivergence(context.Background(), owned)` and assert deltas with `testutil.ToFloat64` on the child series:
     - success with one divergent and one absent id among three owned → `ok` +1, `mismatch` +1, `absent` +1.
     - success with all three agreeing → `ok` +1, `mismatch` and `absent` unchanged.
     - read error and nil-client series assertions are owned by prompt 4's integration file; do not duplicate them here.
     Assert deltas, never absolutes: these counters are process-global and never reset. Hold the cases in a table declared outside the test function, as `task_rerun_metric_internal_test.go` does — `make precommit` lints with `funlen` at 80 lines, and inline cases with handler construction trip it.

4. **Document the metric contract** in `docs/vault-api-task-index.md`. Add a `## Metric contract` section:
   - name `executor_view_divergence_total`, label `result`, closed set `ok`, `mismatch`, `absent`, `read_error`;
   - `ok` and `read_error` once per pass that reaches the check; `mismatch` and `absent` once per task id;
   - pre-initialised at 0 for all four values;
   - no task id, key or timestamp is ever a label value;
   - why the name is new rather than a label value on `executor_reconcile_passes_total`.

5. **Changelog.** Add a `## Unreleased` section at the top of `CHANGELOG.md`, immediately under the intro lines and above `## v0.44.0`; no such section exists today. Put one bullet under it in the file's existing voice (`- feat: **bold summary.** prose`) describing the detector end to end: one bounded unfiltered index read per reconcile pass that reaches the drive loop, per-id divergence lines carrying both sides of status and phase, absent classification, a summary with compared/divergent/index age/warnings, a read failure is logged and non-fatal, and the new closed-label counter. Follow `changelog-guide.md`; do not rename an existing version heading and do not touch a released section.
</requirements>

<constraints>
- Do NOT add a task id, claim key, job name or timestamp as a label value.
- Do NOT reuse the `task_vault_mismatch` or `task_vault_unstamped` label values from `executor_reconcile_passes_total`.
- Do NOT increment `ok` only when there is no divergence; it is per successful read.
- Do NOT increment `mismatch` or `absent` per pass; they are per id.
- Do NOT increment any series when there is no index client.
- Do NOT pre-initialise an unbounded counter such as `RedriveCappedTotal`; only this closed set is seeded here.
- Do NOT change the check's log lines or the comparison semantics from prompt 2.
- Add the standard license header to every new Go file.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
ROOTDIR=/workspace make precommit
go test -mod=mod -count=1 ./pkg/metrics/ ./pkg/handler/
grep -n 'view_divergence' pkg/metrics/metrics.go
</verification>
