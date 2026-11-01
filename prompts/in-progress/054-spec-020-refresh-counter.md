---
status: failed
spec: [020-executor-config-accepted]
execution_id: agent-task-executor-config-accepted-exec-054-spec-020-refresh-counter
dark-factory-version: v0.198.7
created: "2026-11-01T18:24:00Z"
queued: "2026-11-01T20:36:38Z"
started: "2026-11-01T20:52:38Z"
completed: "2026-11-01T20:55:44Z"
branch: dark-factory/executor-config-accepted
lastFailReason: 'execute prompt: docker run failed: wait command: exit status 137'
---

# Count the accepted-set refresh by result, seeded at zero

<summary>
- The accepted-set refresh is counted by outcome, with a closed two-value label set
- A successful refresh increments the ok series
- A failed Config read increments read_error and leaves the published set untouched
- Both series exist at zero before the first pass, so "never refreshed" is visible
- The counter carries no pair labels; cardinality stays at two
</summary>

<objective>
Make the accepted-set refresh observable on both of its outcomes, so a refresh that never ran, a refresh that failed and a refresh that found nothing changed are three distinguishable states rather than one silence.
</objective>

<context>
This prompt builds on prompt 1, which must already be in the tree. Read it first:

- `pkg/handler/config_accepted.go` — `refreshConfigAccepted(ctx context.Context) error`, the `pairs`/`deleted` summary line, the `h.configProvider == nil` skip, and the `errors.Wrapf(ctx, err, "list agent configs for accepted set")` return on a failed `Get`.
- `pkg/metrics/metrics.go` — `ConfigAccepted` (the gauge from prompt 1), and the `init()` block: `ViewDivergenceTotal`, `ReconcilePassesTotal`, `initClaimSchemeSeries` and `initReconcileRefusedGateSeries` are the precedents for seeding a closed result set with `.Add(0)`.
- `pkg/handler/task_event_handler.go` — `ReconcileOnce`'s `if err := h.refreshConfigAccepted(passCtx); err != nil { glog.Warningf(...) }` call site (it must keep logging and continuing, never abort the pass).

If `pkg/handler/config_accepted.go` or `metrics.ConfigAccepted` is absent, stop and fail with `prompt 1 has not landed: pkg/handler/config_accepted.go or metrics.ConfigAccepted is absent`; do not re-implement it here.

Coding docs:
`/home/node/.claude/plugins/marketplaces/coding/docs/go-prometheus-metrics-guide.md` — the counter-pre-initialisation rule: a closed label domain is seeded so `rate(...) == 0` can fire on a path that never ran; an unbounded domain is not.
`/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`.
</context>

<requirements>
1. **Declare the counter in `pkg/metrics/metrics.go`**, beside `ConfigAccepted`:
   ```go
   // ConfigAcceptedRefreshTotal counts accepted-set refreshes by result. The
   // result set is closed at compile time — `ok` and `read_error` — so both
   // series are seeded in init(): without the seed a refresh that never ran is
   // indistinguishable from one that ran and found nothing changed, and only the
   // first of those is a fault. Deliberately carries no pair labels: this
   // counter answers "did the refresh run", not "what did it publish".
   var ConfigAcceptedRefreshTotal = promauto.NewCounterVec(
       prometheus.CounterOpts{
           Name: "executor_config_accepted_refresh_total",
           Help: "Total number of accepted-set refreshes by result (ok, read_error).",
       },
       []string{"result"},
   )
   ```

2. **Seed the closed set in `init()`** of the same file, beside the other seeding loops:
   ```go
   // Pre-initialise the closed refresh result set so `rate(...) == 0` can fire on
   // a refresh that never ran, rather than skipping silently on an absent series.
   for _, result := range []string{"ok", "read_error"} {
       ConfigAcceptedRefreshTotal.WithLabelValues(result).Add(0)
   }
   ```

3. **Increment in `pkg/handler/config_accepted.go`**, in `refreshConfigAccepted`:
   - The `h.configProvider == nil` branch increments **nothing** — no refresh happened, and counting it would make an unwired handler look healthy.
   - On the `h.configProvider.Get(ctx)` error path, increment `metrics.ConfigAcceptedRefreshTotal.WithLabelValues("read_error").Inc()` immediately before returning the wrapped error. Nothing is published and nothing is deleted on this path — the previous set stays visible, because an empty accepted side makes the consumer's `unless` return every open record.
   - On the `ctx.Err()` cancellation path, also count `read_error` before returning: the read did not complete, and the result set is closed at two values.
   - On success, increment `metrics.ConfigAcceptedRefreshTotal.WithLabelValues("ok").Inc()` after the publish and delete work has completed, immediately before the `event=config_accepted_refreshed` summary line.
   - Do not add a third result value and do not increment `ok` per pair; one refresh is one increment.

4. **Keep the vault label injected, not read.** The `vault` label value is `h.vaultMatcher.Vault()` (set in prompt 1). Do not move vault resolution into `pkg/metrics`, do not call `os.Getenv` there, and do not re-derive the value in this prompt. `VaultMatcher` is constructed by `NewTaskEventHandler` from its `taskVault` argument, which `main.go` supplies as `a.effectiveTaskVault()` (`TASK_VAULT`, falling back to `VAULT_NAME`); the risk this requirement guards is the value being dropped between `main` and the label, not the env resolution itself, which `main_internal_test.go` already covers.

Tests for this behavior land in the next prompt; do not write them here.

Before finishing, re-run every line of `<verification>` and confirm each one passes, then walk spec acceptance criteria 4 and 5 against the change and state which requirement satisfied each.
</requirements>

<constraints>
- The result label domain is closed at exactly two values: `ok`, `read_error`. No other value may be emitted.
- Both series are seeded at `0` in `init()`; do not seed them lazily on first use.
- The counter carries only the `result` label — never `phase`, `task_type`, `vault` or a Config name.
- A failed read must publish nothing and delete nothing; only the counter moves.
- Never call `Reset()` on `metrics.ConfigAccepted` or on this counter.
- `pkg/metrics` must not import `pkg/handler` and must not read the environment.
- `ReconcileOnce` must keep logging the refresh error and continuing the pass; the refresh must never abort a reconcile pass.
- Use `errors.Wrapf` from `github.com/bborbe/errors` on the error path; no `fmt.Errorf`, no bare `return err`.
- Do not change the gauge's name, help or label set from prompt 1.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
`ROOTDIR=/workspace make precommit` — must pass.
`go test -mod=mod -count=1 ./pkg/metrics/ ./pkg/handler/ ./pkg/`
`grep -rn 'executor_config_accepted_refresh_total' pkg/metrics/` — the counter is declared with the documented name, help and `result` label.
`grep -n 'ConfigAcceptedRefreshTotal' pkg/metrics/metrics.go` — the declaration and the `init()` seeding loop are both present.
`grep -c 'WithLabelValues("ok").Inc()' pkg/handler/config_accepted.go` — prints 1.
`grep -c 'WithLabelValues("read_error").Inc()' pkg/handler/config_accepted.go` — prints 2, one on the read-error path and one on the cancellation path.
`! grep -q 'os.Getenv' pkg/metrics/metrics.go` — the metrics package reads no environment.
`! grep -rq 'ConfigAccepted.Reset()' pkg/` — the gauge is never reset.
</verification>
