---
status: completed
spec: [020-executor-config-accepted]
summary: Published executor_config_accepted{phase,task_type,vault} 1 from every reconcile pass with set-then-delete refresh, wired through a handler option and factory/main, and corrected the CRD default-phase doc.
execution_id: agent-task-executor-config-accepted-exec-053-spec-020-config-accepted-gauge
dark-factory-version: v0.198.7
created: "2026-11-01T18:24:00Z"
queued: "2026-11-01T20:36:35Z"
started: "2026-11-01T20:36:37Z"
completed: "2026-11-01T20:52:36Z"
branch: dark-factory/executor-config-accepted
---

# Publish the accepted (phase, task_type) set as a gauge, from the reconcile pass

<summary>
- A running executor states, as Prometheus series, which (phase, task_type) pairs its Configs accept
- One series per pair, value 1, tagged with the executor's task vault
- The set is derived from the Configs on every reconcile pass, not from a literal
- A pair no Config accepts any more is deleted by label values; the family is never reset
- A refresh can never leave the family empty while pairs are still accepted
- A failed Config read publishes nothing and deletes nothing, so the previous set survives
- The set is published before the pass's early returns, including view-not-ready
- Each pass that publishes emits one summary line with configs, pairs and deleted counts
- The Config provider is injected through a handler option, so no interface or fake changes
- The stale default-phase list in the CRD doc is corrected to match the code
</summary>

<objective>
Give the already-merged `unreachable-phase` alert the right-hand side it joins against: publish `executor_config_accepted{phase,task_type,vault} 1` for exactly the pairs the executor's Config CRs accept, refreshed at the top of every reconcile pass, with set-then-delete ordering so the family is never spuriously empty.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for execution rules.

Then read these files, in this order, fully:

- `pkg/handler/task_event_handler.go` — the `taskEventHandler` struct (fields `resolver`, `vaultMatcher`, `reconcilePassTimeout`), `NewTaskEventHandler`, `ReconcileOnce` (the `passCtx, cancel := context.WithTimeout(...)` line and the `if !h.view.Ready()` early return), `effectiveTriggerPhases(cfg *pkg.AgentConfiguration) domain.TaskPhases` (mid-file), and the `defaultTriggerPhases` var (near the top of the file, not beside the function).
- `pkg/handler/task_event_handler_options.go` — the `HandlerOption` pattern; copy the shape of `WithVaultAPIIndexClient`.
- `pkg/handler/vault_matcher.go` — `VaultMatcher.Vault()` returns the executor's own task vault; the handler already holds it as `h.vaultMatcher`.
- `pkg/task_type_filter.go` — `EffectiveTaskTypes(taskType string, taskTypes []string) []string`.
- `pkg/agent_configuration.go` — `AgentConfiguration.Trigger` is `*agentv1.Trigger`, the same type as `agentv1.ConfigSpec.Trigger`, so a `pkg.AgentConfiguration{Trigger: item.Spec.Trigger}` value can be handed to `effectiveTriggerPhases` without converting the whole Config.
- `k8s/apis/agent.benjamin-borbe.de/v1/types.go` — `Trigger{Phases domain.TaskPhases; Statuses domain.TaskStatuses}` and `ConfigSpec.TaskType` / `ConfigSpec.TaskTypes`.
- `pkg/metrics/metrics.go` — the declaration style: `promauto.NewGaugeVec` / `NewCounterVec` with a doc comment explaining why the metric exists. Note `ReconcilePassesTotal` is the neighbour whose label vocabulary this one must not reuse.
- `pkg/config_resolver.go` — how `k8s.Provider[agentv1.Config]` is used: `items, err := r.provider.Get(ctx)`.
- `pkg/factory/factory.go` — `CreateConfigResolver` and `CreateConsumer` (the `handler.NewTaskEventHandler(...)` call with its option list).
- `main.go` — `eventHandlerConfig := factory.CreateEventHandlerConfig()`, the `resolver := factory.CreateConfigResolver(eventHandlerConfig, ...)` line and the `factory.CreateConsumer(...)` call. `eventHandlerConfig` is the `k8s.Provider[agentv1.Config]` to pass in.
- `docs/agent-crd-specification.md` — lines 53 and 58 state the default phases as `[planning, in_progress, ai_review]`; `defaultTriggerPhases` in the code is `planning`, `execution`, `ai_review`.

Coding docs:
`/home/node/.claude/plugins/marketplaces/coding/docs/go-prometheus-metrics-guide.md`,
`/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`,
`/home/node/.claude/plugins/marketplaces/coding/docs/go-functional-options-pattern.md`,
`/home/node/.claude/plugins/marketplaces/coding/docs/go-composition.md`.
</context>

<requirements>
1. **Declare the gauge in `pkg/metrics/metrics.go`**, in the same block style as its neighbours:
   ```go
   // ConfigAccepted is the set of (phase, task_type) pairs this executor's agent
   // Config CRs currently accept, one series per pair with value 1, tagged with
   // the publishing executor's task vault.
   //
   // It is a gauge, not a counter: it describes current state. It is the
   // right-hand side of the cross-repo `unreachable-phase` join — the controller
   // publishes agent_controller_open_tasks on the same three labels and subtracts
   // this side with `unless on (phase, task_type)`. The label names are frozen by
   // that merged rule, and the metric name carries no `_total`.
   //
   // Never call Reset() on it. A reset-then-set refresh leaves a window in which
   // the family is empty, and under `unless` an empty right side returns every
   // open record — the exact false all-records alert this metric exists to
   // prevent. Removed pairs are deleted by label values instead.
   var ConfigAccepted = promauto.NewGaugeVec(
       prometheus.GaugeOpts{
           Name: "executor_config_accepted",
           Help: "Accepted (phase, task_type) pairs per vault, value 1 per pair; the right-hand side of the unreachable-phase join.",
       },
       []string{"phase", "task_type", "vault"},
   )
   ```
   Do **not** seed any label values for this gauge — a `GaugeVec` with no label values registers no family at all, and a fabricated pair would assert an acceptance no Config declared. That absence before the first pass is accepted and is absorbed by the alert's `for: 1h`.

2. **Add a new file `pkg/handler/config_accepted.go`** (package `handler`, standard license header). It holds the pair type, the set computation and the refresh:

   ```go
   // configAcceptedPair is one (phase, task_type) pair the Configs accept.
   type configAcceptedPair struct {
       phase    string
       taskType string
   }

   // configAcceptedPairs returns the union over configs of (effective trigger
   // phases) x (effective task types). The union is namespace-wide by design:
   // every executor in the namespace sees every Config and publishes the same
   // pairs; only the vault label differs between instances.
   func configAcceptedPairs(items []agentv1.Config) map[configAcceptedPair]struct{} {
       pairs := make(map[configAcceptedPair]struct{})
       for _, item := range items {
           phases := effectiveTriggerPhases(&pkg.AgentConfiguration{Trigger: item.Spec.Trigger})
           for _, phase := range phases {
               for _, taskType := range pkg.EffectiveTaskTypes(item.Spec.TaskType, item.Spec.TaskTypes) {
                   pairs[configAcceptedPair{phase: string(phase), taskType: taskType}] = struct{}{}
               }
           }
       }
       return pairs
   }
   ```

   and the refresh method on `*taskEventHandler`:
   ```go
   // refreshConfigAccepted publishes the accepted (phase, task_type) set for this
   // executor's vault. Set-then-delete, under one lock: a refresh interrupted
   // after the deletes but before the sets would leave the family missing pairs
   // it accepts, which is the under-publish the consumer's `unless` turns into an
   // all-records alert. Interrupted the other way round the worst outcome is a
   // stale pair surviving one interval — a superset, never a subset.
   func (h *taskEventHandler) refreshConfigAccepted(ctx context.Context) error {
       if h.configProvider == nil {
           glog.V(2).Infof("event=config_accepted_refresh_skipped reason=no_provider")
           return nil
       }
       items, err := h.configProvider.Get(ctx)
       if err != nil {
           return errors.Wrapf(ctx, err, "list agent configs for accepted set")
       }
       if err := ctx.Err(); err != nil {
           return errors.Wrapf(ctx, err, "refresh accepted config set cancelled")
       }
       pairs := configAcceptedPairs(items)
       vault := h.vaultMatcher.Vault()

       h.configAcceptedMu.Lock()
       defer h.configAcceptedMu.Unlock()
       for pair := range pairs {
           metrics.ConfigAccepted.WithLabelValues(pair.phase, pair.taskType, vault).Set(1)
       }
       deleted := 0
       for pair := range h.configAcceptedPairs {
           if _, ok := pairs[pair]; ok {
               continue
           }
           metrics.ConfigAccepted.DeleteLabelValues(pair.phase, pair.taskType, vault)
           deleted++
       }
       h.configAcceptedPairs = pairs

       glog.Infof(
           "event=config_accepted_refreshed configs=%d pairs=%d deleted=%d",
           len(items), len(pairs), deleted,
       )
       return nil
   }
   ```
   The `pairs` value in that line is the number of series this refresh published for this vault; the `deleted` value is the number it removed. Do not add fields to the line.

3. **Extend the `taskEventHandler` struct** in `pkg/handler/task_event_handler.go` with three fields (place them beside `vaultMatcher`):
   ```go
   // configProvider is the Config store the accepted-set refresh reads. It is the
   // same provider the resolver reads; nil skips the refresh, which is the shape
   // of every test that does not assert the metric.
   configProvider libk8s.Provider[agentv1.Config]
   // configAcceptedMu guards configAcceptedPairs: the pairs published by the last
   // successful refresh, so a pair no Config accepts any more is deleted by label
   // values rather than by resetting the family.
   configAcceptedMu    sync.Mutex
   configAcceptedPairs map[configAcceptedPair]struct{}
   ```
   Add the import `agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"` to that file (`libk8s` and `sync` are already imported). Initialise the map in `NewTaskEventHandler`: `configAcceptedPairs: make(map[configAcceptedPair]struct{}),`.

4. **Add the option** to `pkg/handler/task_event_handler_options.go`:
   ```go
   // WithConfigProvider supplies the Config store the accepted-set metric reads.
   // Nil (the default) skips the refresh.
   func WithConfigProvider(provider libk8s.Provider[agentv1.Config]) HandlerOption {
       return func(h *taskEventHandler) {
           h.configProvider = provider
       }
   }
   ```
   Import `libk8s "github.com/bborbe/k8s"` and the `agentv1` package in that file.

5. **Call the refresh at the top of `taskEventHandler.ReconcileOnce`** in `pkg/handler/task_event_handler.go`, immediately after `defer cancel()` and **before** the `passCtx = withJobCache(passCtx)` reassignment, so it runs under the pass timeout and before every early return:
   ```go
   // The accepted-pair set is published before every early return below. A view
   // that is not ready, an unstamped snapshot or a mismatched vault are
   // independent of which pairs the Configs accept, and the consumer needs the
   // second fact regardless of the first.
   if err := h.refreshConfigAccepted(passCtx); err != nil {
       glog.Warningf("event=config_accepted_refresh_failed err=%v", err)
   }
   ```
   Do **not** touch `serviceReconcileLoop.ReconcileOnce` in `pkg/spawner/service_reconcile_loop.go` — that type is a different component and attaching the refresh there satisfies nothing.

6. **Wire the provider through the factory and `main.go`.**
   - `pkg/factory/factory.go`, `CreateConsumer`: add a parameter `configProvider libk8s.Provider[agentv1.Config]` immediately after `resolver pkg.ConfigResolver`, and add `handler.WithConfigProvider(configProvider),` to the option list of the `handler.NewTaskEventHandler(...)` call.
   - `main.go`, the `factory.CreateConsumer(...)` call: pass `eventHandlerConfig` in that new position (immediately after `resolver`). `eventHandlerConfig` is already the `pkg.EventHandlerConfig` the resolver was built from, and it satisfies `libk8s.Provider[agentv1.Config]` — that is the assignment `pkg.NewConfigResolver(handler pkg.EventHandlerConfig, ...)` already relies on.
   - `CreateConsumer` has exactly one call site (`main.go`). Confirm with `grep -rn 'CreateConsumer(' .` before finishing.

7. **Correct the stale default in `docs/agent-crd-specification.md`.** In the `spec.trigger` table row and in the paragraph below the table, the default phase list reads `[planning, in_progress, ai_review]`; `defaultTriggerPhases` in `pkg/handler/task_event_handler.go` is `planning`, `execution`, `ai_review`. Change both default lists, keeping each document's own voice:
   - Table row, line 53 — ``phases `[planning, in_progress, ai_review]` `` → ``phases `[planning, execution, ai_review]` ``.
   - Paragraph below the table, line 58 — ``(phases: `planning`, `in_progress`, `ai_review`; statuses: `in_progress`)`` → ``(phases: `planning`, `execution`, `ai_review`; statuses: `in_progress`)`` — each phase name stays individually backticked, and the *status* default stays `in_progress`.
   Do not change the `spec.trigger.phases` "Valid values" list — `in_progress` is still a valid phase string, and `heartbeat`/`in_progress` mentions about task *status* stay as they are.

Tests for this behavior are the next-but-one prompt's job; do not write them here.

Before finishing, re-run every line of `<verification>` and confirm each one passes, then walk acceptance criteria 1–6 of the spec against the change and state, for each, which requirement satisfied it.
</requirements>

<constraints>
- It is a gauge named exactly `executor_config_accepted`; no `_total` suffix, and the Go identifier is `metrics.ConfigAccepted`.
- Label names are frozen: `phase`, `task_type`, `vault` — exactly these three, no others (no task id, assignee, Config name or namespace).
- The value is always `1`; never increment, sum or scale by Config count. Two Configs accepting the same pair produce one series.
- Never call `Reset()` on the gauge. Removal is `DeleteLabelValues` only.
- Never publish, delete or clear anything when `provider.Get` fails; the previous set must survive a transient API error.
- Never seed a placeholder series for the gauge.
- `pkg/metrics` must not read the environment: the vault value arrives as an argument from the handler, which already holds it as `h.vaultMatcher.Vault()` (which is `TASK_VAULT`, falling back to `VAULT_NAME`, resolved in `main.go`).
- Reuse `effectiveTriggerPhases` (with its `defaultTriggerPhases` fallback) and `pkg.EffectiveTaskTypes`. Do not fork or re-implement either; if one must move to be reachable, move it — do not copy it.
- One `provider.Get(ctx)` per pass; no per-Config API call. The whole refresh runs under the pass context.
- The union is over Configs: one Config's removal must not narrow the set for another Config that still accepts the pair.
- Use `errors.Wrapf(ctx, err, "...")` from `github.com/bborbe/errors`; no `fmt.Errorf`, no bare `return err` on a new path.
- Do not change spawn, routing or eligibility behaviour; this metric observes the existing resolution and feeds nothing back.
- Add the standard license header to every new Go file.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass (the nil-provider path must keep every existing handler test working unchanged).
</constraints>

<verification>
`ROOTDIR=/workspace make precommit` — must pass.
`go test -mod=mod -count=1 ./pkg/metrics/ ./pkg/handler/ ./pkg/ ./pkg/factory/`
`grep -rn 'executor_config_accepted' pkg/metrics/` — the gauge is declared with the documented name, help and three labels.
`! grep -rq 'ConfigAccepted.Reset()' pkg/` — the gauge is never reset.
`grep -n 'metrics.ConfigAccepted' pkg/handler/config_accepted.go` — the refresh publishes through `metrics.ConfigAccepted`.
`grep -n 'config_accepted_refreshed' pkg/handler/config_accepted.go` — the summary line exists with `configs=`, `pairs=`, `deleted=`.
`grep -c 'execution' docs/agent-crd-specification.md` — prints 2, one per corrected default list (a half fix prints 1); `! grep -q 'in_progress, ai_review' docs/agent-crd-specification.md` — the stale table-row list is gone.
</verification>
