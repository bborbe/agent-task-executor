---
status: approved
approved: "2026-11-01T16:13:03Z"
branch: dark-factory/executor-config-accepted
---

## Summary

- The executor resolves every Config on each reconcile and therefore knows exactly which `(phase, task_type)` pairs the fleet accepts — and exports none of it, so no consumer can join an open agent record against the accepted side.
- The consumer is already written and already loaded on dev: `bborbe/nuke`'s `agent/alerts/unreachable-phase-alert.yaml` (PR #930, `aa31b1b8`) joins `agent_controller_open_tasks` against `executor_config_accepted` with `unless on (phase, task_type)`.
- PromQL's `unless` returns **every** left element when the right side is empty, so the missing half is not a blank dashboard — once the controller gauge lands without this metric, the rule fires on every open record in the watched vault instead of on the defect.
- The set is already computed in-process: `effectiveTriggerPhases` decides the phase half on every task the executor evaluates, and `EffectiveTaskTypes` decides the type half. This spec exports that resolution rather than re-deriving it.
- The hard part is not publishing the set but never un-publishing it: a refresh that blanks the series, even for one scrape or one failed Config read, reproduces the exact all-records firing the metric exists to prevent.

## Problem

`pkg/config_resolver.go` lists the Config CRs on every `Resolve` call, and `pkg/handler/task_event_handler.go`'s `effectiveTriggerPhases` plus `pkg/task_type_filter.go`'s `EffectiveTaskTypes` turn each of them into the `(phase, task_type)` pairs that agent accepts. That union is the fleet's accepted set, recomputed many times per reconcile pass, and it is discarded every time.

The consequence is on the consumer side. The `unreachable-phase` check needs to fail when a task record enters a pair no Config accepts. Neither component holds both halves: the executor resolves Configs but cannot enumerate the vault, and the controller scans the vault but carries no Kubernetes Config client. The chosen mechanism is therefore a two-metric join — `executor_config_accepted{vault,phase,task_type}` on this side, `agent_controller_open_tasks{vault,phase,task_type}` on the other — and the join is only sound when both sides carry series.

`unless on (phase, task_type)` subtracts right from left. An empty right side subtracts nothing, so the left side passes through whole. That is not a degraded reading; it is the opposite of the intended one — the rule would fire on every open record and read as a total outage, while the defect it was written to find stays indistinguishable from the noise. The alert is already applied on dev (`monitoring` namespace, CR `unreachable-phase`, `for: 1h`, currently `inactive` only because both sides are empty). This half must land first.

## Goal

A running executor states, as Prometheus series, exactly which `(phase, task_type)` pairs its Configs currently accept — one series per pair, tagged with the vault that executor owns — and the series set tracks Config changes in both directions: a newly accepted pair appears, a pair a Config no longer accepts disappears, and neither a refresh nor a failed Config read ever removes a pair that is still accepted.

## Non-goals

- **The controller-side gauge.** A separate change in the controller repo. This spec neither writes it nor waits on it.
- **The alert itself.** Already authored and merged in `bborbe/nuke`. Not re-authored here, and this spec does not change its expression.
- **Deciding which pairs are accepted.** The metric reports the resolution as it stands. Changing what a Config accepts is a Config change, not a metric change.
- **Filtering out `human_review`, `done`, or any other phase.** That exclusion deliberately lives in the alert expression where a reviewer can see it. The metric exports what Configs accept, including pairs that only ever rest.
- **Per-task or per-record accounting.** This is a set, not a census. Counting open records is the controller's job.
- **A second source of truth for routing.** Eligibility and spawning keep reading the Config directly. The metric observes the same resolution; it does not feed anything back into it.

## Assumptions

- The executor's Config provider returns the complete set for the namespace on each `Get`, so one call per pass is a full picture rather than a partial one. `Resolve` already relies on this on every task.
- Config CRs are the only producer of the accepted set. A pair accepted by a hand-created Config outside the chart still appears, which is the desired behaviour — the metric reports what the cluster actually holds.
- The controller's gauge will label `vault` with the vault slug the controller scans. This spec's `vault` label must equal that slug on the same board, which is why it reads `TASK_VAULT` rather than `VAULT_NAME` (see Constraints) — on the prod `personal` board those two differ, and only the former matches.

## Acceptance Criteria

- [ ] **The series carries exactly the three join labels, and the value is `1`.** The rendered exposition contains `executor_config_accepted{phase="…",task_type="…",vault="…"} 1` — label names `phase`, `task_type`, `vault`, and no other label — and the metric name carries no `_total` suffix.
  - evidence: the integration test's gathered output has one family named `executor_config_accepted` whose `Help` is non-empty, whose label-name set equals exactly `{phase, task_type, vault}`, and whose sample values are all `1`; and `grep -c 'executor_config_accepted_total' pkg/` returns 0

- [ ] **The set is derived from the Configs, not from a literal — adding and removing a Config changes it.** Given a fake provider holding Configs A and B, the gathered series equal the union of their cross-products; removing B's `task_type` from the provider and refreshing removes exactly that pair and leaves A's pairs at `1` in the same gather.
  - evidence: a `DescribeTable` over provider snapshots asserts the full gathered label-set per entry — entry (a) two Configs with disjoint types yields both sets; entry (b) after removing one type, the pair count drops by exactly the number of phases that Config triggered and every surviving series still reads `1`; entry (c) a pair declared by two Configs appears once, not twice

- [ ] **A vanished pair is deleted, and no refresh blanks the surviving set.** The implementation never calls `Reset()` on the gauge; a pair that is no longer accepted is removed by deleting its label values, so a scrape taken between two states can never observe an empty series set.
  - evidence: `grep -rn 'ConfigAccepted.Reset()\|\.Reset()' pkg/metrics/ pkg/handler/ | grep -c 'executor_config_accepted\|ConfigAccepted'` returns 0 — ⚠️ **the grep, not the gather, is what catches `Reset()`:** a `Reset()`-then-`Set`-everything implementation yields a correct family at gather time and only the empty window between the two calls is invisible to a post-hoc read. The entry-(b) fixture's separate contribution is the ordering check: after the removal, every unaffected pair is still present with value `1` in the same gather, which a delete-then-set order that drops pairs fails. (A post-hoc gather cannot observe the ordering either; DB 4's rationale is where that argument lives.)

- [ ] **The `vault` label is `TASK_VAULT`, falling back to `VAULT_NAME`, and is injected rather than read at the metric site.** A deployment carrying only `VAULT_NAME=shared` emits `vault="shared"`; the same deployment with `TASK_VAULT=personal` also set emits `vault="personal"`.
  - evidence: two unit entries at the gauge/handler level asserting the **emitted** label for an injected task vault of `shared` and of `personal` — the assertion is on the series, not on the resolver; and `grep -c 'os.Getenv("VAULT_NAME")' pkg/metrics/` returns 0, so the value arrives as a constructor argument rather than an env read inside the metric package. ⚠️ Do not satisfy this by re-testing `effectiveTaskVault()`: env resolution is already covered by `main_internal_test.go`'s own table, and the risk this AC exists for is the value being dropped between `main` and the label.

- [ ] **A failed Config read leaves the previous set intact.** When the provider returns an error, the gathered series are byte-identical to the previous successful gather, the failure is counted on its own series, and no pair is deleted.
  - evidence: a fixture that publishes a set, then fails the provider on the next refresh — the second gather equals the first, `executor_config_accepted_refresh_total{result="read_error"}` incremented by 1, and `executor_config_accepted_refresh_total{result="ok"}` did not increment; and both `result="ok"` and `result="read_error"` are present at `0` in a gather taken before the first pass, proving the closed set is seeded rather than appearing only on first use

- [ ] **The set is published by the reconcile pass itself, including a pass that aborts early — not by a refresh on its own ticker.** A pass that returns on `view_not_ready` still publishes the accepted set and emits one summary line; a process whose reconcile loop never runs publishes nothing and logs nothing.
  - evidence: a fixture whose view is not ready gathers a non-empty `executor_config_accepted` family, and its captured log contains one line matching `event=config_accepted_refreshed configs=\d+ pairs=\d+ deleted=\d+` **whose `pairs` value equals the number of series that fixture gathered** — so `configs=0 pairs=0 deleted=0` beside a non-empty family fails. A fixture whose `taskEventHandler.ReconcileOnce` is never invoked gathers no series carrying that fixture's own `vault` value and emits no `event=config_accepted_refreshed` line. A standalone `time.Tick` goroutine satisfies the first half and fails the second.
  - ⚠️ **Each fixture must start from an emptied family.** `metrics.ConfigAccepted` is a package-level `promauto` gauge on the default registry, so a family-granularity assertion fails for a correct implementation as soon as any earlier fixture has published into it. Isolate per fixture either by deleting the label values of every gathered tuple first (`DeleteLabelValues` — a delete, so the no-`Reset()` rule still holds) or by narrowing the assertion to the fixture's own `vault` value.

- [ ] **Post-Deploy (Rung-2):** the dev `personal` executor publishes the set tagged with its **task** vault, not its Config-resolution vault.
  - evidence: resolve the pod in the same shell, then read its metrics — `POD=$(kubectlnukedev -n agent get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep '^agent-task-executor-personal' | head -1)` then `kubectlnukedev -n agent get --raw "/api/v1/namespaces/agent/pods/$POD:9090/proxy/metrics" | grep executor_config_accepted` returns ≥2 series, each with `vault="personal"` and value `1`. Dev's `personal` entry carries `vaultName: shared` / `taskVault: personal`, so `vault="shared"` here is the `VAULT_NAME`-only defect this AC exists to catch. (`:9090` is the chart's `LISTEN` value and `containerPort`, not an assumption.)
  - `deploy_check:` `kubectlnukedev -n agent get deploy/agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `$(git describe --tags --abbrev=0)`

- [ ] **Post-Deploy (Rung-3):** both prod executors publish the set, and a named accepted pair is present while a named unaccepted pair is absent in the same read.
  - evidence: the same pod-resolution and `:9090` proxy read, run on `kubectlnukeprod -n agent` once with prefix `agent-task-executor-openclaw` and once with `agent-task-executor-personal`, returns ≥2 series each with `vault="openclaw"` / `vault="personal"` and value `1`; the `openclaw` read contains a series with `phase="execution",task_type="github-update-go"` and contains no series with `task_type="probe"` at `phase="human_review"` (the `egress-probe` Config accepts `execution` only)
  - `deploy_check:` `kubectlnukeprod -n agent get deploy/agent-task-executor-openclaw -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `$(git describe --tags --abbrev=0)`

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — format, generate, test, check, addlicense all clean
- `go test -mod=mod -count=1 ./pkg/metrics/ ./pkg/handler/ ./pkg/` — the refresh, the fixtures and the existing handler suite pass
- `grep -rn 'executor_config_accepted' pkg/metrics/` — the gauge is declared with the documented name, help and label set. Package-scoped, not file-pinned, so a new `pkg/metrics/` file carrying the declaration passes
- `grep -n 'executor_config_accepted' docs/` — the metric contract doc exists and names the metric, its three labels, the level-not-counter rule and the never-empty-on-failure rule
- `grep -rn 'ConfigAccepted.Reset()' pkg/` — returns 0 lines; the scoped form, because a bare `Reset()` grep would match unrelated metrics

### Operator-executable (runs on the host after PR merge)

- `make test` — full suite green
- `kubectlnukedev -n agent get deploy agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}'` — dev runs the released tag
- `kubectlnukedev -n agent get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep '^agent-task-executor-personal'` then the `:9090` proxy read above — the set is live on dev

## Desired Behavior

1. **One Config read per pass, at the top of the pass.** `taskEventHandler.ReconcileOnce` resolves the accepted set from a single `provider.Get(ctx)` before the view-readiness check and before every other early return, so a pass that aborts on `view_not_ready`, `task_vault_unstamped` or `task_vault_mismatch` still publishes the set. ⚠️ **Name the type, not just the method:** `pkg/spawner/service_reconcile_loop.go` defines its own `ReconcileOnce`, and attaching the refresh to that one would satisfy nothing this spec asks for. The extra read costs no API round trip: `k8s.Provider.Get` is served from an in-memory map behind a mutex (`bborbe/k8s`'s `eventHandler` implementation), so this is a map scan per pass whether or not the pass resolved any task.

   **The handler does not currently hold that provider.** `taskEventHandler` carries `resolver pkg.ConfigResolver` in `pkg/handler/task_event_handler.go`, whose two methods — `Resolve` and `ResolveByTaskType` — both return a single Config and neither enumerates. Two ways to reach the set, and this spec picks the second:
   - *(rejected)* add a third method, `List(ctx) ([]AgentConfiguration, error)`, to `pkg.ConfigResolver`. That regenerates `mocks/config_resolver.go` and breaks five hand-rolled fakes — three that would fail to compile (`staticResolver` and `failingResolver` in `pkg/handler/claim_job_control_internal_test.go`, `indexTerminalSeedResolver` in `pkg/handler/index_terminal_seed_internal_test.go`) and two that embed the interface and would panic when the refresh called the new method (`refusalResolver` in `pkg/handler/task_event_handler_reconcile_refusal_internal_test.go`, `stubResolver` in `pkg/handler/view_readiness_internal_test.go`). (File names only: line numbers on test fakes drift the moment a prompt touches the file.)
   - **(chosen) pass the existing `k8s.Provider[agentv1.Config]` into the handler alongside the resolver** and call `Get` on it directly. No interface changes, no mock regeneration, no fake churn — the provider is already constructed in `main.go` for the resolver.

   Each pass that reaches the refresh also emits one log line carrying the number of Configs read, the number of pairs published and the number deleted, so a set that silently stopped tracking Config changes is visible in the log as well as in the series.

2. **The pair set is the union over Configs of phases × types.** For each Config, the phase half is `effectiveTriggerPhases(cfg)` — which already falls back to `planning`, `execution`, `ai_review` when the trigger is absent or empty — and the type half is `EffectiveTaskTypes(cfg.TaskType, cfg.TaskTypes)`, which unions the deprecated singular field with the list and removes duplicates. The cross-product of the two is that Config's accepted pairs; the metric's set is their union across every Config returned.

   ⚠️ **That union is namespace-wide, not vault-scoped, and the choice is deliberate.** The provider is fed by a namespace watch, so every executor in `agent` sees every Config and publishes the **same** pair set — only the `vault` label differs between instances. Scoping the union to the Configs this executor's resolver would match was considered and rejected: it is not what the executor actually accepts (it would spawn against any Config in the namespace that resolves), and a vault with no Configs of its own would publish an empty right-hand side, which under `unless` returns every open record. See DB 6 for what the label therefore does and does not mean.

3. **A pair is published as one series, value `1`, and the label vocabulary is closed.** The set is a gauge because it describes current state, not an accumulation. Two Configs accepting the same pair produce one series, not two, and the value is never incremented, summed or scaled by Config count. Exactly three labels — `vault`, `phase`, `task_type` — with no task id, assignee, Config name or namespace among them, so the series count is bounded by Configs × trigger phases × task types: a few dozen for this fleet.

4. **Removal is a delete, never a reset, and the order is set-then-delete.** The refresh sets every pair in the new set first, then deletes the label values of pairs present in the previous set and absent from the new one. The order is load-bearing, not stylistic: a refresh interrupted after the delete and before the sets would leave the family **missing pairs it accepts**, which is the under-publish the consumer's `unless` turns into an all-records alert. Interrupted the other way round, the worst outcome is a stale pair surviving one interval. The invariant is therefore **never spuriously empty and never a subset** — a scrape may briefly see a superset, never an absence. A genuinely empty accepted set (every Config gone) is a true reading and is allowed to empty the family; the rule forbids emptiness caused by ordering, a read error or a reset, not emptiness that reflects the cluster.

5. **A failed read is a non-event for the series.** When `provider.Get` returns an error, the refresh publishes nothing, deletes nothing, and increments `executor_config_accepted_refresh_total{result="read_error"}`; a successful refresh increments the same counter with `result="ok"`. The previous set stays visible, because an empty accepted side makes the consumer's `unless` return every open record.

6. **The `vault` label is the executor's task vault.** It carries `TASK_VAULT` when set and `VAULT_NAME` otherwise — the value the handler already holds as its vault matcher — so it equals the controller's `vaultName` label on the same board. `VAULT_NAME` alone is wrong here: the prod `personal` entry sets `vaultName: shared` / `taskVault: personal`, so a `VAULT_NAME`-sourced label would emit `shared` for the board the controller reports as `personal`. ⚠️ **That does not break today's join, and the spec must not claim it does.** The merged expression joins `on (phase, task_type)` only and carries no `vault` selector on either operand (`bborbe/nuke` `agent/alerts/unreachable-phase-alert.yaml`), so a wrong `vault` value would not change the current result set. ⚠️ **`vault` names the publishing instance; it does not scope the set.** Because the union is namespace-wide (DB 2), every executor publishes the same pairs and the label says only *which executor published this line*. It is not a claim that these pairs are accepted only by that vault's Configs — today no such difference exists, and on prod no Config is vault-suffixed at all. The label must still equal the controller's vault value for the same board, because the `{vault="…"}` selectors both operands are due to gain (in the controller task's own commit) are only expressible if the two sides agree on the value, and renaming a published label afterwards is the expensive repair.

   ⚠️ **The selector is not sound until the union is scoped, and this spec does not make it sound.** Under a namespace-wide union a vault-suffixed Config contributes its pairs to every executor's series, so a `{vault="personal"}` selector would treat a `personal` record in such a pair as accepted. Today that is latent — dev's `egress-probe-trading` and `egress-probe-sentry` declare the same `probe`/`execution` pair as the plain `egress-probe`, and prod has no suffixed Config at all — but it is the reason the selector must not be added on the strength of this label alone. Scoping the union is a separate decision with its own empty-side hazard; it is not smuggled in here.

## Constraints

- **It is a gauge, declared as `metrics.ConfigAccepted`, and the name carries no `_total`.** The value is current state, not a count. `prometheus.NewGaugeVec` with `Name: "executor_config_accepted"` and a non-empty `Help` describing the accepted set and its join purpose. The Go identifier is pinned because an AC greps for it; a differently-named variable makes that grep vacuous.
- **The gauge is legitimately absent before the first pass, and that is not to be fixed by a pre-seed.** A `GaugeVec` with zero label values registers no family at all, so the metric does not appear between pod start and the first successful refresh. That is accepted: the first pass runs at startup and the consumer's `for: 1h` absorbs the gap. Do not invent a startup placeholder series — a fabricated pair would assert an acceptance no Config declared, which is the one thing this metric must never do.
- **Seed the refresh counter's closed set in `init()`.** `executor_config_accepted_refresh_total{result}` is a closed two-value set (`ok`, `read_error`), and `pkg/metrics` already seeds every such set (`initClaimSchemeSeries` and friends) because a `CounterVec` with no series registers no family at all. Without the seed, the wedged-refresh detection in Failure Modes cannot tell "never refreshed" from "nothing changed" — one is a fault, the other is quiet.
- **The label names are frozen by the consumer.** `phase`, `task_type` and `vault` — spelled exactly as the controller gauge spells them. `unless on (phase, task_type)` joins only on shared label names, and the alert's `{vault="…"}` selector matches only a label of that name; renaming any of the three breaks a rule that is already merged and already applied on dev.
- **`vault` comes from the task vault, never from `VAULT_NAME` alone.** See Desired Behavior 6. The value is passed in by the caller that already constructs the vault matcher; the metrics package must not read the environment.
- **Never `Reset()` the gauge.** `Reset()` followed by `Set()` leaves an interval in which the family is empty, and a 15 s scrape can land inside it. Under `unless`, an empty right side returns the whole left side, so that interval is a false all-records alert — the precise failure this metric exists to prevent. Delete the removed label values instead.
- **Never clear on a read error.** Same mechanism, different trigger. A transient API error must not blank the accepted side.
- **The refresh runs before the pass's early returns.** A view that is not ready must not stop the accepted set from being published, because the two are independent facts and the consumer needs the second one regardless of the first.
- **Reuse the existing resolution functions; do not re-implement them.** `effectiveTriggerPhases` (and its `defaultTriggerPhases` fallback) and `EffectiveTaskTypes` are the definitions of "accepted". A second copy that drifts from them would publish a set that disagrees with what the executor actually spawns, which is worse than publishing nothing. If the phase helper must move or be exported to be reachable from the refresh, move it — do not fork it.
- **The union is over Configs, so one Config's removal cannot remove a pair another still accepts.** This is the property that makes a "remove a task type" experiment observable only for a type accepted exactly once; the metric must not narrow per Config.
- **Bounded work per pass.** One `provider.Get(ctx)` per pass, no per-Config API call, and the whole refresh runs under the existing pass context so a cancelled pass does not leave a half-published set.
- **Errors wrap.** Use the repository's error conventions; no `fmt.Errorf`, no bare `return err` on a new path.
- **Tests are Ginkgo `DescribeTable` / `Entry`.** Package suites call bare `RunSpecs`; the suite timeout is configured once for the root package in `main_test.go`, not per package.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection |
|---|---|---|---|
| `provider.Get` returns an error | nothing published, nothing deleted, `refresh_total{result="read_error"}` increments, pass continues normally | next pass retries; the previous set stays visible and the consumer's join keeps working | `read_error` rising while the series count is flat |
| Config list is empty (all Configs removed) | the new set is empty, so every previously published pair is deleted — an accurate reading, not an error | re-apply the chart; the next pass repopulates | series count at 0 with `result="ok"` incrementing |
| A Config declares no task types | it contributes no pairs; the refresh does not fail and does not emit an empty-string pair | none needed — a Config with no types spawns nothing either | the Config's assignee absent from every pair it would otherwise claim |
| A Config's trigger is absent or empty | the phase half falls back to `defaultTriggerPhases`, matching what the spawn path actually does | none needed | a Config whose pairs include `planning`/`execution`/`ai_review` without declaring them |
| Reconcile pass returns early (`view_not_ready`, unstamped, mismatched) | the set is still published, because the refresh runs before those returns | none needed | series present while `executor_reconcile_passes_total{result="view_not_ready"}` is rising |
| Two Configs accept the same pair | one series, value `1`, not two and not `2` | none needed | series count equal to the distinct-pair count, not the sum of cross-products |
| Refresh stops running (loop wedged) | the series freeze at their last values rather than disappearing | restart the executor; confirm `refresh_total{result="ok"}` advances | `refresh_total{result="ok"}` flat while `executor_reconcile_passes_total` advances |
| Metric package reads `VAULT_NAME` directly | the `personal` board emits `shared` while the controller reports `personal`, so the two gauges disagree about which vault a series describes and the selectors both operands are due to gain would suppress genuine defects | pass the task vault in as a constructor argument | the dev `personal` executor's series carry `vault="shared"` |
| Refresh interrupted between the sets and the deletes | survivors intact and the removed pairs may persist for one interval — a superset, never a subset, so no accepted pair is ever missing | next pass converges; no operator action | the `pairs` and `deleted` values in the next summary line |
| The Config CRD gains a phase or task-type value the controller's gauge does not emit | extra series that can never match, so the join is unaffected; the `pairs` count in the summary line jumps, which is the tell | none needed — confirm the controller's vocabulary before relying on the new pair | `pairs` rising without a matching Config change |

## Security / Abuse

- The metric publishes Config *routing* metadata only: phase names, task-type names and a vault slug. It publishes no task content, no assignee identity beyond the already-public task-type vocabulary, and no Config name or namespace.
- The label set is closed and enumerated from Config fields, so an operator cannot turn it into an unbounded cardinality sink by naming tasks — there is no per-record label.
- The refresh reads Configs through the `k8s.Provider[agentv1.Config]` the executor already constructs for its resolver, passed into the handler as described in Desired Behavior 1. It introduces no new credential, no new API scope and no new endpoint.
- The refresh performs no writes to the cluster. A failed read degrades to "publish nothing", never to a partial or fabricated set.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | The gauge and the refresh, split by package: declare the `GaugeVec` as `metrics.ConfigAccepted` in `pkg/metrics`; compute the union over Configs and publish from `pkg/handler`, which already imports `pkg/metrics` — the compute cannot live in `pkg/metrics`, where `effectiveTriggerPhases` is unreachable and the import would cycle. Publish set-then-delete (never `Reset()`), call it at the top of `taskEventHandler.ReconcileOnce` before every early return, pass the `k8s.Provider[agentv1.Config]` into the handler, emit the summary log line, and correct the stale `defaultTriggerPhases` value in `docs/agent-crd-specification.md` (it reads `in_progress`; the code says `execution`) | 1, 2, 3, 4 | 2, 3, 6 (partial 1 — the `vault` label arrives with prompt 2) | — |
| 2 | Failure isolation, the vault label and the counter seed: the `provider.Get` error path publishes and deletes nothing, `executor_config_accepted_refresh_total{result}` carries the closed `ok`/`read_error` set seeded in `init()`, and the `vault` value is injected from the task vault — never read from the environment inside `pkg/metrics` | 5, 6 | 4, 5 | prompt 1 |
| 3 | Tests and the contract doc: `DescribeTable` fixtures for the union, the single-series dedupe, the removal-with-survivors assertion, the read-error no-op, the two vault-label resolutions, the seeded counter at zero, and the early-return pass still publishing — each fixture starting from an emptied family. Plus `docs/executor-metrics-contract.md`, recording the right-hand half of the cross-repo join the way the controller half recorded its own: metric name, the three labels in declaration order, level-not-counter, never-spuriously-empty, and what the `vault` label does and does not scope | — | 1, 2, 3, 4, 5, 6 | prompts 1, 2 |

ACs 7 and 8 are post-deploy operator verification (Rung-2 dev, Rung-3 prod). No prompt covers them deliberately: they run on the host after the merge, through the task's verification ladder, not inside the YOLO container.

## Do-Nothing Option

`executor_config_accepted` has no producer, so `unless on (phase, task_type)` has an empty right side and returns every open record. The alert that is already merged and already applied on dev stops being a defect detector and becomes a permanent all-records page the moment the controller gauge lands — and it must land second, so this is the next thing that happens rather than a hypothetical. Recovering from that means either reverting the controller gauge or silencing a rule that is correct, and a silenced rule is how the original defect would survive a second time. Every open record also stays unjudgeable in the meantime: there is no accepted side to compare against, so "this record sits in a pair nobody accepts" cannot be answered from the metrics at all.
