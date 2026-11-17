---
status: completed
spec: [020-executor-config-accepted]
summary: Proved the accepted-set metric contract with metrics/handler fixtures, documented the cross-repo join, and merged the two changelog bullets into one.
execution_id: agent-task-executor-config-accepted-exec-055-spec-020-tests-and-contract
dark-factory-version: v0.198.7
created: "2026-11-01T18:24:00Z"
queued: "2026-11-01T20:36:40Z"
started: "2026-11-01T21:00:31Z"
completed: "2026-11-01T21:11:26Z"
branch: dark-factory/executor-config-accepted
---

# Prove the accepted-set metric with fixtures and record its contract

<summary>
- The gathered family carries exactly the three join labels and the value 1
- Two Configs' pairs union into one set, a pair declared twice appears once, and a removed type removes exactly its pairs while every survivor still reads 1
- A failed Config read leaves the gathered set byte-identical and moves only the counter
- A cancelled pass counts as a read error and publishes nothing
- The vault label is the injected task vault, not the Config-resolution vault
- A pass that aborts on view-not-ready still publishes the set and logs one summary line
- A handler whose pass never runs publishes nothing and logs nothing
- Both refresh counter results exist at zero before the first pass
- The metric contract is documented for the controller-side reader
- The changelog records the new metric
</summary>

<objective>
Pin the accepted-set metric's observable contract with fixtures that would fail a `Reset()`-then-set implementation, a read-error clear, a per-Config narrowing or an environment-read vault, and record the cross-repo join contract as a durable document.
</objective>

<context>
This prompt builds on prompts 1 and 2, which must already be in the tree. Read them first:

- `pkg/metrics/metrics.go` — `ConfigAccepted` (`GaugeVec`, labels `phase`, `task_type`, `vault`), `ConfigAcceptedRefreshTotal` (`CounterVec`, `result` ∈ {`ok`, `read_error`}), and their `init()` seeding.
- `pkg/handler/config_accepted.go` — `refreshConfigAccepted`, `configAcceptedPairs`, and the `event=config_accepted_refreshed configs=%d pairs=%d deleted=%d` summary line.
- `pkg/handler/task_event_handler.go` — `ReconcileOnce` calls the refresh before the `view.Ready()` check.

Test precedents to copy, not reinvent:

- `pkg/handler/task_event_handler_test.go` — `TestHandler` / `RunSpecs`, `testTaskVault`, and the exact `handler.NewTaskEventHandler(...)` argument list this suite uses.
- `pkg/handler/task_view_test.go` — `captureStderr(fn func()) string`, which redirects glog's stderr into a pipe. The summary line is a plain `glog.Infof`, so it is captured with `logtostderr` alone; no `v` level is needed.
- `pkg/handler/task_reconcile_test.go` — the `mocks.FakeTaskView` / `mocks.FakeConfigResolver` / `pkg.NewTaskStore()` / `libtime.NewCurrentDateTime()` construction shape and `testutil.ToFloat64` delta assertions.
- `pkg/spawner/service_reconcile_loop_test.go` — `fakeProviderImpl`, the local stub shape for a `k8s.Provider[agentv1.Config]`.
- `pkg/metrics/metrics_test.go` — `gatherLabels(mfs, name, labelName)` and the `prometheus.DefaultGatherer.Gather()` pattern.
- `docs/vault-api-task-index.md` § *Metric contract* — the document voice and table shape for the new contract doc.

Coding docs:
`/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`,
`/home/node/.claude/plugins/marketplaces/coding/docs/go-prometheus-metrics-guide.md`,
`/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md`.
</context>

<requirements>
1. **`pkg/metrics/metrics_test.go` — three specs** (package `metrics_test`, existing `TestMetrics` suite; do not add a suite file):

   a. `It("registers executor_config_accepted with exactly the three join labels and value 1")`. The family does not exist until a series is set, so first set one sample — `metrics.ConfigAccepted.WithLabelValues("execution", "github-update-go", "cfgtest").Set(1)` — and `defer metrics.ConfigAccepted.DeleteLabelValues("execution", "github-update-go", "cfgtest")`. Then `prometheus.DefaultGatherer.Gather()`, find the `executor_config_accepted` family, and assert: `mf.GetHelp()` is non-empty; the label-name set of its single sample is `ConsistOf("phase", "task_type", "vault")`; the sample's `GetGauge().GetValue()` is `1`.

   b. `It("registers no executor_config_accepted_total family")` — after setting and clearing a sample as above, build the name map from `Gather()` and assert `Expect(names).NotTo(HaveKey("executor_config_accepted_total"))`.

   c. `It("pre-initialises both accepted-refresh results at zero")` — `gatherLabels(mfs, "executor_config_accepted_refresh_total", "result")` is `ConsistOf("ok", "read_error")` (exactly two), and each child series reads `0` via `testutil.ToFloat64(metrics.ConfigAcceptedRefreshTotal.WithLabelValues(result))` before any refresh runs.

2. **`pkg/handler/config_accepted_test.go`** (new file, package `handler_test`, license header). Helpers, all package-level so the specs stay inside the `funlen` 80-line budget:

   ```go
   // acceptedSeries is one gathered executor_config_accepted sample.
   type acceptedSeries struct {
       phase    string
       taskType string
       vault    string
       value    float64
   }

   // acceptedTestProvider is a controllable k8s.Provider[agentv1.Config].
   type acceptedTestProvider struct {
       configs []agentv1.Config
       err     error
   }

   func (p *acceptedTestProvider) Get(_ context.Context) ([]agentv1.Config, error) {
       out := make([]agentv1.Config, len(p.configs))
       copy(out, p.configs)
       return out, p.err
   }
   ```
   Plus `gatherConfigAccepted() []acceptedSeries` (read `prometheus.DefaultGatherer.Gather()`, keep only the `executor_config_accepted` family, read the `phase`/`task_type`/`vault` label values and `GetGauge().GetValue()`), `clearConfigAccepted()` (gather, then `metrics.ConfigAccepted.DeleteLabelValues(phase, taskType, vault)` for every tuple — a delete, never a `Reset()`), and a handler builder:

   ```go
   func newAcceptedTestHandler(
       provider *acceptedTestProvider,
       view handler.TaskView,
       taskVault string,
   ) handler.ClaimLifecycleEventHandler
   ```
   which calls `handler.NewTaskEventHandler(...)` with the same positional arguments the existing suite uses (`mocks.FakeJobSpawner`, `"dev"`, `base.Branch("prod")`, `""`, `true`, `true`, `&mocks.FakeConfigResolver{}`, `&mocks.FakeResultPublisher{}`, `pkg.NewTaskStore()`, `libtime.NewCurrentDateTime()`, `view`, `taskVault`, `handler.DefaultReconcilePassTimeout`, `handler.DefaultReconcileInterval`, `pkg.ClaimModeOff`, `nil`) and appends `handler.WithConfigProvider(provider)`.

   **Isolation rule for every spec in this file:** `clearConfigAccepted()` in a `BeforeEach` (or at the top of the spec), and give each fixture its own `vault` slug (e.g. `cfgtest-a`, `cfgtest-b`, …) so a family-wide assertion cannot be satisfied by an earlier fixture's series. `metrics.ConfigAccepted` is a package-level `promauto` gauge on the default registry, so this is required for a correct implementation, not a workaround.

   Config fixtures are `agentv1.Config` values with `ObjectMeta{Name: …}` and a `Spec{Assignee: …, TaskType/TaskTypes: …, Trigger: …}`. A `Trigger` of `&agentv1.Trigger{Phases: domain.TaskPhases{domain.TaskPhaseExecution}}` gives one phase; a nil `Trigger` gives the three default phases.

3. **Fixtures — `DescribeTable` over provider snapshots** (fixture builders at package level, `Entry` rows inside the table):

   - **entry (a) union:** Config A declares type `github-update-go` with a one-phase trigger; Config B declares types `probe` and `review` with a nil trigger (three phases each). `ReconcileOnce` → the gathered set equals the union of both cross-products (1 + 3 + 3 = 7 series), every value `1`.
   - **entry (b) removal with survivors:** start from the same two Configs, publish, then drop one of B's types from the provider and call `ReconcileOnce` again. Exactly the pairs of that type are gone — the number removed equals the number of phases B triggered (3 for a nil trigger) — and every surviving series still reads `1` in the same gather. This ordering assertion is the point: a delete-then-set implementation that drops pairs fails it.
   - **entry (c) dedupe:** two Configs declaring the same `task_type` with the same trigger produce one series for each pair, not two, and the value is `1` (never `2`).
   - **entry — all Configs gone:** publish from the two Configs, then set the provider's list to empty and call `ReconcileOnce` again. The family for this fixture's vault is empty, the summary line's `deleted` equals the previous pair count, and `read_error` did not move. This is the one legitimate empty — a truthful "no Config accepts anything" reading — and it is what distinguishes the invariant from fixture (d)'s read-error no-op, where the previous set must survive.
   - **entry — Config with no task types:** a Config with `Assignee` set and both `TaskType` and `TaskTypes` empty contributes no series, the pass does not fail, and no series appears carrying an empty `task_type` label.

   Assertions are on the gathered label tuples (`phase`, `task_type`, `vault`), not on counts alone, so a wrong phase or type is caught.

4. **Fixture (d) — the three refresh outcomes are distinguishable.** Publish a set, then for each arm capture `gatherConfigAccepted()` and the current `testutil.ToFloat64(metrics.ConfigAcceptedRefreshTotal.WithLabelValues("ok"))` and `...("read_error")` as baselines.

   - **success arm:** call `ReconcileOnce` against a healthy provider. `ok` increased by exactly `1` and `read_error` did not move. This is the only place in the suite that asserts the success path counts at all — without it an implementation that never increments `ok` passes every other fixture and every grep.
   - **cancelled arm:** build an already-cancelled context (`ctx, cancel := context.WithCancel(context.Background()); cancel()`) and call `ReconcileOnce(ctx)`. `read_error` increased by exactly `1`, `ok` did not move, and the gathered set equals the baseline — the refresh returned before publishing. Do not assert on `ReconcileOnce`'s own return value here; only the metric deltas and the gathered set.
   - **failed-read arm:** set `provider.err = stderrors.New("boom")` (import the standard library as `stderrors "errors"`, the alias this repo uses for a plain sentinel — see `ErrLeaseNotExpired` in `pkg/claim/lease_manager.go`; `github.com/bborbe/errors`'s `New(ctx, msg)` attaches context data, which this fixture does not need), call `ReconcileOnce`, then assert `ReconcileOnce` returned `nil` (the pass continues), `gatherConfigAccepted()` equals the captured snapshot (same tuples, same values — compare with `ConsistOf` over the tuples), and `read_error` increased by exactly `1` while `ok` did not increase.

   Assert deltas, never absolutes: these counters are process-global and never reset.

5. **Fixtures (e) — the vault label is the injected task vault.** With one provider holding one Config declaring `task_type: probe` at `phase: execution`, build two handlers, one with `taskVault` `shared` and one with `personal`, and call `ReconcileOnce` on each — assert the first handler's series, then clear the family, then run the second. Assert the emitted series for the first carries `vault="shared"` and for the second `vault="personal"`. The assertion is on the gathered series, not on `VaultMatcher` — the risk this fixture exists for is the value being dropped between `main` and the label.

6. **Fixtures (f) — the pass publishes, and only the pass publishes.**
   - **early-return pass:** `mocks.FakeTaskView` with `ReadyReturns(false)`, provider holding two Configs with distinct types and nil triggers (6 pairs). Run `captureStderr(func() { Expect(h.ReconcileOnce(ctx)).To(Succeed()) })`. Assert the gathered family for this fixture's vault is non-empty and has 6 series; then match the captured output against `regexp.MustCompile(`event=config_accepted_refreshed configs=(\d+) pairs=(\d+) deleted=(\d+)`)`, require exactly one match, and assert the captured `pairs` value equals the number of series gathered for this vault and `configs` equals `2`. A `configs=0 pairs=0 deleted=0` line beside a non-empty family fails this.
   - **loop never runs:** build a handler with its own vault slug and never call `ReconcileOnce`; wrap the construction in `captureStderr` and assert the output contains no `event=config_accepted_refreshed`, and that `gatherConfigAccepted()` contains no series carrying that vault value.

7. **Document the contract in `docs/executor-metrics-contract.md`** (new file, no license header — Markdown docs in this repo do not carry one). Record the right-hand half of the cross-repo join, in the voice of `docs/vault-api-task-index.md` § *Metric contract*:
   - the join: this side (`executor_config_accepted`) is the **right** operand, subtracted from `agent_controller_open_tasks` (controller) by `unless on (phase, task_type)`, and why an empty right side returns every open record;
   - metric name `executor_config_accepted`, a **gauge**, one series per accepted pair, value `1`;
   - the three labels in declaration order — `phase`, `task_type`, `vault` — and that the names are frozen by the merged rule;
   - the accepted set is the union over all Configs in the namespace of `effectiveTriggerPhases` × `EffectiveTaskTypes`; it is namespace-wide, not vault-scoped;
   - what `vault` does and does **not** mean: it names the publishing instance (its task vault, `TASK_VAULT` falling back to `VAULT_NAME`) and does not scope the pair set; a `{vault="…"}` selector is therefore not sound until the union is scoped, which this metric does not do;
   - never-spuriously-empty: set-then-delete, never `Reset()`, never cleared on a read error; a scrape may see a superset for one interval, never a subset; a genuinely empty accepted set (every Config gone) is a truthful reading and may empty the family;
   - the refresh counter `executor_config_accepted_refresh_total{result}` with the closed set `ok` / `read_error`, both seeded at `0`;
   - cardinality is bounded by Configs × trigger phases × task types; no task id, assignee, Config name or namespace is ever a label.

8. **Changelog.** `CHANGELOG.md` already carries a `## Unreleased` section, and prompts 1 and 2 each appended a `feat:` bullet to it describing part of this feature. **Merge those two bullets into one** `feat:` bullet in the file's existing voice (`- feat: **bold summary.** prose`) describing the metric end to end — do not add a third bullet, and do not rename or touch a released section. The merged entry covers: the gauge name and its three labels, value 1 per accepted pair, the union over Configs, set-then-delete with no `Reset()`, a failed read is a non-event, the refresh runs before the pass's early returns, the closed `ok`/`read_error` refresh counter seeded at zero, and the vault label being the task vault. Follow `changelog-guide.md`.

9. **Self-check before finishing.** Re-run the `<verification>` block and confirm every command passes, then walk spec ACs 1–6 against the fixtures above and confirm each AC has a named fixture that would fail without the implementation.
</requirements>

<constraints>
- Tests are Ginkgo `DescribeTable` / `Entry`. Package suites call bare `RunSpecs`; do not add a new suite file or a per-package timeout — the suite is configured once.
- Every fixture starts from an emptied family: `clearConfigAccepted()` (a `DeleteLabelValues` loop) and a unique vault slug per fixture. Never call `Reset()` in a test either.
- Assert deltas, never absolutes, on `ConfigAcceptedRefreshTotal`: it is process-global and never reset.
- Do not weaken a fixture to make it pass. If entry (b)'s survivor assertion or fixture (f)'s `pairs` equality fails, the implementation is wrong, not the test.
- Do not re-test `effectiveTaskVault()` or the env resolution in `main.go`; that is covered by `main_internal_test.go`'s own table. The vault fixtures assert the emitted label only.
- Do not duplicate the family-shape assertions between `pkg/metrics/metrics_test.go` and the handler fixtures: the metrics specs own the family name, help and label set; the handler fixtures own the published tuples.
- The contract doc records the metric as shipped; it must not claim the `vault` label scopes the set, and must not claim the merged alert currently uses a `vault` selector — it does not.
- Add the standard license header to every new Go file.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
`ROOTDIR=/workspace make precommit` — must pass.
`go test -mod=mod -count=1 ./pkg/metrics/ ./pkg/handler/ ./pkg/`
`for s in 'executor_config_accepted' 'phase' 'task_type' 'vault' 'unless on (phase, task_type)' 'DeleteLabelValues'; do grep -q -- "$s" docs/executor-metrics-contract.md || exit 1; done` — the name, the three labels, the join and the never-reset rule are all documented.
`! grep -rq 'ConfigAccepted.Reset()' pkg/` — the gauge is never reset (the scoped form; a bare `Reset()` grep would match unrelated metrics).
`! grep -rq 'executor_config_accepted_total' pkg/` — no such metric name exists.
`grep -q 'executor_config_accepted' CHANGELOG.md` — the unreleased entry exists.
`[ "$(grep -rl RunSpecs pkg/handler/ | wc -l)" -eq 1 ]` — only `task_event_handler_test.go` defines the package suite; no new suite file was added.
</verification>
