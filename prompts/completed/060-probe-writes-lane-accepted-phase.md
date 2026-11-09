---
status: completed
summary: The liveness probe now writes, per Config, a phase the Config's effective trigger accepts and the executor runs (planning preferred, never todo/human_review/done), replacing the legacy in_progress value that Phase() normalised to execution; [planning]-only lanes are now probed.
execution_id: agent-task-executor-probe-lane-phase-exec-060-probe-writes-lane-accepted-phase
dark-factory-version: v0.198.17
created: "2026-11-08T20:23:07Z"
queued: "2026-11-08T20:23:07Z"
started: "2026-11-08T20:40:10Z"
completed: "2026-11-08T20:49:43Z"
---

# Probe writes a phase the target lane's trigger accepts

<summary>
- The scheduled liveness probe currently stamps every probe task with the status value `in_progress` in its phase field
- The executor reads that legacy value as `execution`, so lanes whose trigger accepts only `planning` skip every probe and their liveness is never measured
- After this change each probe carries a phase taken from its own lane's trigger list, preferring `planning`, otherwise the lane's first phase the executor actually runs
- A lane whose trigger lists no phase the executor would run (only `todo`, `human_review` or `done`) gets no probe at all, so a probe can never land in an operator inbox or sit at a phase that never runs
- Lanes with no trigger configured fall back to the executor's existing default list, which yields `planning`
- The executor's default trigger-phase list lives in one shared place, so the probe and the run gate cannot drift apart
- Lanes that already work (those accepting `execution`) keep receiving probes
</summary>

<objective>
Make the liveness probe write, per Config, a phase that Config's effective trigger accepts and the executor will actually run, so the probe triggers on `[planning]`-only lanes such as `sentry-collector-agent` and `sentry-fix-agent` instead of being skipped by the phase gate, without breaking `[execution]`-only lanes.
</objective>

<context>
Read CLAUDE.md if present for project conventions.
Read `pkg/probe/probe.go` — `healthcheckRunner.Run` loops over every Config from `configProvider.Get` and publishes a `taskcmd.CreateCommand` (frontmatter key `"phase": "in_progress"`) and a `taskcmd.UpdateFrontmatterCommand` (updates key `"phase": "in_progress"`) per Config.
Read `pkg/handler/task_event_handler.go` — `defaultTriggerPhases` (planning, execution, ai_review) and `effectiveTriggerPhases(cfg *pkg.AgentConfiguration)`, which returns `cfg.Trigger.Phases` or the default when the Trigger is nil or its `Phases` is empty. The gate skips a task whose phase (parsed via `lib.TaskFrontmatter.Phase()`, which normalises the legacy `in_progress` to `execution`) is not in that list.
Read `pkg/handler/task_event_handler_phases.go` — `terminalPhases` (human_review, done) and `IsTerminal`; terminal phases are never spawned.
Read `pkg/handler/config_accepted.go` — another caller of `effectiveTriggerPhases`, built from `agentv1.Config.Spec.Trigger`.
Read `pkg/task_type_filter.go` and `pkg/task_type_filter_test.go` — `EffectiveTaskTypes`, the existing example of a shared "effective" helper in package `pkg`; follow its placement, style and test layout.
Read `k8s/apis/agent.benjamin-borbe.de/v1/types.go` — `Trigger` (field `Phases domain.TaskPhases`) and `ConfigSpec.Trigger *Trigger`.
Read `docs/agent-crd-specification.md` § spec.trigger — all six phases (`todo, planning, execution, ai_review, human_review, done`) are valid `trigger.phases` values, so lanes listing only non-runnable phases can exist.
Phase constants come from `github.com/bborbe/vault-cli/pkg/domain` (`domain.TaskPhaseTodo`, `domain.TaskPhasePlanning`, `domain.TaskPhaseExecution`, `domain.TaskPhaseAIReview`, `domain.TaskPhaseHumanReview`, `domain.TaskPhaseDone`; pinned at `github.com/bborbe/vault-cli v0.126.3`, file `pkg/domain/task_phase.go`).
Read `pkg/probe/probe_test.go` — Ginkgo/Gomega suite using `mocks.FakeConfigProvider` and `mocks.FakeCommandPublisher`; existing cases "create-task frontmatter has phase in_progress" and "update-frontmatter has phase in_progress" assert the old value.
Note: `pkg/handler` imports `pkg/probe`, so `pkg/probe` must NOT import `pkg/handler` (import cycle). `pkg` imports neither, so `pkg/probe` may import `pkg`.
</context>

<requirements>
1. In package `pkg`, new file `pkg/trigger_phases.go` (mirroring `pkg/task_type_filter.go`), add an exported default list `DefaultTriggerPhases domain.TaskPhases` (planning, execution, ai_review — same members and order as today's `defaultTriggerPhases`) and an exported function `EffectiveTriggerPhases(trigger *agentv1.Trigger) domain.TaskPhases` returning the trigger's `Phases` when the trigger is non-nil and `Phases` is non-empty, otherwise `DefaultTriggerPhases`. Add GoDoc on both.
2. In `pkg/handler/task_event_handler.go`, make `defaultTriggerPhases` and `effectiveTriggerPhases` use `pkg.DefaultTriggerPhases` / `pkg.EffectiveTriggerPhases` so there is exactly one definition of the default list. Keep `effectiveTriggerPhases`'s signature and behaviour unchanged (all existing handler tests must pass untouched).
3. In `pkg/probe/probe.go`, add an unexported helper that picks the probe phase for one Config from `pkg.EffectiveTriggerPhases(config.Spec.Trigger)`: return `planning` if the list contains it; otherwise the first entry that is also in `pkg.DefaultTriggerPhases` (planning, execution, ai_review) — this excludes `todo` (a probe at todo would land in the operator approval inbox) and the terminal phases `human_review` and `done` (the gate never spawns them, and a probe at human_review would sit in the operator review inbox); otherwise report "no phase" (a `bool` second return).
4. In `healthcheckRunner.Run`, compute the phase per Config with that helper and write it (as a string) into BOTH the create-command frontmatter `"phase"` and the update-frontmatter `"phase"`. If the helper reports no phase, publish nothing for that Config, log `glog.V(2).Infof("event=probe_skipped assignee=%s reason=no_spawnable_trigger_phase", ...)` (import `github.com/golang/glog`), and continue with the next Config. Leave every other frontmatter key (`status`, `task_type`, `assignee`, `stage`, `trigger_count`, `current_job`, `job_started_at`, `spawn_notification`) unchanged.
5. Update `pkg/probe/probe_test.go`: replace the two `in_progress` phase assertions, and add a `DescribeTable` over Config triggers asserting the `phase` value in BOTH published commands (create-command `Frontmatter` and update-command `Updates`, type-asserted from `PublishArgsForCall` exactly as the existing cases do):
   - `[planning]` → `planning`
   - `[execution]` → `execution`
   - `[execution, planning]` → `planning` (planning preferred regardless of order)
   - `[todo, planning, execution, ai_review]` → `planning`
   - `[todo, execution]` → `execution` (todo skipped)
   - `[ai_review]` → `ai_review`
   - `[human_review, execution]` → `execution` (terminal skipped)
   - Trigger nil → `planning`; Trigger with empty `Phases` → `planning`
   Add a second `DescribeTable`: a Config with `Trigger{Phases: [todo]}` / `[human_review]` / `[done]` plus a second normal Config → exactly 2 Publish calls total (only the normal Config is probed).
6. Boundary test (probe → executor gate) in a new `pkg/handler/probe_phase_gate_internal_test.go` (package `handler`; `pkg/handler` already imports `pkg/probe`, so there is no cycle; use a plain `testing.T` table test like `pkg/handler/concurrency_capacity_internal_test.go`). For every Config trigger in the requirement 5 table that yields a probe (re-list them in this file; the probe_test table is not importable): run `probe.NewHealthcheckRunner` with the `pkg/probe/mocks` fakes, take the create-command `Frontmatter` and the update-command `Updates`, parse each phase exactly as the executor does with `lib.TaskFrontmatter(...).Phase()`, and assert it is non-nil, that `effectiveTriggerPhases(&pkg.AgentConfiguration{Trigger: cfg.Spec.Trigger}).Contains(*phase)` is true, and that `IsTerminal(*phase)` is false. A membership check against the list the helper picked from does NOT satisfy this: the original bug was the string `in_progress` passing the write side and being normalised to `execution` by `Phase()` on the read side.
7. Add `pkg/trigger_phases_test.go` (package `pkg_test`, mirroring `pkg/task_type_filter_test.go`) testing `EffectiveTriggerPhases` for a nil trigger, empty `Phases`, and a non-empty list.
8. Add to `CHANGELOG.md` a new `## Unreleased` section directly above the first `## v` heading, with one bullet: `- fix: the liveness probe writes a phase its target Config's trigger accepts and the executor runs (planning preferred; never todo, human_review or done) instead of the status value in_progress, which the executor read as execution — so [planning]-only lanes such as sentry-collector-agent and sentry-fix-agent were skipped and never probed`.
9. Self-check before finishing: re-run `<verification>` and confirm every command passes; walk each requirement above against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT change the probe task identifier derivation (`probeTaskID`, `probeNamespace`) — the comment there forbids it.
- Do NOT change the `status` value the probe writes; only the `phase` key changes.
- Do NOT change `effectiveTriggerPhases`'s behaviour or any spawn-gate semantics; existing tests must still pass.
- Non-goal: a Config whose `trigger.statuses` excludes `in_progress` is still not probed — out of scope for this prompt.
- Use `github.com/bborbe/errors` for any error wrapping; no `fmt.Errorf`.
- Do NOT run `go mod vendor`; never use `-mod=vendor`.
- Existing tests must still pass.
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit` — must pass.
Then: `! grep -q '"phase":[[:space:]]*"in_progress"' pkg/probe/probe.go` — must succeed (no hard-coded in_progress phase left).
Then: `grep -c '^## Unreleased$' CHANGELOG.md` — must print `1`.
Then: `awk '/^## /{sec=$0} /liveness probe writes a phase/{print sec}' CHANGELOG.md` — must print `## Unreleased`.
</verification>
