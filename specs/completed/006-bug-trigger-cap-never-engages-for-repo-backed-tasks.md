---
status: completed
approved: "2026-09-08T19:14:49Z"
generating: "2026-09-08T19:15:10Z"
prompted: "2026-09-08T19:20:46Z"
verifying: "2026-09-08T19:33:04Z"
completed: "2026-09-08T20:08:57Z"
branch: dark-factory/bug-trigger-cap-never-engages-for-repo-backed-tasks
---

## Summary

- The scoped spawn-trigger cap is **opt-in**: it engages only when the task frontmatter carries `max_triggers`.
- The `github-update-go` watcher emits tasks **without** `max_triggers`, so every repo-backed update task is effectively uncapped — the exact failure mode this cap was built to stop.
- Live reproduction on prod 2026-09-08: task `37f5803e-3955-5b67-d1aa-2ac9ff04ce32` (Update Go bborbe/ip at `314c4b3`) respawned **3 jobs ~9 min apart** in the same `planning` phase + same ref, `trigger_count` climbing to 4, with **no `max_triggers`** — until the executor's escalation machinery cleared `assignee` (a non-terminal park).
- The loop driver: the agent's `needs_input` (which would park the task) carried a fabricated environment claim; the false-claim guard refuted it (the workdir exists) and converted it to `failed` — and `failed` is retryable, back in the spawn allowlist.
- The safe fix is the one the anchor task's design note recorded 2026-08-31: **default the cap on when the task carries a `ref`** — a repo-backed task's scope moves with the repo, so a constant-scope accrual cannot occur; recurring tasks carry no `ref` and stay uncapped (v0.7.1 regression guard holds).

## Problem

The scoped cap merged in #29 fixed the *opt-in* cap's correctness (a scoped budget no longer leaks across phases/commits) but left the engagement gate unchanged: `max_triggers` must be present in the frontmatter. The only producer that needed the cap — the `github-update-go` watcher — never emits it. So a repo whose planning gate deterministically fails with a retryable result respawns unbounded: 26 pods in ~30 min in the 2026-08-30 incident, and a repeat live on prod today. The cap is the defense-in-depth behind the `needs_input` routing, and today's loop proved the routing alone is not enough: when the agent's parking reason is a fabricated environment claim, the guard converts it to `failed` (retryable), and nothing stops the respawn.

## Goal

A repo-backed task (one whose frontmatter carries a `ref`) is capped by default: it may not spawn more than `max_triggers` jobs in the same `phase`+`ref` scope — using the lib default of 3 when the field is absent. A recurring task (no `ref`, constant scope) remains uncapped by default, exactly as today. An explicit `max_triggers` still overrides the default in both cases.

## Reproduction

Live, on prod, 2026-09-08, task `37f5803e-3955-5b67-d1aa-2ac9ff04ce32` (Update Go bborbe/ip at `314c4b3`):

```
# kubectlnukeprod -n prod get jobs | grep 37f5803e
github-update-go-agent-37f5803e-20260908172649   Complete   1/1   9m13s   35m
github-update-go-agent-37f5803e-20260908173639   Complete   1/1   8m54s   25m
github-update-go-agent-37f5803e-20260908174541   Complete   1/1   9m6s    16m
```

Task frontmatter during the loop:

```
status: in_progress
phase: planning
assignee: ""          # cleared only after escalation, a non-terminal park
previous_assignee: github-update-go-agent
trigger_count: 4
trigger_scope: planning:314c4b32
# NO max_triggers field
```

Job log (latest pod, `github-update-go-agent-37f5803e-20260908174541-6bhsq`):

```
I0908 17:54:43.268624  steps_planning.go:674] planning: env-claim check workdir=... stat_exists=true reason="workdir ... is not accessible — all tools are restricted to /agent only ..."
I0908 17:54:43.268638  steps_planning.go:191] planning: env-claim refuted — workdir exists, not clearing assignee: ...
I0908 17:54:43.269120  result-deliverer.go:165] publishing task update for taskID=37f5803e-... status=failed
{"Status":"failed","Message":"needs_input reason claims an environment problem but workdir ... exists on disk — false claim, not clearing assignee: ..."}
```

Dark-factory: `dark-factory --version` — the executor is deployed via the `agent` Helm chart (`docker.prod.nuke.benjamin-borbe.de:443/bborbe/agent-task-executor:v0.13.1`); the bug is reproduced in the executor's unit test suite (spec `recurring task carries no ref, absent max_triggers → uncapped` stays green, which is the contract this spec changes).

## Expected vs Actual

- **Expected** (documented behavior — anchor task `Agent Gate Failure Never Terminal — Unbounded 60s Respawn Loop`, SC #2): *no task spawns more than N jobs for the same `phase`+`ref` without escalating or parking.* The cap survives result write-back.
- **Actual**: a repo-backed task with a deterministically failing planning gate respawned 4× in the same `phase`+`ref` scope with no cap engagement, because the cap requires `max_triggers` and the watcher never emits it.

## Why this is a bug

The cap's own design rationale (recorded in the anchor task's 2026-08-31 design note and in `task_event_handler.go`'s comment) identified the safe default-on form explicitly: *"the safe form of default-on is to gate it on `ref` being present."* The shipped #29 kept the cap opt-in instead, and the consequence — an uncapped repo-backed loop — is exactly what the anchor task's SC #2 and the DoD gate ("verified on prod: a task with a deliberately failing gate parks/aborts after the retry cap instead of respawning past it") require. Today's live loop is the repro that disproves the DoD gate.

## Desired Behavior

1. A repo-backed task (frontmatter carries `ref`) engages the scoped spawn-trigger cap even without an explicit `max_triggers`, using the lib default of 3.
2. A repo-backed task at `trigger_count >= 3` in an unchanged `phase`+`ref` scope is skipped — no new Job is spawned for that spawn decision.
3. A repo-backed task below the cap still spawns and records the trigger (increment or scope write, per existing semantics).
4. A task with an explicit `max_triggers` is capped exactly as before, with or without a `ref`.
5. A task with neither `ref` nor `max_triggers` (the recurring-task shape) is uncapped exactly as before — the v0.7.1 regression guard holds.
6. A changed `phase` or `ref` still resets the budget, so a repo that advances to a new commit earns a fresh retry.

## Constraints

- Repo conventions are frozen: Ginkgo/Gomega v2 tests, `github.com/bborbe/errors` wrapping (never `fmt.Errorf`), counterfeiter mocks for new dependencies (`//counterfeiter:generate`), glog `V(n)` gating.
- The recurring-task regression guard MUST hold: a task with **no `ref` and no `max_triggers`** (the v0.7.1 / 2026-08-27 Daily Sentry Triage shape) stays uncapped — its scope is a constant and a default cap would strip `assignee` and kill the re-dispatch loop. The existing spec `does not skip spawn when max_triggers is absent (recurring task)` (`test-task-cap-absent`, trigger_count 5) keeps asserting `SpawnJobCallCount() == 1`.
- A task that carries `ref` but also an explicit `max_triggers` keeps the explicit value (the lib `MaxTriggers()` returns the field value; default 3 only when absent — `github.com/bborbe/agent v0.86.0` `agent_task-frontmatter.go`).
- Scope-change semantics unchanged: a changed `phase` or `ref` still resets the budget (fresh count 1), so a repo that moves to a new commit earns a retry.
- Scope adoption semantics unchanged: an absent `trigger_scope` adopts the current scope carrying the count forward — never a reset.
- The default-engage gate is `ref` presence via `Frontmatter.String("ref")` — the same key `triggerScope` already uses to build `<phase>:<ref[:8]>`. The explicit `max_triggers` opt-in path is **retained**: the cap engages when `ref` is present OR `max_triggers` is present. A task with no `ref` scopes on phase alone (constant) and stays uncapped **only when `max_triggers` is also absent** — an explicit `max_triggers` without a `ref` (the existing `test-task-cap-1` shape) must still cap.
- CHANGELOG: add an `## Unreleased` bullet for this fix (section currently absent — HEAD is `## v0.13.1`).

## Acceptance Criteria

- [ ] A repo-backed task (frontmatter has `ref`, no `max_triggers`) at `trigger_count >= 3` in an unchanged scope is skipped (no spawn) — evidence: new Ginkgo spec in `pkg/handler/task_event_handler_test.go` with frontmatter `ref`, `trigger_count: 3`, no `max_triggers`, asserts `SpawnJobCallCount() == 0` and `PublishIncrementTriggerCountCallCount() == 0` (test exit 0)
- [ ] A repo-backed task below the default cap still spawns and increments — evidence: new Ginkgo spec with `ref`, `trigger_count: 2`, no `max_triggers`, asserts `SpawnJobCallCount() == 1` (test exit 0)
- [ ] The recurring-task regression guard is unchanged — evidence: the existing `does not skip spawn when max_triggers is absent (recurring task)` spec (no `ref`, no `max_triggers`, `trigger_count: 5`) still asserts `SpawnJobCallCount() == 1` (test exit 0)
- [ ] An explicit `max_triggers` still caps — evidence: the existing `skips spawn when trigger_count >= max_triggers (cap reached)` spec (`max_triggers: 3`, `trigger_count: 3`) still asserts `SpawnJobCallCount() == 0` (test exit 0)
- [ ] Scope change still resets the budget for a repo-backed task — evidence: the existing `resets the budget when the ref changes` spec still asserts a scope-reset write (test exit 0)
- [ ] The engagement-gate comment in `applyTriggerBudget` is updated to state that `ref` presence default-engages the cap (no stale "stays OPT-IN" claim) — evidence: `grep -nE 'String\\("ref"\\)|default-engage|repo-backed' pkg/handler/task_event_handler.go` returns ≥1 line stating the default-engage rule, and `grep -c 'absent max_triggers still means no cap' pkg/handler/task_event_handler.go` returns 0

## Verification

### Container-executable (runs inside the dark-factory YOLO container at prompt time)

- `make precommit` — exits 0
- `go test ./pkg/...` — exits 0; the new and existing cap specs above are green

### Operator-executable (runs on the host after PR merge, spec verification ladder)

- `git show --stat HEAD | grep -c 'task_event_handler.go'` — ≥1 (the gate change landed in the handler)
- `gh pr diff <n> | grep -cE 'String\\("ref"\\)|Frontmatter\\["ref"\\]'` — ≥1 (the gate reads the `ref` key)
- **Seam-adequacy note (why no live-cluster replay here):** the bug is a pure spawn-decision gate condition inside `applyTriggerBudget` — no external system (cluster, Kafka, vault) participates in the decision, so the unit specs at that seam exercise the full behavior (skip / increment / scope-write per frontmatter input). The bug-workflow "replay the reproduction" mandate applies to runtime-symptom bugs whose defect crosses an external boundary; this one does not. The anchor task's DoD gate (re-observe a repo-backed loop parked on prod after deploy) is the anchor task's closure step, not this spec's.

## Failure Modes

| Trigger | Expected behavior | Recovery |
|---|---|---|
| A repo-backed task legitimately needs >3 spawns in one `phase`+`ref` (e.g. long-running multi-attempt update) | Cap fires at the lib default 3 | Operator sets explicit `max_triggers` higher on the task; scope change (new ref) also resets the budget |
| `ref` present but task is actually recurring in disguise (constant ref) | Cap engages at 3 — same as any repo-backed task at a fixed ref | Correct per SC #2; scope never moves means the repo never advanced, so parking is the right outcome |
| A repo-backed task with no `ref` (watcher omits it) | Stays uncapped (gate is `ref` presence) | The `github-update-go` watcher always emits `ref` today (verified on live tasks); if a producer stops emitting it, this cap silently downgrades — flagged in the anchor task as the watcher-side invariant |
