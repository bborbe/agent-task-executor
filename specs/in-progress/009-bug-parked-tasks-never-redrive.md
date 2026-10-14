---
status: generating
approved: "2026-10-14T21:33:38Z"
generating: "2026-10-14T21:34:16Z"
branch: dark-factory/bug-parked-tasks-never-redrive
---

## Summary

- An agent that cannot finish a task clears its own `assignee` before parking it. The task stays `status: in_progress` and keeps its `previous_assignee`, so it is not terminal — but it is also no longer routable.
- Every path back into the spawn filter tests `assignee`, so an empty one is skipped by construction: the executor's reconcile floor (`reconcileTask`) and the controller's `RedriveSweep` both exclude it.
- Nothing re-emits the task either: `TaskUpdated` is published only when a task file's content hash changes, and a parked file does not change.
- The park is therefore permanent. The only lever today is a human writing the assignee back by hand.
- Observed live on 2026-10-14: 20 Go repositories sat on `go 1.27.1` with an already-fixed gate and no path back into the queue; a human re-drove 46 parked tasks by hand to unblock them.

## Problem

A task parked by its own agent is indistinguishable, from the outside, from a task nobody has looked at yet: it is `in_progress`, it sits at a trigger phase, and it has a file on disk. Nothing about it says "this was tried and failed". Because every automated re-drive gate reads `assignee`, the fleet has no mechanism that recovers such a task — the backlog grows monotonically and is only cleared when a human notices and writes each task back. On 2026-10-14 that was 46 hand writes across 20 repositories, and the same class had already been hand-driven once before (84 tasks) with no durable mechanism landing in between.

## Goal

A task whose assignee was cleared re-enters the spawn filter on its own, with no vault write by an operator and no restart of any controller, and the routing value it receives is one the configuration actually accepts for that task's type.

## Reproduction

Measured on prod (`nuke-prod`) on 2026-10-14, executor image `docker.prod.nuke.benjamin-borbe.de:443/bborbe/agent-task-executor:v0.18.9`.

1. A task exists whose agent cleared its own assignee:

   ```
   $ cd ~/Documents/Obsidian/OpenClaw/tasks
   $ grep -E '^(status|phase|assignee|previous_assignee|trigger_count|current_job):' \
       "Update Go bborbe-git-sync 6e5ff79.md"
   status: in_progress
   phase: planning
   assignee: ""
   previous_assignee: github-update-go-agent
   trigger_count: 0
   current_job: ""
   ```

   `status: in_progress`, `phase: planning`, no `current_job` — it passes every gate except the assignee one.

2. Both re-drive paths exclude it. In `pkg/handler/task_event_handler.go`, `reconcileTask` (line 1049):

   ```go
   if task.Frontmatter.Assignee() == "" {
       return task, nil, false, nil
   }
   ```

   and its `resolveConfig` helper (line 410), which returns no Config at all for such a task:

   ```go
   if task.Frontmatter.Assignee() == "" {
       return nil, false, nil
   }
   ```

   The controller's `pkg/redrive/redrive_sweep.go` carries the same shape:

   ```go
   if task.Frontmatter.Assignee() == "" {
       return false
   }
   ```

3. The reconcile floor reports evaluating almost nothing. Prod executor log, 2026-10-14T20:11Z:

   ```
   I1014 20:11:22.526719  event=reconcile evaluated=3 re_driven=0 deferred=3 skipped=6618 glob="25 Tasks/*.md"
   ```

   `skipped=6618` against `evaluated=3`: the vault holds thousands of task files and the floor admits three.

4. No event arrives to rescue it. The controller's scanner publishes only on a content-hash change, and the parked file's hash is stable, so no `TaskUpdated` is emitted for it. The watcher that produced the task declines to emit a replacement: prod `github-update-go-watcher` skips 25 repositories with `reason=sha_unchanged` (824 such skips in a 6-hour window).

5. Consequence, measured: of those 25 `sha_unchanged` repositories, 22 were still on `go 1.27.1`, and all 25 held live `in_progress` tasks — so the tasks existed, were eligible in every other respect, and were never picked up.

6. The working lever is a hand write. After `vault-cli task set "<task>" assignee github-update-go-agent` for 46 tasks across 20 repositories, and after obsidian-git pushed them, repositories began converting: `github-vuln-watcher`, `git-sync` and `auth-http-proxy` reached `go 1.27.2` within the hour.

## Expected vs Actual

**Expected.** A task at a trigger `status` and `phase`, with no job in flight, is re-driven by the reconcile floor regardless of whether its `assignee` is currently populated — the floor's purpose is to recover tasks that would otherwise be stranded. Documented in the floor's own contract (`pkg/handler/task_event_handler.go`, `ReconcileOnce` doc comment: *"re-drives every eligible task through the existing spawn path"*) and in the operator runbook `Agent - Re-Drive Parked Tasks`, which states that *"a task that was filtered out at dispatch time (… empty `assignee` …) is skipped, emits no new event, and **stays parked forever**"* and that *"Fixing the root cause is not enough — already-parked tasks need an explicit nudge"*.

**Actual.** The floor admits three tasks out of 6,621 and re-drives none. The runbook's remedy is a manual `vault-cli task set` per task, executed 84 times on 2026-10-14 and a further 46 times the same day.

## Why this is a bug

The empty-assignee skip exists to stop the executor spawning an agent for a task that names no agent — a correct guard against `skipped_unknown_assignee` churn. But it is applied on the *recovery* path, where the task's own `task_type` is still present and still identifies exactly one agent. The guard's precondition (no routing information) is false there: `task_type: github-update-go` resolves to a Config whose `spec.taskTypes` lists it. The guard therefore rejects tasks the system has enough information to route, and does so on the one path whose stated purpose is to recover them.

## Assumptions

- The Config store is readable during a reconcile pass. When it is not, the pass skips and the next tick retries — the existing behaviour, unchanged by this fix.
- A `task_type` maps to exactly one Config. Where it maps to more than one, no write is published and the ambiguity surfaces as a skip (see Failure Modes).
- The command path the executor already uses for its other frontmatter writes is applied by the controller, and the resulting hash change publishes `TaskUpdated` — the same mechanism `PublishSpawnNotification` relies on today.
- `previous_assignee` is never a routing source. It records what a task carried before it was parked, not what it should carry next.
- `task_type` is present on every task this fix targets. A task with neither `assignee` nor `task_type` carries no routing information and stays parked.

## Acceptance Criteria

- [ ] A parked task is re-driven with no vault write by an operator. Evidence: a test that lists one task file with `assignee: ""`, `status: in_progress`, a trigger `phase`, no `current_job`, and a `task_type` the Config store resolves, then asserts the reconcile pass publishes exactly one frontmatter update naming that task.
- [ ] The assignee written is the one the resolved Config declares, not the task's `previous_assignee`. Evidence: a test whose fixture carries `previous_assignee: stale-agent` and a Config resolving its `task_type` to `assignee: correct-agent`, asserting the published update's value is `correct-agent` and that `stale-agent` appears in no published command.
- [ ] A task whose `task_type` resolves to no Config is not written. Evidence: negative — a test with a fixture whose `task_type` matches no Config, asserting zero commands are published for that task's identifier.
- [ ] A task that already carries an assignee is not rewritten. Evidence: negative — a test whose fixture carries `assignee: some-agent`, asserting no frontmatter update for `assignee` is published for it.
- [ ] A terminal task is not re-driven. Evidence: negative — fixtures at `status: completed` and `status: aborted`, asserting no command is published for either.
- [ ] A task at a phase outside its Config's trigger set is not re-driven. Evidence: negative — a fixture at `phase: human_review` with a Config whose `trigger.phases` omits it, asserting no command is published.
- [ ] The re-drive writes no counter key. Evidence: a test asserting the published update's `Updates` map contains no `trigger_count` and no `trigger_scope` key, and that the existing trigger-cap specs still pass under `make test`.
- [ ] **Post-Deploy (Rung-3):** in prod, a task parked with an empty assignee is re-driven without any vault edit, and its routing value is restored. Evidence: `kubectlnukeprod -n prod logs <executor-pod> --since=2h | grep -E 'event=reconcile_redrive'` returns ≥1 line naming a task that was parked before the deploy, and `grep -m1 '^assignee:'` on that task's file returns a non-empty value.
  - `deploy_check:` `kubectlnukeprod -n prod get pods -l app=agent-task-executor -o jsonpath='{.items[0].spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` to be filled with the exact tag at prompt time when `EXECUTOR_VERSION` is bumped

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — lint / vet / test clean
- `make test` — the executor's Ginkgo suite passes, including the new reconcile specs
- `grep -n 'skipped_assignee' pkg/handler/task_event_handler.go` — the consumer path's guard is still present, returning a line ≥ 1

### Operator-executable (runs on the host after PR merge, spec verification ladder)

- `make build` — a fresh image builds
- `kubectlnukeprod -n prod get pods -l app=agent-task-executor -o jsonpath='{.items[0].spec.containers[0].image}'` — reports the newly released tag
- `kubectlnukeprod -n prod logs <executor-pod> --since=2h | grep -E 'event=reconcile_redrive'` — returns ≥ 1 line for a task parked before the deploy

## Desired Behavior

1. A reconcile pass treats a task as eligible when its `assignee` is empty but its `task_type` resolves to a Config — provided the task is at a trigger `status` and `phase` for that Config, its `stage` matches the executor's branch, and it has no `current_job`.
2. The re-drive writes the resolved Config's `spec.assignee` onto the task, and writes no other frontmatter key.
3. The write reaches the vault through the same command path every other executor-originated frontmatter write uses, so the controller applies it and the resulting hash change publishes `TaskUpdated` for the task — which is what the normal spawn path consumes.
4. A task whose `task_type` resolves to no Config is left untouched, and its skip is attributed to the existing unknown-assignee counter rather than to a new one.
5. A task that already carries an assignee, a terminal task, and a task outside its Config's trigger set are all unaffected — the change adds a case, it does not widen an existing one.
6. The published value is idempotent: a second pass over a task whose assignee was already restored publishes nothing for it.

## Constraints

- The re-drive must not resolve the assignee from the task's `previous_assignee`. That value is a history, not a routing decision — the operator runbook records a full night lost to exactly this substitution (2026-09-13, two prod probes written to an agent whose effective `task_type` set excludes the task's type, sitting with a `## Failure` for hours). `pkg/result_publisher.go` writes that key on the failure path, so it is present on every parked task.
- The Config's `taskType` / `taskTypes` semantics are documented in `docs/agent-crd-specification.md`, whose own line reads *"Filtering on this list is enforced by a follow-up spec."* This spec is that follow-up; the doc stays the reference for the field contract.
- The controller's `RedriveSweep` is **out of scope**. It carries the same empty-assignee guard, but the executor's reconcile floor is the source-of-truth recovery path (spec 005) and re-drives independently of the controller, so fixing the floor alone closes the stranding. The sweep's guard is left as it is.
- The trigger budget (`trigger_count` against the Config's `max_triggers`) is out of scope and must not change behaviour. A capped task stays capped.
- `parseAndFilter` — the Kafka-consumer path — keeps its empty-assignee skip. This spec changes the recovery path only.
- The Config's own trigger set stays authoritative, as `reconcileTask` already implements it via `effectiveTriggerPhases` / `effectiveTriggerStatuses`.
- No new Config field, environment variable, CLI flag or CRD field.

## Failure Modes

| Trigger | Expected behavior | Detection | Recovery |
|---|---|---|---|
| Config store unreadable during a pass | The pass logs and skips; no task is written | `event=reconcile_skip reason=resolve_error` names the path | Next tick retries; the executor restarts only if the store stays unreadable across ticks |
| A `task_type` maps to more than one Config | No write is published for that task | `event=reconcile_skip` naming the task and the ambiguous `task_type` | Operator narrows the Config's `taskTypes`; the task re-drives on the next pass |
| Two executor replicas reconcile simultaneously | The existing per-assignee spawn lock serializes them; the write is idempotent | Not reachable at `replicas: 1`; a second replica is visible as a second `event=reconcile` line per tick | Roll back to one replica; scaling requires the lease named in the lock's doc comment |
| A re-driven task fails its gate again and re-parks | The task returns to the parked state and is re-driven on the next pass | The task's `assignee` reads empty again after a pass that restored it | The gate itself is the fix; no additional retry budget is added, and the existing trigger cap bounds the churn |
| The controller is down when the write is published | The command expires unapplied | No `TaskUpdated` for the task, and the task's `assignee` still reads empty | Re-published on the next reconcile pass |

## Suggested Decomposition

Prompts should be generated in this order — each row is a single prompt with a clear scope.

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Config resolution by `task_type`, with tests | 1, 4 | 2, 3 | — |
| 2 | Reconcile-floor eligibility + assignee write, with tests | 2, 3, 5, 6 | 1, 4, 5, 6, 7 | prompt 1 |

Rationale: prompt 1 establishes the resolution contract in isolation and is independently testable; prompt 2 consumes it. The Post-Deploy AC is verified by the spec-verifier after the merge, not by a prompt.
