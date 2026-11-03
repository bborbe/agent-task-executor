---
status: draft
kind: bug
---

## Summary

- An agent Job that dies on its deadline without ever being placed on a node never counts toward the retry cap, so its task re-queues until a human clears the assignee.
- The cap is armed for exactly one kind of failure — a crashed pod that produced no output — and every other failure leaves the record uncapped.
- The obvious place to fix this (the background sweeper that classifies unscheduled pods) does not work: by the time it looks, the record has already been published and removed.
- The fix arms the cap when the failed pod was never scheduled, and leaves every other failure — including a pod that ran and then hit its deadline, and every recurring task — exactly as it is today.

## Problem

A task whose Job can never be placed on a node burns a full `activeDeadlineSeconds` budget (1800 s by default) per attempt, then re-queues and burns another. Nothing escalates: `crashFailureUpdates` writes `max_triggers` only for `ZombieReasonPodCrashNoStdout`, and the controller's escalation is gated on `TriggerCapEngaged` (defined in `bborbe/agent-task-controller`), which requires either an explicit `max_triggers` or a `ref`. A record with neither — the shape the reported incident has — therefore accrues `trigger_count` without bound. Observed records sit at 9, 41 and 90.

The operator-visible cost is two-fold: cluster slots are consumed by Jobs that can never run, and a genuinely stuck task never surfaces in the inbox as something needing attention, because the only signal the cap produces (a cleared `assignee` plus an escalation) never fires.

## Goal

A Job that dies on its deadline without ever scheduling counts toward the trigger cap. After the third such failure the record carries `max_triggers: 3`, the controller clears the `assignee`, and the task stops re-queuing on its own. A Job that ran and then hit its deadline, and any recurring task, behave exactly as they do today.

## Acceptance Criteria

- [ ] A `deadline_exceeded` failure whose pod never scheduled arms the cap — evidence: a spec at the `PublishFailure` seam whose task carries no `max_triggers`, whose Job condition is `Failed/DeadlineExceeded` and whose pod carries `PodScheduled=False`, asserts the published `UpdateFrontmatterCommand` contains `max_triggers: 3`; `go test ./pkg/...` exits 0
- [ ] A `deadline_exceeded` failure whose pod reached a terminal phase before the deadline does **not** arm — evidence: the same spec shape with a pod in `Succeeded`/`Failed` and no `PodScheduled=False` condition asserts the published `UpdateFrontmatterCommand` contains no `max_triggers` key; `go test ./pkg/...` exits 0
- [ ] An unresolvable pod does **not** arm — evidence: a `deadline_exceeded` failure whose pod lookup returns nothing (pod GC'd, or the lister not synced) asserts the published `UpdateFrontmatterCommand` carries `current_job: ""` and no `max_triggers` key; `go test ./pkg/...` exits 0
- [ ] The existing crash arming still holds — evidence: the current `pod_crash_no_stdout` arming spec still asserts `max_triggers: 3` and passes (call sites updated mechanically where the publisher's signature changed); `go test ./pkg/...` exits 0
- [ ] An already-present `max_triggers` is left alone on the deadline path — evidence: a spec with `max_triggers: 5` on the task and a never-scheduled `deadline_exceeded` failure asserts the published update carries no `max_triggers` key (the operator's budget is not lowered); `go test ./pkg/...` exits 0
- [ ] The deadline arm is distinguishable from the crash arm in the log — evidence: the deadline arm emits the existing `event=crash_arm_decision` line carrying `parsed=deadline_exceeded` and `decision=arm`, and the new spec asserts that line for the arming case; `grep -n 'parsed=deadline_exceeded' pkg/result_publisher.go` returns ≥1 line
- [ ] `make precommit` exits 0
- [ ] CHANGELOG carries the fix — evidence: `grep -n '## Unreleased' CHANGELOG.md` returns line ≥1 and the bullet under it names the deadline arm; `git diff --stat` shows `CHANGELOG.md` modified
- [ ] **Post-Deploy (Rung-2):** a task with no `max_triggers` whose Job cannot schedule arms the cap on dev — evidence: `kubectlnukedev -n agent logs deploy/agent-task-executor-personal --since=3h | grep 'event=crash_arm_decision' | grep 'decision=arm'` returns ≥1 line naming the replay task, and `vault-cli --vault private-personal task get "<replay task>" max_triggers --output json` reports `3`
  - `deploy_check:` `kubectlnukedev -n agent get deploy agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `v0.55.2` — the predicted next patch tag (`## Unreleased` carries only `fix:` bullets); the operator updates it to the tag actually deployed if that differs.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — exits 0
- `go test ./pkg/...` — exits 0; the new deadline-arm specs and the unchanged crash-arm spec are green
- `grep -n 'ZombieReasonDeadlineExceeded' pkg/result_publisher.go` — returns ≥1 line (the deadline path is reachable from the arm gate)
- `grep -n 'parsed=deadline_exceeded' pkg/result_publisher.go` — returns ≥1 line
- `grep -n '## Unreleased' CHANGELOG.md` — returns line ≥1

### Operator-executable (runs on the host after PR merge, spec verification ladder)

- `kubectlnukedev -n agent get deploy agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}'` — prints the tag carrying this fix
- `kubectlnukedev -n agent logs deploy/agent-task-executor-personal --since=3h | grep 'event=crash_arm_decision'` — shows the deadline arm decision for the replay task
- `vault-cli --vault private-personal task get "<replay task>" max_triggers --output json` — reports `3` after the third never-scheduled kill

## Desired Behavior

1. A failed Job whose `JobFailed` condition reason is `DeadlineExceeded` resolves its pod through the pod lister the `jobWatcher` already exposes and uses for the `BackoffLimitExceeded` path.
2. When that pod carries a `PodScheduled=False` condition, the failure is classified as never-scheduled and the arm gate writes `max_triggers: defaultMaxTriggers` (3), exactly as the `pod_crash_no_stdout` path does.
3. When the pod reached a terminal phase (`Succeeded`/`Failed`) before the deadline, or carries no `PodScheduled=False` condition, the arm gate returns without writing `max_triggers` — the record is published with `current_job: ""` only, as today.
4. When the pod cannot be resolved (GC'd, lister not synced), the arm gate returns without writing `max_triggers`. An unresolvable pod is not evidence of never having scheduled.
5. An already-present `max_triggers` is never overwritten on this path, preserving an operator-set budget.
6. The `pod_crash_no_stdout` arming behavior is unchanged in every respect.
7. The arm decision is logged on the existing `event=crash_arm_decision` line, carrying the parsed reason and the decision, so an operator can tell a deadline arm from a crash arm without reading the record.
8. The store-miss path (`PublishStoreMissFailure`) reaches the same arm gate and behaves identically — a never-scheduled deadline failure arms there too. This is intentional, not an asymmetry.

## Constraints

- **Pod-state plumbing.** The arm gate (`crashFailureUpdates`, `pkg/result_publisher.go`) is a package-level function that receives only a reason string; the pod lister lives on `jobWatcher` (`pkg/job_watcher.go:352`) and is already used to resolve pods for `BackoffLimitExceeded`. The pod-state signal must therefore be carried from the watcher into the gate — either as an added `PublishFailure` parameter or as a publisher dependency. Either shape is acceptable; what is frozen is that the **reason string continues to flow through unchanged**, and that the gate still branches on the parsed reason so the deadline path stays greppable in `pkg/result_publisher.go`.
- **Spec 006's recurring-task guard holds for every class except the never-scheduled one.** A task with no `ref` and no `max_triggers` stays uncapped when its pod ran and hit the deadline, or when its pod cannot be resolved. Only a pod that demonstrably never scheduled arms the cap. This is a deliberate, narrow amendment to `specs/completed/006-bug-trigger-cap-never-engages-for-repo-backed-tasks.md` § Constraints; that spec is not edited — the new spec supersedes its constraint for this one class and carries the pointer.
- **`specs/in-progress/004-bug-job-failure-reason.md` is at `status: verifying`** and owns the `DeadlineExceeded` / `BackoffLimitExceeded` classification split in `job_watcher.go`. This spec consumes that classification and must not re-open it.
- Repo conventions are frozen: Ginkgo/Gomega v2 tests, `github.com/bborbe/errors` wrapping (never `fmt.Errorf`), counterfeiter mocks for new dependencies (`//counterfeiter:generate`), glog `V(n)` gating.
- The published reason string for a never-scheduled deadline kill stays `deadline_exceeded`. The class is discriminated at the arm gate, not by introducing a new reason value — changing the reason would alter the `## Failure` body every operator greps and would collide with spec 004.
- CHANGELOG: add an `## Unreleased` bullet (HEAD is `## v0.55.1`).

## Failure Modes

| Trigger | Expected behavior | Recovery |
|---|---|---|
| Pod already GC'd when the deadline failure is handled | No arm; `current_job: ""` published as today | None needed — the task re-queues and a later cycle with a resolvable pod arms it |
| Pod lister not synced | No arm | None — same as an unresolvable pod |
| Pod is `Pending` but `PodScheduled` is `True` (scheduled, containers not started) | No arm — the pod was placed | Investigate image pull / container start separately; this spec covers only unschedulable pods |
| A legitimately long task is killed at its deadline after running | No arm | Operator raises `activeDeadlineSeconds` on the config |
| Recurring task (no `ref`, no `max_triggers`) whose pod never schedules | Arms at 3, assignee cleared | Correct per this spec: a recurring task that can never schedule is stuck, not merely re-dispatching. Re-dispatch resumes when an operator re-assigns |
| A legitimate task is armed in error | `max_triggers: 3` written, assignee cleared on the third failure | Reversible: remove `max_triggers`, re-assign. The next failure cycle re-arms only if the pod is still never scheduled |
| The controller's increment lands before the arm write | The increment sees no cap and clears nothing; the cap takes effect from the next failure cycle | Expected; costs one extra cycle (documented at `pkg/result_publisher.go`) |

## Reproduction

Observed on nuke dev, 2026-11-03. Task `6c83d4be-e75e-4747-acf9-3ecfec4daa99`, Job `service-analysis-agent-6c83d4be-20261103103532`, `activeDeadlineSeconds: 1800`, executor `v0.55.1`.

```
task record (private-personal/25 Tasks/Analyse agent-task-controller Memory Held Between Reconcile Loops.md, ## Progress):
  - 2026-11-03: Run 1 (`service-analysis-agent-6c83d4be-20261103103532`) failed `deadline_exceeded` at 11:05:34Z
    (Job `activeDeadlineSeconds: 1800`).

other records on the same cluster, same class:
  trigger_count: 9   (service-analysis-agent smoke task)
  trigger_count: 41  (Service Analysis Needs-Input Probe Missing Path)
  trigger_count: 90  (hypothesis-smoke-test-bbr-h5e-delegation-v6, no ref, no max_triggers)
```

Commands that produced the cluster evidence:

```bash
kubectlnukedev -n agent logs deploy/agent-task-executor-personal --since=96h | grep -c 'crash_arm_decision'   # 0
kubectlnukedev -n agent logs deploy/agent-task-executor-personal --since=96h | grep -c 'service-analysis'     # 73
kubectlnukedev -n agent get jobs | grep 6c83d4be                                                              # no output: TTL-GC'd
```

Unit-level reproduction against the current code:

```
job := Job{ status.conditions = [{ type: JobFailed, status: True, reason: "DeadlineExceeded" }] }
pod := Pod{ status.conditions = [{ type: PodScheduled, status: False }] }   # never placed on a node

JobFailureReason(job)                                    -> deadline_exceeded          # correct
crashFailureUpdates(task, jobName, "deadline_exceeded")  -> { current_job: "" }        # WRONG: no max_triggers
```

Why the sweeper does not cover this class:

```
pkg/zombie_sweeper.go:142-143   if elapsed < deadline { continue }        # only classifies at/after the deadline
k8s/apis/.../v1/types.go:26     DefaultZombieSweeperIntervalSeconds = 60   # polls every 60 s
pkg/job_watcher.go:838          w.taskStore.Delete(taskID)                # event-driven watcher removes the task after publishing
pkg/job_watcher.go:952-955      failureReason returns the deadline reason with no pod read
```

Both become eligible at the same instant (`DefaultZombieJobTimeoutSeconds` and the Job's `activeDeadlineSeconds` are both 1800), so the informer-driven watcher publishes `deadline_exceeded` and deletes the task before the sweeper's next tick. The sweeper's `pod_not_scheduled` classification is therefore unreachable for a Job that reaches its deadline.

## Expected vs Actual

**Expected** — the cap's design contract (anchor task `Agent Gate Failure Never Terminal — Unbounded 60s Respawn Loop`, SC #2): *no task spawns more than N jobs for the same scope without escalating or parking.* A Job that can never schedule is the clearest case of that contract, and it is the case the cap does not cover.

**Actual** — `crashFailureUpdates` (`pkg/result_publisher.go:515-542`) returns early for every parsed reason other than `ZombieReasonPodCrashNoStdout`, so `max_triggers` is never written on the deadline path, `TriggerCapEngaged` stays false for a record with no `ref`, and `trigger_count` accrues without bound.

## Why this is a bug

The cap exists to stop a task from respawning without bound, and spec 006 explicitly widened it for repo-backed tasks on the reasoning that a constant-scope accrual should not run forever. A never-scheduled Job is a stronger instance of the same failure: its scope is constant by construction, it cannot succeed on any retry, and each attempt costs a full deadline of cluster time. The gap is not a design decision anyone recorded — it is the shape the crash-only gate inherited from a path that predates the deadline classification split.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Deadline arm: carry pod state from the watcher into the arm gate, arm on `PodScheduled=False`, keep every other class unchanged, add the Ginkgo specs, add the CHANGELOG bullet | 1-8 | 1-8 | — |

Rationale: a single prompt carries the whole change — one gate condition, the pod plumbing, the negative cases, and the changelog. Splitting would separate the arming behavior from the guards that keep it narrow, which is where the regression risk lives.

No new scenario. The behavior is a publish-gate decision plus a pod-state read; unit and integration specs at the `PublishFailure` seam reach it in full. The Rung-2 live replay in the Acceptance Criteria is the runtime check.

## Do-Nothing Option

Not acceptable to keep. The reported incident is live on dev, three records show unbounded accrual (9, 41, 90), and each attempt burns 1800 s of cluster time. The fix is one gate condition plus a pod-state read, guarded by negative cases — the cost of doing nothing grows with every stuck task.
