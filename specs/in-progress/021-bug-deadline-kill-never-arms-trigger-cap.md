---
status: approved
approved: "2026-11-03T19:54:03Z"
generating: "2026-11-03T19:58:19Z"
branch: dark-factory/bug-deadline-kill-never-arms-trigger-cap
---

## Summary

- An agent Job that dies on its deadline without ever being placed on a node never counts toward the retry cap, so its task re-queues until a human clears the assignee.
- The cap is armed for exactly one kind of failure — a crashed pod that produced no output — and every other failure leaves the record uncapped.
- **A failure-time pod lookup cannot work, and this was proven on the cluster rather than assumed.** The Job controller deletes the pod one second *before* it stamps the Job's `DeadlineExceeded` condition, so when the informer delivers the failed Job the pod is already gone. The first shipped attempt resolved the pod at that moment and therefore never armed (`decision=not_never_scheduled pod_resolved=false`, nuke dev 2026-11-04).
- The fix arms on the zombie sweeper's existing `pod_not_scheduled` classification instead. That classification reads the pod while it is still `Pending` and unschedulable — about two minutes after it fails to place, rather than thirty minutes after the deadline, by which time the pod no longer exists.
- Every other failure — a pod that ran and then hit its deadline, and every recurring task — behaves exactly as it does today.

## Problem

A task whose Job can never be placed on a node burns a full `activeDeadlineSeconds` budget (1800 s by default) per attempt, then re-queues and burns another. Nothing escalates: `crashFailureUpdates` writes `max_triggers` only for `ZombieReasonPodCrashNoStdout`, and the controller's escalation is gated on `TriggerCapEngaged` (defined in `bborbe/agent-task-controller`), which requires either an explicit `max_triggers` or a `ref`. A record with neither — the shape the reported incident has — therefore accrues `trigger_count` without bound. Observed records sit at 9, 41 and 90.

The operator-visible cost is two-fold: cluster slots are consumed by Jobs that can never run, and a genuinely stuck task never surfaces in the inbox as something needing attention, because the only signal the cap produces (a cleared `assignee` plus an escalation) never fires.

## Goal

A Job whose pod cannot be placed on any node counts toward the trigger cap. The sweeper publishes `pod_not_scheduled` once the pod has been `Pending` with `PodScheduled=False` for longer than its grace window, and the cap is armed on the **first** such failure (`max_triggers: 3` written); on the third, the controller clears the `assignee` and the task stops re-queuing on its own. A Job that ran and then hit its deadline, and any recurring task, behave exactly as they do today.

This also caps the loop about 28 minutes earlier than the deadline path would have: the stuck Job stops after three spawns instead of burning a full `activeDeadlineSeconds` per attempt.

## Non-goals

- **Arming on the store-miss path.** `taskFromFailedJob` marks `max_triggers` present-but-nil precisely so that path never overwrites an operator budget; changing that sentinel is a separate spec with its own regression risk.
- **A new `ZombieReason` value.** The class already has one — `pod_not_scheduled` — and this spec uses it rather than inventing a second. The published reason for a deadline kill that *did* run stays `deadline_exceeded`.
- **Changing what `pod_not_scheduled` means.** The sweeper's classification and its 2-minute grace window are untouched; only *when* the sweeper is allowed to reach it changes.
- **Arming on `deadline_exceeded`.** That path is retained for the pod that ran and hit its deadline, but it no longer arms: its pod is unresolvable by construction (see Reproduction).
- **Raising `activeDeadlineSeconds` or resizing the cluster.** The fix is about escalation, not about making the stuck Job run.

## Assumptions

- The pod lister for the task's `jobNamespace` is synced. When it is not, the sweeper skips that task for the tick and retries on the next one, so a never-scheduled task is armed a tick or two later rather than not at all.
- `specs/in-progress/004-bug-job-failure-reason.md` (`status: verifying`) lands first, and this spec consumes its `DeadlineExceeded` / `BackoffLimitExceeded` split without changing it.
- `TriggerCapEngaged`'s "explicit `max_triggers` or present `ref`" semantics, as implemented in `bborbe/agent-task-controller`, are as described here.

## Acceptance Criteria

- [ ] A `pod_not_scheduled` failure arms the cap — evidence: a spec at the `PublishFailure` seam whose task carries no `max_triggers` and whose reason is `pod_not_scheduled` asserts the published `UpdateFrontmatterCommand` contains `max_triggers: 3`; `go test ./pkg/...` exits 0
- [ ] A `deadline_exceeded` failure does **not** arm — evidence: the same spec shape with reason `deadline_exceeded` asserts the published `UpdateFrontmatterCommand` carries `current_job: ""` and no `max_triggers` key; `go test ./pkg/...` exits 0
- [ ] The sweeper publishes `pod_not_scheduled` before the deadline — evidence: a sweeper spec whose task is well inside its `ZombieJobTimeoutSeconds` (5 min elapsed against the 1800 s default) and whose pod is `Pending` with `PodScheduled=False` past the 2-minute grace window asserts one `PublishFailure` call with reason `pod_not_scheduled`; `go test ./pkg/...` exits 0
- [ ] The existing crash arming still holds — evidence: the current `pod_crash_no_stdout` arming spec still asserts `max_triggers: 3` and passes; `go test ./pkg/...` exits 0
- [ ] An already-present `max_triggers` is left alone on the never-scheduled path — evidence: a spec with `max_triggers: 5` on the task and a `pod_not_scheduled` failure asserts the published update carries no `max_triggers` key (the operator's budget is not lowered); `go test ./pkg/...` exits 0
- [ ] The Job-informer path publishes the deadline reason without resolving a pod — evidence: a spec driving the failure handler with a `Failed/DeadlineExceeded` Job asserts `PublishFailure` was called once with reason `deadline_exceeded`; `go test ./pkg/...` exits 0
- [ ] The store-miss path does **not** arm — evidence: a `pod_not_scheduled` failure published through `PublishStoreMissFailure` asserts the published `UpdateFrontmatterCommand` contains no `max_triggers` key (the nil sentinel still suppresses it); `go test ./pkg/...` exits 0
- [ ] The never-scheduled arm is distinguishable from the crash arm in the log — evidence: a spec capturing glog output (via the existing `captureGlogStderr` helper) for a `pod_not_scheduled` failure asserts the emitted line contains `parsed="pod_not_scheduled"` and `decision=arm`; `go test ./pkg/...` exits 0
- [ ] `make precommit` exits 0
- [ ] CHANGELOG carries the fix — evidence: `grep -A2 '## Unreleased' CHANGELOG.md | grep -i deadline` returns ≥1 line; `git diff --stat` shows `CHANGELOG.md` modified
- [ ] **Post-Deploy (Rung-2):** a task with no `max_triggers` whose Job cannot schedule arms the cap on dev. Build the unschedulable Job by giving the replay agent a `nodeSelector` matching no node on dev (or a memory request above every node's allocatable), and confirm `kubectlnukedev -n agent get pod <replay-pod> -o jsonpath='{.status.conditions}'` shows `PodScheduled=False` before the deadline — evidence: `kubectlnukedev -n agent logs deploy/agent-task-executor-personal --since=3h | grep 'event=crash_arm_decision' | grep 'decision=arm'` returns ≥1 line naming the replay task, and `vault-cli --vault private-personal task get "<replay task>" max_triggers --output json` reports `3`
  - `deploy_check:` `kubectlnukedev -n agent get deploy agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `$(git describe --tags --abbrev=0 origin/master)` — resolved at verification time to the tag the release cut produced, so a patch-vs-minor prediction cannot make Phase 0.5 refuse a correct deploy.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — exits 0
- `go test ./pkg/...` — exits 0; the new deadline-arm specs and the unchanged crash-arm spec are green
- `grep -n 'ZombieReasonDeadlineExceeded' pkg/result_publisher.go` — returns ≥1 line (the deadline path is reachable from the arm gate)
- `grep -n '## Unreleased' CHANGELOG.md` — returns line ≥1

### Operator-executable (runs on the host after PR merge, spec verification ladder)

- `kubectlnukedev -n agent get deploy agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}'` — prints the tag carrying this fix
- `kubectlnukedev -n agent logs deploy/agent-task-executor-personal --since=3h | grep 'event=crash_arm_decision'` — shows the deadline arm decision for the replay task
- `vault-cli --vault private-personal task get "<replay task>" max_triggers --output json` — reports `3` after the third never-scheduled kill

## Desired Behavior

1. The zombie sweeper classifies an unschedulable pod at its 2-minute grace window, not only at the deadline: once a task's `job_started_at` is older than `podNotScheduledGraceWindow`, the sweeper resolves the namespace and reads the pod lister even when `elapsed < deadline`.
2. When `classify` returns `pod_not_scheduled`, the sweeper publishes it immediately, without waiting for the deadline. That reason reaches the arm gate, which writes `max_triggers: defaultMaxTriggers` (3), exactly as the `pod_crash_no_stdout` path does.
3. When `classify` returns anything else, the original deadline gate still applies: nothing is published before `elapsed >= deadline`, so a pod that is merely slow, a task with no pod yet, and a healthy Running pod all behave as today.
4. A `deadline_exceeded` failure never arms. The Job controller deletes the pod one second before it stamps that condition, so the arm gate cannot discriminate the never-scheduled class there — and arming on the bare reason would cap every task that legitimately ran to its deadline.
5. An already-present `max_triggers` is never overwritten on this path, preserving an operator-set budget.
6. The `pod_crash_no_stdout` arming behavior is unchanged in every respect.
7. The arm decision is logged on the existing `event=crash_arm_decision` line, carrying the parsed reason and the decision, so an operator can tell a never-scheduled arm from a crash arm without reading the record.
8. The store-miss path (`PublishStoreMissFailure`) reaches the same arm gate but does **not** arm: `taskFromFailedJob` builds the task with `max_triggers` present-but-nil, and the gate treats a present key as already-armed. That sentinel is deliberate — an absent key there would overwrite an operator-set cap of 5 or 6 with the default 3 — so this path stays exactly as it is today.
9. `PublishFailure` keeps its pod argument, which the log-tail capture added on master now consumes. The arm gate no longer reads it: the live replay proved that value is always nil for a deadline kill, so gating the arm on it would leave the suite green while the runtime never armed. The pod feeds the log tail only.

## Constraints

- **The arm gate decides on the reason alone.** The first shipped attempt carried the failed Job's pod into the gate so it could read `PodScheduled=False`. The live replay showed the value is always nil where it matters — the Job controller deletes the pod one second before stamping `DeadlineExceeded` — so the gate now decides on the reason, and the reason is produced by the only component that can still see the pod: the sweeper. `PublishFailure` keeps its pod argument because the log-tail capture consumes it; the gate ignores it.
- **The sweeper owns the never-scheduled signal.** `classify` (`pkg/zombie_sweeper.go`) already returns `ZombieReasonPodNotScheduled` for a `Pending` pod carrying `PodScheduled=False` past `podNotScheduledGraceWindow`. This spec does not change that classification or its grace window; it changes only when the sweeper is allowed to reach it, moving the gate from `elapsed >= deadline` to `elapsed >= grace` for that one class.
- **A per-task pod read every tick.** The early classification runs `resolveTaskNamespace` and one lister query per task per sweep, where previously that work happened only for past-deadline tasks. It reads the informer cache and issues no API calls, so the cost is an in-memory selector scan per task on a 60 s tick.
- **One definition of "never scheduled".** `hasPodScheduledFalse(pod)` already exists at `pkg/zombie_sweeper.go:276` in the same package. Reuse it — a second, independent `PodScheduled` check would let the sweeper's classification and the arm gate's classification diverge silently.
- **Spec 006's recurring-task guard holds for every class except the never-scheduled one.** A task with no `ref` and no `max_triggers` stays uncapped when its pod ran and hit the deadline, or when its pod cannot be resolved. Only a pod that demonstrably never scheduled arms the cap. This is a deliberate, narrow amendment to `specs/completed/006-bug-trigger-cap-never-engages-for-repo-backed-tasks.md` § Constraints; that spec is not edited — the new spec supersedes its constraint for this one class and carries the pointer. Spec 006's own acceptance criterion ("the recurring-task regression guard is unchanged … `SpawnJobCallCount() == 1`") still holds at the controller test level, because that fixture task carries no `max_triggers`; the two specs do not disagree.
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
| A legitimate task is armed in error | `max_triggers: 3` written, assignee cleared on the third failure | Reversible: `vault-cli --vault <v> task remove "<task>" max_triggers` then `vault-cli --vault <v> task set "<task>" assignee "<agent>"`. The next failure cycle re-arms only if the pod is still never scheduled |
| The publish fails after the arm decision is made | No key written — the decision is local to `crashFailureUpdates`; the caller logs the publish error | The next failure cycle re-arms |
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

Why a failure-time pod lookup cannot work — **measured on nuke dev, 2026-11-04, executor `v0.55.2`**:

```
job replay-probe-agent-...-20261104114728, activeDeadlineSeconds 1800, pod unschedulable (memory 500Gi)

namespace events:
  3m18s  Normal   SuccessfulDelete  job/...  Deleted pod: ...-qc4nq
  3m17s  Warning  DeadlineExceeded  job/...  Job was active longer than specified deadline

executor log:
  event=crash_arm_decision task=35518b96-... reason="deadline_exceeded" parsed="deadline_exceeded" \
    parsed_ok=true decision=not_never_scheduled pod_resolved=false
```

The Job controller deletes the pod **one second before** it stamps `DeadlineExceeded`, so the informer-delivered failure always arrives after the pod is gone and `jobPod` correctly returns nil. The first shipped attempt armed on `deadline_exceeded` + `PodScheduled=False` and therefore never armed at all: an earlier replay of the same class ran `trigger_count` from 1 to **11** with no `max_triggers` written, looping until the sweeper's `executor_watch_lost` cleared the assignee.

The pod state is observable only while the pod is `Pending`. That is the sweeper's window, and it is why this spec moves the signal there.

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
