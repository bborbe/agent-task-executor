---
status: verifying
approved: "2026-11-08T14:50:14Z"
generating: "2026-11-08T15:03:34Z"
prompted: "2026-11-08T15:03:34Z"
verifying: "2026-11-08T15:20:22Z"
branch: dark-factory/clear-ghost-current-job
---

## Summary

- A task's `current_job` is cleared only when the executor *observes* the named Job end. If the Job disappears first, the field keeps naming a Job that no longer exists, and nothing ever clears it.
  - The Job can disappear through TTL garbage collection across an executor restart, the weekly cluster reboot, or a manual delete.
- Such a "ghost" field tells every reader the record is claimed by running work when nothing runs. It also keeps the ghost-claim regression metric from reaching 0.
- This spec adds a step to the reconcile pass that clears a ghost `current_job`. It acts only when three independent reads agree that the named work runs nowhere, and only after the executor's own spawn path has been excluded under its per-task lock.
- The step writes nothing but `current_job: ""`. It changes no status, phase, claim or body, and it does not change dispatch.

## Problem

Every existing writer of `current_job: ""` reacts to an event the executor saw:
- the job-success clear;
- the crash-failure update;
- the type-mismatch failure;
- an abort.

A Job removed before any such event leaves the field set indefinitely:
- the Job informer registers no delete handler;
- spawned Jobs carry no ownerReference;
- after a restart the task store is empty, so neither the zombie sweeper nor the dispatcher holds an entry naming the Job.

Measured on prod on 2026-11-06: one open record named `pr-reviewer-agent-99750c2d-20261103233145`, while the cluster held no agent Jobs at all. The record later completed with the stale field still set.

## Goal

On every reconcile pass that completes the drive loop, every open record this executor owns has its `current_job` cleared if all of these hold:
- the field names a Job absent from the cluster;
- no Job for the task runs in this cluster;
- the task's shared claim reads free;
- this executor has no spawn of it recorded or in flight.

No other record's `current_job` is ever written by this step.

## Non-goals

- Releasing, renewing or writing any distributed claim.
- Changing dispatch. This covers `suppressForActiveCurrentJob`, `jobProvablyFinished`, `currentJobFinishedWithoutStoreEntry`, the claim probe and the spawn path.
- Bounding the dispatch suppression of a ghost whose claim lease is unreadable or whose claim key cannot be derived. This step leaves those records alone (`guard=claim_held` / `claim_unprobeable`).
- Records whose `status` is `completed` or `aborted`.
- Clearing `job_started_at` or any field other than `current_job`.
- A vault-side compare-and-set. `UpdateFrontmatterCommand` (`bborbe/agent` `command/task`, v0.113.0) carries no precondition, and adding one is a cross-repo change.
- New configuration flags, env vars or metrics.

## Assumptions

- **Pass shape.** `ReconcileOnce` (`pkg/handler/task_event_handler.go`) runs every `RECONCILE_INTERVAL`; the default is `60s` and prod and dev leave it unset. A pass does not complete the drive loop when it returns early on:
  - view not ready;
  - an unstamped or mismatched vault;
  - an aborted gather or drive (`passDone`).
- **Owned tasks.** `ownedTasks(snapshot, h.vaultMatcher)` yields tasks from the replay-backed view with complete frontmatter. `private-agent` records carry `target_vault: openclaw`, so the `agent-task-executor-openclaw` deployment owns them.
- **The write.** `ResultPublisher.PublishClearCurrentJob(ctx, task, jobName)` writes only `current_job: ""`. Its 1 h LRU dedupe is keyed by `jobName` and shared with `PublishFailure`. A repeat call for the same name within that hour returns nil without writing.
- **Cluster reads.**
  - The step uses the handler method `h.isJobActive(ctx, task, config)`, the same call dispatch uses. With an assignee'd Config it answers from `activeJobsFor` → `jobSpawner.ListActiveJobs` (pass-cached; `finishSpawn` invalidates the entry). With no assignee it calls `jobSpawner.IsJobActive` directly.
  - `jobSpawner.JobExists(ctx, namespace, name)` returns false with no error on NotFound.
- **Claim probe.** `h.claimProbe(ctx, task)` returns `claimProbeHeld`, `claimProbeFree` or `claimProbeUnavailable`, plus an error.
  - Claim modes `off` and `shadow` never return `claimProbeFree`, so this step never clears there.
  - Prod and dev run `CLAIM_MODE=on` (read on 2026-11-08).
  - Claim semantics: `docs/claim-lease-protocol.md`.
- **In-process spawn state.** `h.taskStore` (a concrete `*pkg.TaskStore`) holds an entry only after `storeSpawnedJob`. The spawn path calls `markSpawnPending(taskID)` and then takes `lockClaimRelease(taskID)` across the claim. `spawnPendingFor(taskID)` reports that window.
- **Config resolution counts.** `resolveConfig`, reached through `resolveConfigForReconcile`, increments `TaskEventsTotal{skipped_unknown_assignee|error}` and `SkippedUnknownAssigneeTotal`.
- **Verbosity.** All three prod executors run `-v=2`.

## Acceptance Criteria

**Test fixture.** Every row drives `ReconcileOnce` against a ready view and the real `*pkg.TaskStore`, with Counterfeiter fakes for the job spawner, the claim client and `ResultPublisher`. Each row starts from a base record that would be cleared, and varies one thing.

- **Base record:**
  - owned, `status: in_progress`, `stage` equal to `h.branch`;
  - `current_job: ghost-job-1`;
  - no store entry, not spawn-pending;
  - an assignee whose Config resolves.
- **Fake defaults:** no active Job for the task (`ListActiveJobs` omits it, and `IsJobActive` is false for the no-assignee case), `JobExists` false, probe `claimProbeFree`, all with no error.

- [ ] **Decision table.** Every row passes, asserted on `PublishClearCurrentJob` call counts and on which reads were or were not made:

  | Row | Varied | Publish calls | Also asserted |
  |---|---|---|---|
  | a | nothing | exactly **1**, `jobName == "ghost-job-1"` | the cleared line |
  | b | `JobExists` true | **0** | probe not called (order) |
  | c | the task is in `ListActiveJobs` for its assignee (and separately, a resolved Config whose `Assignee` is empty, via the fake resolver, with `IsJobActive` returning true) | **0** | `JobExists` not called (order) |
  | d | probe `claimProbeHeld` | **0** | kept line `guard=claim_held` |
  | e | probe error, and separately `claimProbeUnavailable` | **0** | `guard=claim_unprobeable` |
  | f | `JobExists` errors, and separately `ListActiveJobs` errors | **0** | `guard=read_failed` |
  | g | store entry inserted as a side effect of the fake claim client's read | **0** | `guard=store_entry` |
  | h | the task is marked spawn-pending as a side effect of the fake claim client's read (an `*_internal_test.go` spec calls `markSpawnPending` directly) | **0** | `guard=store_entry` |
  | i | `status: completed`, and separately `aborted` | **0** | 0 `JobExists` calls |
  | j | `stage` differs from `h.branch` | **0** | 0 `JobExists` calls |
  | k | `current_job` empty | **0** | 0 `JobExists` calls |
  | l | an assignee that is unknown, or that is empty with no `task_type` match, and separately the namespace invalid | **0** | `guard=namespace_unresolved`; `TaskEventsTotal` and `SkippedUnknownAssigneeTotal` unchanged by the step |
  | m | publish returns an error | 1 per pass, each erroring | `guard=publish_failed`; the pass returns nil; 2 calls across 2 passes |

  Evidence: `go test ./pkg/handler/ -v -args -ginkgo.focus='ghost current_job' 2>&1 | tee /tmp/ghost.out | grep -qE 'Ran (1[3-9]|[2-9][0-9]) of' && grep -q 'SUCCESS!' /tmp/ghost.out`. Every row is a spec under a `Describe` whose text contains `ghost current_job`.
- [ ] **Summary line.** Every pass that completes the drive loop emits exactly one `glog.V(2)` line `event=ghost_current_job_pass scanned=<n> cleared=<n> kept=<n> unreached=<n>`, including when every count is 0.
  - `scanned` is the number of candidates evaluated; `cleared + kept == scanned`.
  - `unreached` is the number of candidates skipped because `passCtx` expired mid-step.
  - A pass that does not complete the drive loop emits none.
  - Evidence: a focused spec runs two zero-candidate passes and captures 2 lines, a view-not-ready pass captures 0, and a pass whose context expires after the first of two candidates captures `scanned=1 … unreached=1`.
- [ ] **Per-record lines.** Every candidate (DB2) emits exactly one of these per pass:
  - `glog.Infof` `event=ghost_current_job_cleared task=<id> job=<name>`;
  - `glog.V(2)` `event=ghost_current_job_kept task=<id> job=<name> guard=<label>`, where `<label>` is one of `store_entry`, `namespace_unresolved`, `job_active`, `job_exists`, `claim_held`, `claim_unprobeable`, `read_failed`, `publish_failed`.
  - Evidence: rows a, d, e, f, g, l and m assert their line.
- [ ] **Post-Deploy (Rung-2):** a seeded ghost clears on nuke dev.
  - `deploy_check:` `kubectlnukedev -n agent get deploy/agent-task-executor-openclaw -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `$(git describe --tags --abbrev=0)`
  - Setup: the operator seeds one record in `private-agent/tasks` with:
    - `target_vault: openclaw`, `stage: dev`, `status: in_progress`, `phase: human_review`;
    - an `assignee` whose Config exists on nuke dev;
    - a fresh `task_identifier`;
    - `current_job` set to a name absent from `kubectlnukedev get jobs -A -o name`.
  - Evidence:
    - within 3 minutes, `kubectlnukedev -n agent logs deploy/agent-task-executor-openclaw --since=10m | grep -c "event=ghost_current_job_cleared task=<id>"` returns ≥ 1;
    - after the controller commits, the record reads `current_job: ""` on `private-agent` `origin/main`.
  - Precondition (applies to both Post-Deploy rungs): with `autoRelease: false`, the release containing this spec's commit is cut before any deploy. Run a rung only once `git tag --contains <fix commit>` lists the tag that `git describe --tags --abbrev=0` returns, so the latest tag cannot name a pre-fix image.
- [ ] **Post-Deploy (Rung-3):** the step runs on prod.
  - `deploy_check:` `kubectlnukeprod -n agent get deploy/agent-task-executor-openclaw -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `$(git describe --tags --abbrev=0)`
  - Evidence:
    - `kubectlnukeprod -n agent logs deploy/agent-task-executor-openclaw --since=4m | grep -c event=ghost_current_job_pass` returns ≥ 2;
    - for each `task_identifier` whose record the ghost-claim metric lists at deploy time, `kubectlnukeprod -n agent logs deploy/agent-task-executor-openclaw --since=5m | grep -cE "event=ghost_current_job_(cleared|kept) task=<id>"` returns ≥ 1.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — format, lint, vet, and the unit and integration tests.
- `go test ./pkg/handler/ -v -args -ginkgo.focus='ghost current_job' 2>&1 | tee /tmp/ghost.out | grep -qE 'Ran (1[3-9]|[2-9][0-9]) of' && grep -q 'SUCCESS!' /tmp/ghost.out` — the count fails the command when the focus selects too few specs, and `SUCCESS!` fails it when a selected spec fails.

### Operator-executable (runs on the host after merge, release and deploy)

- **Dispatch untouched** (operator-side because the run sets `hideGit=true`): `git diff origin/master -U0 -- pkg/handler/task_event_handler_current_job.go pkg/handler/task_event_handler_claim_probe.go pkg/handler/job_lookup_denied.go pkg/result_publisher.go` prints nothing.
- The Rung-2 dev seeded-ghost replay.
- The Rung-3 prod reads.
- The ghost-claim metric before and after the prod deploy: `kubectlnukeprod get jobs -A -o name | sed 's#.*/##' | python3 -I ~/.claude/scripts/regression-metrics/cj.py`.

## Desired Behavior

1. **Placement.** The step runs once per pass, after the drive loop completes and before the pass's `ok` accounting. Passes that return early do not run it. Inside the step, `passDone(passCtx)` is checked before each candidate; on expiry the step stops, counts the rest as `unreached`, and still emits the summary line.
2. **Candidates.** A candidate is an owned task with:
   - a non-empty `current_job`;
   - `status` neither `completed` nor `aborted`;
   - `stage` equal to `h.branch`;
   - not tagged for another cluster (`isForeignCluster` false).
   The stage rule is deliberately stricter than dispatch's `stageSkipResult`: it ignores `STAGE_FILTER=false`. Non-candidates get no reads and no lines.
3. **Checks.** For each candidate, in order; the first failure keeps the field and names its label:
   1. A store entry exists or a spawn is pending → `store_entry`.
   2. The Config is resolved with `resolveConfigForReconcile`'s rules (an unknown assignee is a skip; an empty assignee falls back to `task_type`), then the namespace with `jobNamespaceForTask`. A nil Config, a skip, or an error → `namespace_unresolved`. There is no fallback to the executor's own namespace. The step must not increment `TaskEventsTotal` or `SkippedUnknownAssigneeTotal`. Extracting a non-counting resolution core that `resolveConfigForReconcile` also calls is acceptable, provided the existing callers' counting is unchanged.
   3. `h.isJobActive(ctx, task, config)` → `job_active`.
   4. `JobExists(current_job)` → `job_exists`.
   5. Claim probe: Held → `claim_held`; error or Unavailable → `claim_unprobeable`.
   A cluster-read error in step 3 or 4 → `read_failed`.
4. **Publish.** Under `lockClaimRelease(taskID)`, the step repeats check 1. If it is still clear, it calls `PublishClearCurrentJob(ctx, task, current_job)` before releasing the lock. The spawn path marks the task pending and only then takes this lock for its claim, so one of two things happens:
   - the mark came first, and the step sees pending;
   - the mark came later, and the spawn's claim and spawn notification follow the published clear.
5. **Errors.** Publish error → `publish_failed`. No outcome fails the pass, and the next pass re-evaluates from scratch.
6. **Logging.** One summary line per pass that reaches the step, and one cleared or kept line per candidate. A clear repeated before the replayed view shows `""` hits the publisher dedupe and logs `cleared` again. That is expected, so the Rung-2 and Rung-3 evidence counts are `≥ 1`.

## Constraints

- `ResultPublisher` and `PublishClearCurrentJob` are unchanged.
- No new flags, env vars or metrics, and no `main.go` change.
- The dispatch files listed under Verification are unchanged.
- Ginkgo/Gomega, with Counterfeiter fakes for the interfaces. The task store is exercised as the real `*pkg.TaskStore`.
- At most 3 remote reads (the active-Job read, which is usually served from the per-pass `ListActiveJobs` cache; `JobExists`; the claim probe) per candidate per pass.

## Failure Modes

| Trigger | Expected behavior | Detection | Recovery |
|---|---|---|---|
| Kubernetes API unavailable | Field kept | `guard=read_failed` | Automatic on the next pass. Confirmed by the label disappearing after the API recovers |
| Claim store (vault-api) unavailable | Field kept | Existing `event=claim_probe_failed`, plus `guard=claim_unprobeable` | Automatic on the next pass |
| Kafka publish fails | Field kept; the dedupe is not recorded | `guard=publish_failed` | Automatic. Confirmed by a later `ghost_current_job_cleared task=<id>` |
| The controller drops the clear command | View keeps the name; repeat calls hit the 1 h dedupe | Repeated `cleared` lines for one task over more than a few minutes | Retries after the dedupe TTL. Operator reads the controller log for the `taskIdentifier` |
| This executor spawns the task concurrently | Locked re-check sees the store entry or the pending mark; no publish | `guard=store_entry` | None needed |
| Another cluster spawns between the claim probe and the publish (residual; no vault-side compare-and-set exists) | Field may read `""` while the foreign Job runs | The foreign executor's spawn log; empty `current_job` on the record | Bounded. That cluster's held claim and its `IsJobActive` still gate dispatch, and its job watcher writes the field at terminal. No double run follows from the empty field |
| Clock skew between clusters | The claim probe compares `LeaseUntil + TakeoverGrace` with the local clock, so a skewed clock can read a live lease as free | No direct signal | Bounded by the same guards as the previous row: `IsJobActive` (local) and the foreign claim fence |
| Many ghosts after a rebuild; client-go throttling (5 QPS) | Reads may slow; the step stops at `reconcilePassTimeout` | `unreached=<n>` in the summary; throttling warnings | The unreached candidates are evaluated on later passes |
| The drive loop keeps exhausting `reconcilePassTimeout` | The step never runs (accepted: the drive loop takes precedence) | No `ghost_current_job_pass` line, plus the existing `logAborted` lines | Fix the drive-loop cost; ghosts persist meanwhile, which is the pre-spec state |
| Executor restarts mid-pass | Stateless; the next pass re-evaluates | — | Automatic |

## Security / Abuse

No new inputs, endpoints or credentials. The only write is `current_job: ""` through the existing publisher, to records the executor already owns.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Ghost `current_job` step in `ReconcileOnce`, the non-counting Config resolution, its log lines, and the decision-table specs | 1–6 | Decision table, Summary line, Per-record lines | — |

Rationale: one package and one table. The Post-Deploy ACs are operator rungs.

## Do-Nothing Option

Ghost fields accumulate at every Job removal the executor does not observe, which happens at minimum at each weekly nuke reboot. Each one leaves the ghost-claim metric non-zero, makes the record read as claimed to operators and goal probes, and leaves a hand edit of agent-runtime records as the only repair. Dispatch is not blocked when the claim is free, so the cost is truthfulness and operator time, not lost work.
