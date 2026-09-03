---
status: prompted
approved: "2026-09-03T17:42:38Z"
generating: "2026-09-03T17:57:12Z"
prompted: "2026-09-03T18:13:09Z"
branch: dark-factory/bug-executor-restart-drops-deferred-respawn-queue
---

## Summary

- An executor restart silently drops every task that was deferred behind the concurrency cap: they sit at `in_progress` forever, indistinguishable from work in flight.
- `seedDeferredRespawnsFromStore` is meant to rebuild the deferral queue after a restart, but it scans `taskStore` — which the same restart just emptied — and additionally skips tasks with `current_job: ""`, which is exactly what a concurrency-cap deferral looks like.
- Task publication is edge-triggered on vault file changes, so a dropped deferral is never re-driven by a new event.
- Fix: a periodic reconcile loop that derives state from the world — the vault (via git-rest) and live k8s Jobs — and re-drives any eligible task without a live Job through the existing spawn path.
- The in-memory deferral map stays, but only as a fast path over the reconcile floor.

## Problem

The executor holds working state in memory only. `deferredRespawns` (the queue of tasks waiting to spawn because their assignee is over `maxConcurrentJobs`) and `taskStore` (the executor's view of spawned tasks) are both plain in-memory maps built fresh at startup. A restart wipes both. `seedDeferredRespawnsFromStore` — the existing restart-recovery — scans `taskStore`, so after a restart it reads an empty store and restores nothing. And even with a warm store it skips every task whose frontmatter `current_job` is empty, which is the defining shape of a concurrency-cap deferral (`deferIfAtConcurrencyCap` never writes `current_job` — no Job was spawned).

Observed in prod 2026-08-16 09:42:30Z: a routine `BRANCH=prod make apply` rolled the executor. Before the restart ~74 `github-update-go` tasks were correctly deferred behind `maxConcurrentJobs: 1`, cycling at ~70 `event=concurrency_cap` per 5-minute window. After it: `event=concurrency_cap` per cycle 0, active jobs 0, all 74 tasks still `planning`, none moving, no error anywhere. Restarts are routine in Kubernetes — node drains, evictions, OOM kills, Keel digest updates, deliberate deploys — so a restart being destructive is the bug, not the restart.

## Goal

After this work, an executor restart is non-destructive: every task that should be running (status `in_progress`, runnable phase, assignee set, no live Job) is re-driven by a periodic reconcile loop that reads the vault through git-rest and checks live Jobs. No vault edit, no operator action, and no `current_job` value is required for recovery. Queue depth is observable across a restart via a metric or log line.

## Non-goals

- Fix the concurrency cap itself — working correctly ([[agent-task-executor maxConcurrentJobs Is Not Enforced Under Concurrent Spawns]]).
- Fix the mislabeled failure reason (`deadline_exceeded` vs `BackoffLimitExceeded`) — separate spec 004 (in progress).
- Change the Kafka event path — stays edge-triggered; the reconcile loop is the pull path, not a replacement.
- Add persistence for the deferral queue — rejected; reconcile subsumes it and also fixes orphans from published-but-lost events.

## Reproduction

1. Configure an assignee with `maxConcurrentJobs: 1` (Config CRD).
2. Publish ≥2 tasks to that assignee so they reach the executor with status `in_progress`, runnable phase, assignee set. The first spawns a Job; the rest are deferred: `deferIfAtConcurrencyCap` adds them to `deferredRespawns` with `retryAfter = now + concurrencyCapRetryDelay` and does NOT write `current_job` to the task frontmatter.
3. Restart the executor pod (any roll: `helm upgrade`, node drain, OOM, Keel digest update).
4. Observe: `event=concurrency_cap` stops appearing, active jobs drop to 0 when the running Job finishes, and the deferred tasks stay `in_progress`/`planning` indefinitely. No error is logged.

Observed evidence (prod 2026-08-16, after restart):

```
event=concurrency_cap per cycle: ~70  →  0
active jobs:                          1  →  0
tasks in planning:                   74  →  74 (unchanged, none moving)
```

`seedDeferredRespawnsFromStore` did not help: it reads `taskStore` (`pkg/task_store.go` — in-memory, wiped by the restart) and skips `current_job == ""` (`pkg/handler/task_event_handler.go:637`), so the recovery mechanism is a no-op precisely when needed.

## Expected vs Actual

Expected (per the restart-safety intent of `seedDeferredRespawnsFromStore`, `task_event_handler.go:637-668`): after a restart, tasks whose deferral was lost are re-added to the queue and drained without external intervention.

Actual: the seed scans an in-memory store the restart emptied and additionally filters out `current_job == ""` (the deferral shape), so the queue stays empty and the tasks are orphaned until a new Kafka event happens to arrive for them.

## Why this is a bug

The executor's own documentation states the invariant: "seedDeferredRespawnsFromStore … restores deferred state lost when the in-memory map is wiped by an executor restart, so a stuck task does not remain stuck for want of a Kafka event that will never arrive" (`task_event_handler.go:637-668`). The mechanism cannot deliver that invariant: its source (`taskStore`) is destroyed by the same restart, and its filter excludes the deferral shape it exists to recover. A restart of any kind silently violates the documented guarantee, and the failure is invisible (the "0 active jobs while work remains" signature looks identical to healthy idling).

## Acceptance Criteria

- [ ] The reconcile loop lists the vault task files via git-rest (`List("<taskDir>/*.md")`) and re-drives every eligible task (status `in_progress`, phase in `{planning, execution, ai_review}`, assignee set, no live Job) through the existing spawn path — evidence: unit test with a fake git-rest client returning an eligible task file and a fake spawner asserts `SpawnJob` is called (test exit 0); the reconcile pass log line names the task id; `make precommit` exits 0
- [ ] A task deferred before a restart (frontmatter `current_job: ""`, status `in_progress`) is re-driven after a cold start with an empty in-memory store and no Kafka event — evidence: unit test constructs an empty `TaskStore`, a git-rest source returning that task, runs one reconcile pass, asserts the spawn path is invoked (test exit 0) and the reconcile log line names the task id
- [ ] The reconcile loop does NOT re-drive: tasks with `status` not `in_progress`, terminal phases, empty assignee, unresolvable Config, or a live Job — evidence: unit test table of skip cases asserts `SpawnJob` is not called for each (test exit 0)
- [ ] The concurrency cap is respected by the reconcile path: an over-cap assignee's task is deferred (not spawned immediately), consistent with the event path — evidence: unit test with a fake spawner reporting `CountActiveJobs == MaxConcurrentJobs` asserts no immediate `SpawnJob` (test exit 0)
- [ ] No double-spawn: a reconcile tick racing the Kafka event path for an uncapped assignee results in exactly one Job — evidence: unit test runs the two paths concurrently against a fake spawner and asserts exactly one `SpawnJob` (test exit 0); the per-assignee spawn lock is taken unconditionally in `spawnIfNeeded`
- [ ] Queue depth is observable across a restart: a Prometheus counter (e.g. `executor_reconcile_redriven_total`) increments per re-driven task and a log line records each reconcile pass's outcome — evidence: unit test asserts the counter increments for a re-driven task (test exit 0)
- [ ] **Post-Deploy (Rung-2):** on dev, after a deploy and a manual executor restart, a task left deferred pre-restart is re-driven without any vault edit — evidence: `kubectlnukedev -n dev logs <executor-pod> --since=30m | grep 'event=reconcile_redrive'` returns ≥1 line referencing the task id
  - `deploy_check:` `kubectlnukedev -n dev get pods -l app=agent-task-executor -o jsonpath='{.items[0].spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` to be filled with the exact tag at prompt time when `EXECUTOR_VERSION` is bumped
- [ ] **Post-Deploy (Rung-3):** in prod, per the task's Success Criteria — defer ≥10 tasks behind `maxConcurrentJobs: 1`, restart the executor, confirm spawning resumes and all N eventually complete — evidence: `kubectlnukeprod -n prod logs <executor-pod> --since=2h | grep 'event=reconcile_redrive'` returns ≥10 lines and the ≥10 tasks reach a terminal status in the vault
  - `deploy_check:` `kubectlnukeprod -n prod get pods -l app=agent-task-executor -o jsonpath='{.items[0].spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` to be filled with the exact tag at prompt time when `EXECUTOR_VERSION` is bumped

## Verification

### Container-executable (runs inside the dark-factory YOLO container at prompt time)

- `make precommit` — exits 0
- `go test ./pkg/...` — exits 0
- `grep -n 'reconcile' pkg/handler/task_event_handler.go` — returns ≥1 line
- `grep -n 'executor_reconcile_redriven_total' pkg/ -r` — returns ≥1 line

### Operator-executable (runs on the host after PR merge, spec verification ladder)

- Release + deploy per [[Deploy Mirrored Agent Service]]: bump `EXECUTOR_VERSION` in `nuke/agent/Makefile`, `cd ~/Documents/workspaces/nuke/agent && BRANCH=dev make apply`, then `BRANCH=master make apply` (prod == master)
- Dev: `kubectlnukedev -n dev rollout status sts/agent-task-executor --timeout=120s`, then restart the executor and confirm `event=reconcile_redrive` appears for a previously-deferred task
- Prod: defer ≥10 tasks behind `maxConcurrentJobs: 1`, `kubectlnukeprod -n prod rollout restart sts/agent-task-executor`, confirm spawning resumes and all N complete

## Desired Behavior

1. On startup, the reconcile loop runs one pass immediately (after the Kafka consumer is connected) and then on a fixed interval; each pass re-drives eligible tasks.
2. Each pass lists task files under the configured task directory via git-rest, parses frontmatter, and evaluates eligibility against the task's own state (`status`, `phase`, `assignee`) plus live Jobs.
3. Re-driving an eligible task reuses the existing `spawnIfNeeded` path, so the concurrency cap, trigger budget, and `current_job` grace-window logic apply identically to event-driven and reconcile-driven spawns.
4. The per-assignee spawn lock in `spawnIfNeeded` is taken unconditionally (not only when a cap is configured), so the reconcile tick and the Kafka consumer serialize count-then-spawn per assignee and cannot double-spawn.
5. A task with a live Job is never re-driven; a task over its assignee's cap is deferred (fast-path entry) rather than spawned.
6. Each reconcile pass records queue depth: a Prometheus counter per re-driven task and a log line summarizing the pass (tasks evaluated / re-driven / deferred / skipped).
7. git-rest unavailability degrades gracefully: the pass is skipped with a log line and the next tick retries; the executor never crashes or stops consuming because the vault is unreachable.

## Constraints

- Repo conventions are frozen: Ginkgo/Gomega v2 tests (no stdlib table tests), `github.com/bborbe/errors` wrapping (never `fmt.Errorf`), counterfeiter mocks for any new dependency (`//counterfeiter:generate`), glog `V(n)` gating.
- The executor keeps its role: it does not gain a vault mount or a filesystem view of the vault; the vault is read through git-rest (`/api/v1/files`, `List`/`Get`), the same client shape `agent-task-controller`'s `pkg/gitrestclient` already uses. The client code is ported into this repo (do not add a cross-repo import of the controller).
- The Kafka event path and `deferIfAtConcurrencyCap`/`checkActiveCurrentJob` behavior are unchanged for event-driven spawns; `spawnIfNeeded`'s only change is the unconditional lock.
- The in-memory `deferredRespawns` map stays as a fast path; the reconcile loop is the source of truth floor and must not be gated on map contents.
- New executor env config: git-rest base URL, gateway secret (referenced by secret name, matching the existing `JobKafkaClientCertSecret` pattern), and the task directory glob. New env vars are documented in the chart and `README.md`.
- Reconcile interval is a package constant (`defaultReconcileIntervalSeconds = 60 * time.Second`), mirroring `DefaultZombieSweeperIntervalSeconds`; no CRD field and no env override in this change.
- CHANGELOG: add an `## Unreleased` bullet (section exists — HEAD is v0.8.3).

## Failure Modes

| Trigger | Expected behavior | Recovery |
|---|---|---|
| git-rest unreachable / 503 | Pass skipped, `event=reconcile_vault_unavailable` logged; Kafka consumption and spawning unaffected | Next tick retries automatically; no operator action |
| git-rest rate-limits `List`/`Get` or the `List`/`Get` contract drifts | Pass skipped with a log line; no re-drive in that pass | Next tick retries; interval constant tunable in code |
| git-rest slow or vault large | Pass bounded by a per-pass timeout; logs evaluate count so cost is visible | Next tick retries; the interval constant is tunable in code if cost stays high |
| Task has a live Job | Skipped (no re-drive) — `IsJobActive` check under the spawn lock | n/a |
| Assignee over cap | Task deferred (fast-path entry), not spawned | Next tick re-evaluates after cap frees |
| Config unresolvable (assignee not a Config CR) | Skipped and counted in the skip log, same as the event path | Operator fixes the Config CR or assignee |
| Reconcile and Kafka event race for the same task | Exactly one Job — per-assignee lock serializes; second path sees the live Job and skips | n/a |
| Restart mid-reconcile | Nothing persisted, nothing lost — next startup pass re-evaluates from the world | n/a |

## Assumptions

- The executor continues to have no vault mount or filesystem view; the vault is read only through git-rest, which is deployed alongside the vault (the same service the controller already reads through).
- git-rest exposes `List(glob)` and `Get(path)` for the vault repo, and its `/readiness` returns 503 while unavailable — the contract the controller's existing client already relies on.
- A task whose Job is active (or whose `current_job` Job still exists) must not be re-spawned; `IsJobActive` is the authority.
- The task directory glob is configurable per instance via env (default `24 Tasks/*.md`), matching how vaults place task files.
- Config CRs remain the authority for assignee→image mapping and concurrency caps, unchanged by this work.

## Security / Abuse Cases

- git-rest reads are read-only (`List`/`Get`); the executor never writes vault files through git-rest.
- The git-rest gateway secret is referenced by secret name (the existing `JobKafkaClientCertSecret` pattern), never logged or embedded in the image.
- The reconcile loop only spawns Jobs for tasks whose assignee resolves to a registered Config CR — the same routing gate as the event path — so it cannot be pointed at arbitrary content.
- No new network listeners or user-input surfaces are added by this change.

## Do-Nothing Option

Doing nothing leaves the invariant documented at `task_event_handler.go:637-668` ("a stuck task does not remain stuck for want of a Kafka event that will never arrive") violated on every restart. Observed cost: 74 tasks orphaned for an unbounded window in prod on 2026-08-16 with no error signal, and the defect compounds as the concurrency cap improves (more tasks deferred → more tasks lost per restart). Restarts are routine — deploys, node drains, OOM, Keel digest updates — so this is a recurring, silent, destructive failure, not an acceptable steady state.

## Suggested Decomposition

Prompts should be generated in this order — each row is a single prompt with a clear scope.

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | git-rest vault reader client (`List`/`Get`/`IsReady`) + executor env wiring (URL, gateway secret by name, task-dir glob) + counterfeiter mock | 2, 7 | 1 | — |
| 2 | Reconcile loop in `taskEventHandler`: startup pass + ticker, eligibility filter, re-drive via `spawnIfNeeded`, metrics + log lines, CHANGELOG entry | 1, 2, 3, 5, 6, 7 | 1, 2, 3, 4, 6 | prompt 1 |
| 3 | Unconditional per-assignee spawn lock in `spawnIfNeeded` + concurrent double-spawn regression test | 4 | 5 | — |
| 4 | Chart + README updates for the new executor env vars | — | 1 | prompt 1 |

Rationale: prompt 1 establishes the vault-read contract; prompt 2 is the core loop on top of it; prompt 3 is the small concurrency hardening that can land independently; prompt 4 is the deploy-surface + docs, depending only on the env vars from prompt 1.

## Open Questions

- None.
