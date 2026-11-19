---
status: prompted
approved: "2026-11-19T08:57:31Z"
generating: "2026-11-19T09:03:53Z"
prompted: "2026-11-19T09:14:29Z"
branch: dark-factory/run-claim-spawn-gate
---

## Summary

- The strategy-prototype per-idea cap is armed only at publish, so an idea is uncapped for the whole run — measured **21 m 47 s** on `prototype-gbpjpy-orb-breakout-m15` (`job_started_at 2026-11-16T21:03:53Z` → `run_id` first visible `21:25:40Z`), with two distinct Jobs admitted on that idea (`…-20261116210353`, `…-20261116211853`) and two the night before.
- It cannot be fixed inside the run: a `done` refusal is parked as `status: completed, phase: done` (`park_succeeded.go:190-210`, `result_publisher.go:1153-1168`), and a `failed` refusal advances `maxConsecutiveRedrives = 3` and can abort the idea. No in-run result status means "refused, retry after the TTL".
- Fix: the **spawn funnel** refuses to start a second Job for a task whose `run_claim` is live. Suppression is a deferral — no Job, no result, no failure, no redrive streak.
- Companion change: `bborbe/trading` writes `run_claim` at the first step of the execution phase. That write is specified separately; this spec owns the gate that reads it.
- ⚠️ **Supersedes** the draft `strategy-prototype-run-claim-caps-from-first-step` on `bborbe/trading` branch `feat/run-claim` (commit `9bf1ba1b90`), which put the refusal inside the run. It is kept on that branch for the record; the reason it cannot work is the `park_succeeded` / `redrive` citation above.

## Problem

`NewRunIDStep` in `bborbe/trading` refuses a second run only when `run_id` is present, and `run_id` is stamped only after a successful publish — the last step of the run. Between admission and publish the idea looks unrun, so every admission in that window is legal.

The executor already has a liveness gate, `suppressForActiveCurrentJob` (`pkg/handler/job_lookup_denied.go:100`), but it keys on `current_job`, which the run itself clears. On the measured night `current_job` was cleared at `21:13:40Z` and a second Job started at `21:18:53Z` — the liveness signal was gone while the run was still in flight.

**This is not the dispatch claim.** The repo's existing "claim" (`pkg/claim`, `acquireSpawnClaim`, `docs/claim-lease-protocol.md`) is a k8s Lease held for the *dispatch* of one Job and released when that Job ends. The trading run outlives the Job that started it — planning and execution are separate spawns — so the lease is already free when the duplicate arrives. `run_claim` is a *frontmatter* fact about the run, not a lease about the dispatch.

## Goal

The spawn funnel refuses to start a second Job for a task that carries a live `run_claim`, and the refusal is a deferral: the task is not parked, not failed, and not counted against its redrive budget. A stale or absent claim leaves today's behaviour unchanged.

## Non-goals

- The trading-side `run_claim` write. Specified separately; this spec assumes it exists.
- Changing `maxConsecutiveRedrives`, `maxConcurrentJobs`, the run window, or `park_succeeded` behaviour.
- Proving **concurrency** of two runs. The surviving record cannot settle whether the two 2026-11-16 admissions overlapped; this gate prevents the second spawn rather than proving the first two overlapped.
- Replacing `current_job`. The two gates are complementary: `current_job` covers spawn→first-step, `run_claim` covers first-step→publish.
- Any change to the claim store, the dispatch lease, or `claim_mode`.

## Assumptions

- **The task frontmatter is readable at spawn.** The funnel already reads `current_job`, `job_started_at`, `trigger_scope` and `max_triggers` from the same object; `TaskFrontmatter.String(key)` reads ad-hoc keys, so no `bborbe/agent` change is needed.
- **`run_claim` is written within seconds of the Job starting**, by the companion trading change, so the uncovered window is spawn→first-step — already covered by `current_job`.
- **`run_claim` is encoded as a quoted RFC3339 string.** A bare YAML date decodes to `time.Time` and re-serializes at midnight UTC, shifting the instant by up to 24 h. This is a shared contract with the companion spec.
- **Suppression is free.** A suppressed spawn creates no Job and publishes no result, so it cannot advance `maxConsecutiveRedrives` or trigger `park_succeeded`. Verified: every durable write in the funnel sits below the insertion point, and all three callers discard `spawnOutcomeSuppressed` (Kafka at `task_event_handler.go:568`; the deferred loop clears only an in-memory stamp; reconcile tallies a counter).
- **`DefaultRunClaimTTL` matches the run budget** — `2 * time.Hour`, matching `runBudgetSeconds: 7200` in the dev Config. It is a single global constant while the budget is per-Config; a second agent with a larger budget would under-cover.
- **On the deferred-respawn path the task object is a snapshot taken at park time** (`deferredEntry{task}` at `task_event_handler_deferred.go:23-27`). The Kafka and reconcile paths carry current data; the deferred replay may not.

## Desired Behavior

1. **New gate in the spawn funnel.** A liveness check is added to `spawnIfNeededClaimed`, placed after the `current_job` gate and before the assignee spawn lock.

2. **Live claim suppresses.** When the task frontmatter carries `run_claim`, it parses as RFC3339, and `now - claim < DefaultRunClaimTTL`, the funnel returns the suppressed outcome without spawning, logging at INFO (`glog.Infof`, the level the sibling `respawn_grace_window` uses) `event=spawn_suppressed_run_claim` with the task id, the claim timestamp and the age, and incrementing `TaskEventsTotal` with label `skipped_run_claim`.

3. **No side effects.** A suppressed spawn writes no frontmatter, parks nothing, publishes no result, and does not touch the redrive streak. It is the same deferral shape as the `current_job` gate.

4. **Absent, stale or unparseable claim spawns.** A missing `run_claim`, one at or beyond `DefaultRunClaimTTL`, or one that does not parse as RFC3339 leaves today's behaviour unchanged — the spawn proceeds.

5. **`current_job` still runs first.** The existing gate keeps its position and semantics; with both set, the `current_job` path suppresses and the new event is not emitted.

6. **The reconcile floor re-drives.** Because suppression is a deferral, the next reconcile pass re-evaluates the task once the claim expires. No operator action is needed to resume it.

## Constraints

- No change to `park_succeeded.go`, `recent_success.go`, or the redrive budget.
- No change to the `current_job` gate's behaviour or ordering.
- The new gate is read-only: it must not write frontmatter on either path.
- Factory composition stays pure; the gate is a method on `taskEventHandler`, not a new dependency.
- `run_claim` is a new key name shared with the trading change; it must not be renamed on one side only.
- **Accepted cost:** a run that *fails* leaves `run_claim` set, so its redrive is suppressed until the TTL — up to 2 h for a run that died in 30 s. With a 10 h window and a concurrency cap of 2, that is roughly 5 attempts per night instead of many. This is the trade the gate makes; it is not a defect to be worked around in implementation.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection | Concurrency |
|---|---|---|---|---|
| Companion trading change not deployed | Gate never fires; behaviour identical to today | Deploy the trading change | No `run_claim` in any task file | No regression below today |
| Run fails before publish | Redrive suppressed until `DefaultRunClaimTTL`; the idea retries after the TTL | Wait for the TTL | Claim older than the TTL in the task file | At most one retry per TTL per idea |
| Run crashes after writing `run_claim` | Spawns suppressed until the TTL elapses, then proceed | Wait for the TTL | Claim older than the TTL | A crashed run blocks its idea for at most 2 h |
| `run_claim` malformed or bare-date | Treated as stale; spawn proceeds | None needed | Value does not parse as RFC3339 | n/a |
| Deferred-respawn replay carries a pre-write snapshot | Gate sees no claim and spawns — the pre-spec behaviour | None; the next pass sees current data | `run_claim` absent from the replay's task object | Narrow: window-close entries are replayed at the reopen tick |
| Clock skew between executor and agent pod | Age computed from the executor's clock | None needed | n/a | Skew shifts the TTL by the skew, far below 7200 s |
| Two goroutines reach the funnel together | The existing per-assignee spawn lock still serialises them | None needed | n/a | The gate is read-only and adds no lock of its own |

## Acceptance Criteria

- [ ] A live `run_claim` suppresses the spawn — evidence: a handler test asserts the suppressed outcome and no `SpawnJob` call, and `make test` exits 0.
- [ ] The suppression logs and counts — evidence: the test asserts the `spawn_suppressed_run_claim` path is taken and `TaskEventsTotal` gains the `skipped_run_claim` label.
- [ ] A suppressed spawn writes nothing — negative evidence: the test asserts no `UpdateFrontmatterCommand` and no park command was published.
- [ ] A missing `run_claim` spawns — evidence: the handler test asserts a spawn proceeds, and `make test` exits 0.
- [ ] A stale `run_claim` spawns — evidence: the test seeds a claim older than `DefaultRunClaimTTL` and asserts a spawn proceeds.
- [ ] A malformed `run_claim` spawns — evidence: the test seeds `run_claim: not-a-timestamp` and asserts a spawn proceeds.
- [ ] The `current_job` gate wins when both are set — evidence: a test with an active `current_job` and a live `run_claim` asserts the `current_job` path suppresses and the `spawn_suppressed_run_claim` event is not emitted.
- [ ] A suppressed task is re-driven once the claim expires — evidence: a test with a fake clock advances past the TTL and asserts the next pass spawns.
- [ ] The `current_job` gate is unchanged — evidence: the existing `job_lookup_denied` tests pass unchanged (`make test` exits 0).
- [ ] **Post-Deploy (Rung-2):** a task carrying a live `run_claim` is suppressed in dev — evidence: seed a dev task with `run_claim: <now>`, no `current_job`, the prototype assignee and an eligible phase; within two reconcile ticks `kubectlnukedev -n agent logs deploy/agent-task-executor-trading --since=5m | grep 'spawn_suppressed_run_claim'` returns ≥1 line naming that task, and the task file is unchanged afterwards.
  - `deploy_check:` `kubectlnukedev -n agent get pods -l app=agent-task-executor,vault=trading -o jsonpath='{.items[*].metadata.creationTimestamp}'`
  - `deploy_target:` a creation timestamp newer than `$(date -u -r $(git log -1 --format=%ct origin/master) +%Y-%m-%dT%H:%M:%SZ)`

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make test` — unit + handler suite passes, including the new gate cases
- `make precommit` — lint, format, generate clean
- `grep -n 'spawn_suppressed_run_claim' pkg/handler/*.go` — the gate's log event exists

### Operator-executable (runs on the host after PR merge)

- Image publish, `EXECUTOR_VERSION` bump in `bborbe/nuke` `agent/Makefile`, `/nuke-deploy dev agent`
- `kubectlnukedev -n agent get pods -l app=agent-task-executor,vault=trading -o jsonpath='{.items[*].metadata.creationTimestamp}'` — newer than the merge
- Seed the fixture task from AC 10 and observe the suppression line at any hour

## Do-Nothing Option

The cap stays armed only at publish, and an idea remains uncapped for the full duration of every run — 21 m 47 s on the measured example, with two admissions per night observed. The nightly window keeps paying for duplicate runs of one hypothesis. The cost of the fix is the accepted 2 h retry delay after a failed run; the cost of not doing it is duplicate runs every night, indefinitely.

## Suggested Decomposition

Two layers (handler gate + tests) in one repo. One prompt.

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | The `run_claim` gate in the spawn funnel, its log event and metric, and handler tests | 1-6 | 1-9 | — |
