---
status: verifying
approved: "2026-10-23T09:20:26Z"
generating: "2026-10-23T09:22:53Z"
prompted: "2026-10-23T09:37:49Z"
verifying: "2026-10-23T10:01:26Z"
branch: dark-factory/bug-redrive-streak-lost-across-clusters-and-restarts
---

## Summary

- The executor stops re-spawning a task after 3 consecutive spawns at an unchanged phase (the redrive cap). The count lives only in one pod's memory.
- With several clusters sharing one vault, and pods that restart on every rollout, the count is spread across processes and reset by restarts, so a task stuck at one phase is never retired: one task failed the same review phase 4 times on 2026-10-23 across two clusters.
- The fix keeps the count in the shared claim store that all clusters already use for dispatch claims, one small record per spawn, so every executor in every cluster reads the same streak.
- The per-pod count stays in place as a safety floor: an unreadable store never lets a task through that the pod already capped, and never silently resets a streak.
- When the claim store is not in use (claim mode off), behaviour is exactly today's per-pod cap. A restart no longer un-caps a task; an operator un-caps it by deleting its streak records.

## Problem

The redrive cap is the last bound on a task that keeps failing at the same phase without advancing: the trigger budget is disarmed by design for tasks with no `ref` and no `max_triggers`, and is reset on every spawn for tasks whose scope reads as changed. The cap's counter is a per-process map. Since several clusters (nuke dev, nuke prod, sun) dispatch from one shared vault under the claim protocol, consecutive spawns of one task land on different pods, each of which sees a streak of 1 or 2 and never reaches 3; every rollout or crash also empties the map. The loop the cap exists to stop therefore continues indefinitely, burning a Job (and its model budget) per attempt.

## Goal

A task spawned 3 times in a row at an unchanged phase is retired from re-driving no matter which cluster or which pod instance performed each spawn, and no matter how many executor restarts happened in between. The streak still resets when the task is spawned at a new phase and is removed when the task reaches a terminal state. Deployments without a claim store keep today's per-process cap unchanged.

## Assumptions

- All clusters that dispatch from one vault share one git-rest instance (one claim namespace). True for nuke dev, nuke prod and sun today.
- git-rest never reaps or expires a ref on its `lease_until`; only explicit deletes remove refs.
- The existing claim-key component validation is the authority for what may appear in a key.

## Non-goals

- Do NOT store the streak in task frontmatter — agent pods rewrite frontmatter from a pre-spawn snapshot and would clobber it (the reason documented on `maxConsecutiveRedrives`).
- Do NOT change the cap value (3), the reset rule (spawn at a new phase), or the terminal-state clear — invariants; if a future consumer demands variation, that's a separate spec.
- Do NOT add a config knob, env var or opt-out for the persisted streak — invariant; it engages whenever claim mode takes a claim.
- Do NOT add a reaper for streak refs of tasks that never reach a terminal state (deleted vault files). They are bounded to at most 3 current-phase refs per task (plus any whose delete failed); a reaper is a separate spec.
- Do NOT add new metrics. `executor_redrive_capped_total` stays the alerting signal; store failures are visible as warning log lines.
- Do NOT change the dispatch claim keys (`claims/task/...`, `claims/repo/...`), the lease constants, or the fence/takeover behaviour for those keys.
- Do NOT count spawns whose trigger-count write fails: the handler returns before recording the streak on that path today, and that stays so (spawns where the trigger-count record fails stay uncounted).

## Reproduction

### Observed (2026-10-23, executor v0.30.5, `CLAIM_MODE=on` on nuke prod and sun)

Operator-reported timeline; the Jobs are garbage-collected since (Job TTL 1800 s):

```
task 1788bf3c   phase: ai_review (unchanged across all four attempts)
08:14Z  Job spawned on sun       -> failed
08:21Z  Job spawned on sun       -> failed
08:27Z  Job spawned on sun       -> failed      (sun pod restarted between attempts — streak lost)
08:32Z  Job spawned on nuke prod -> failed      (prod pod's map holds no streak for this task)
```

No-cap evidence: `executor_redrive_capped_total` (a plain counter, not log-level gated) did not increment on either cluster for the window, and no `event=redrive_capped task=1788bf3c` line appeared. Root cause: the redrive streak in the task event handler is a per-process map; the `maxConsecutiveRedrives` doc comment states the streak is in-memory and "a restart (which empties the streak)" — written before multi-cluster dispatch existed.

### Replayable pre-fix repro (nuke dev, any release before the fix — dev runs v0.30.5, v0.30.6 has the same in-memory streak)

1. Create a dev task whose agent fails deterministically at one phase, with no `ref` and no `max_triggers` (trigger budget disengaged).
2. After its 2nd Job has ended, run `kubectlnukedev -n dev rollout restart deploy/agent-task-executor-personal`.
3. Result pre-fix: **5** Jobs for the task (2 before the restart, 3 after — the new pod starts with an empty streak) before the cap fires.

The restart replay also covers the cross-cluster mechanism: in both cases a process that does not hold the in-memory streak makes the next spawn decision, and post-fix it reads the streak from the shared store.

## Expected vs Actual

- **Expected** (`maxConsecutiveRedrives` doc comment, "a task pinned at one phase across three consecutive spawns has not [advanced], and stays capped until something moves it"): the 4th spawn at `ai_review` is refused, `executor_redrive_capped_total` increments and `event=redrive_capped task=1788bf3c streak=3 cap=3 phase="ai_review"` is logged.
- **Actual**: the 4th spawn ran on a different cluster with no cap engagement, because that pod's in-memory streak for the task was empty.

## Why this is a bug

The cap is documented as "the only [bound] that covers every path". Its accepted cost was "a task stuck across a restart is re-driven up to N more times, not that it loops forever". With pods restarting on every rollout and three clusters alternating spawns, neither half of that holds: the streak never reaches N on any single process, so the task loops without bound.

## Acceptance Criteria

- [ ] **AC1** A streak store reads counts it never wrote: (a) over the in-process fake git-rest pre-seeded (via its `Seed` helper) with `claims/redrive/T/ai_review/1` and `claims/redrive/T/ai_review/2`, a freshly constructed store reports count 2 for (T, `ai_review`) and 0 for (T, `planning`); (b) after store A records 2 spawns of task U at `ai_review`, a separately constructed store B reports count 2 for (U, `ai_review`) — evidence: Ginkgo specs in `pkg/claim` pass (`go test ./pkg/claim/...` exit 0) asserting all three counts.
- [ ] **AC2** Each recorded spawn creates exactly one ref at key `claims/redrive/<taskid>/<phase>/<n>` with cluster field `redrive-streak`, task id T and an RFC3339 `lease_until` 24 h after the injected clock — evidence: spec asserts the fake's stored refs (`…/ai_review/1`, `…/ai_review/2`, cluster `redrive-streak`) and that every create PUT carried an empty `expected_sha`.
- [ ] **AC3** A create answering 409 is retried on a re-listed next `n`, at most 3 attempts in total — evidence: spec injects 409 on the first PUT and asserts the second PUT lands at the next free `n`; a second spec injects 409 on every PUT and asserts exactly 3 PUTs were sent and the record returns an error.
- [ ] **AC4** Reset, clear and foreign keys — evidence: spec asserts (a) after records at `ai_review` ×2 then `planning` ×1, the fake holds only `…/planning/1`; (b) after clear it holds no ref under `claims/redrive/<taskid>/`; (c) refs of another task id are untouched by (a) and (b); (d) a seeded ref under `claims/redrive/T/` whose key does not parse as `claims/redrive/<taskid>/<phase>/<n>` (e.g. `claims/redrive/T/ai_review/x`) is not counted and not deleted by a record.
- [ ] **AC5** An invalid key component is never sent: count, record and clear with a task id containing `/`, or a phase failing the claim-component validation, send zero requests to the fake git-rest and return an error — evidence: spec asserts the fake's recorded request list is empty and each call returned an error.
- [ ] **AC6** Lease-manager `Recover` never touches a streak ref, and the protocol doc says so — evidence: spec in `pkg/claim` seeds a `claims/redrive/<taskid>/ai_review/1` ref whose cluster field equals the manager's own cluster name and whose `lease_until` is inside the fence window, runs `Recover`, and asserts zero `DELETE` requests, zero Job-control calls and the ref still present; `grep -n 'claims/redrive/' docs/claim-lease-protocol.md` returns ≥1 line in the Restart recovery section and ≥1 in a new streak-refs section, and that streak-refs section documents the operator un-cap command (`grep -c 'DELETE /api/v1/refs/' docs/claim-lease-protocol.md` ≥1).
- [ ] **AC7** Two handler instances that share no memory, both built with the real streak store over one shared faked claim-ref transport, alternate spawning one task at an unchanged phase: spawns total exactly 3, the 4th attempt on either instance spawns no Job and logs `event=redrive_capped task=<id> streak=3 cap=3 phase="<phase>"`; separately, a freshly constructed instance (a restarted pod) over a transport pre-seeded with 3 current-phase streak refs it never wrote refuses on its first attempt — evidence: Ginkgo specs in `pkg/handler` assert combined `SpawnJobCallCount() == 3` with 0 further spawns after the 4th and 5th attempts, and `SpawnJobCallCount() == 0` for the pre-seeded instance (`go test ./pkg/handler/...` exit 0).
- [ ] **AC8** Store fail-safe — evidence: handler specs assert: (a) with the transport's list failing and the in-memory streak at 3 for the current phase, the spawn is refused and the transport's `ListCallCount()` is unchanged by the decision (memory short-circuit); (b) with the list failing and no in-memory streak, the spawn proceeds, a warning containing `event=redrive_streak_read_failed task=<id>` is logged, and no DELETE is sent; (c) with every PUT failing, the spawn is not rolled back, the in-memory streak still increments, and a warning containing `event=redrive_streak_write_failed task=<id>` is logged; (d) with a task id or phase that fails key validation, the transport receives zero streak requests, `event=redrive_streak_write_failed task=<id>` is logged, and the in-memory cap still refuses the 4th spawn.
- [ ] **AC9** Claim mode `off` is unchanged — evidence: a spec with mode `off` asserts the claim-ref transport's call count is 0 after 4 spawn attempts (3 spawns, 1 capped); `git diff $(git merge-base HEAD origin/master) -- pkg/handler/task_reconcile_test.go` shows no removed or edited `Expect(` lines in "still re-drives a task whose Job failed, up to the redrive cap" and "resets the streak when the task advances to a new phase".
- [ ] **AC10** Claim-mode wiring shares one transport and leaves claim accounting intact — evidence: `TestLeaseManagerConstructsOnce` passes unmodified, and a new spec that obtains both the lease manager and the streak store (in either order, twice each) asserts the claim-ref client factory was called exactly once and both received the same client instance; `git diff $(git merge-base HEAD origin/master) -- pkg/handler/task_event_handler_claim_test.go` shows "AC7(c): a redrive-capped task makes no additional claim request" with no removed or edited `Expect(` lines; and every `expectClaimDelta` assertion in that file passes, i.e. streak traffic increments no `executor_claim_total{result}` series.
- [ ] **AC11** The in-memory-by-design rationale is replaced — evidence: `grep -c 'The streak is in-memory, and that is load-bearing' pkg/handler/task_event_handler.go` returns 0, `grep -c 'deliberately not persisted' pkg/handler/task_event_handler.go` returns 0, `grep -c 'claims/redrive/' pkg/handler/task_event_handler.go` returns ≥1, and the frontmatter-clobber explanation is retained (`grep -c 'clobbered' pkg/handler/task_event_handler.go` ≥ 1).
- [ ] **AC12** CHANGELOG carries the fix under Unreleased — evidence: `awk '/^## Unreleased/{f=1;next} /^## /{f=0} f' CHANGELOG.md | grep -c 1788bf3c` returns ≥1.
- [ ] **AC13** **Post-Deploy (Rung-2):** on nuke dev, the restart leg of the repro is the discriminating check (pre-fix 5 spawns, post-fix 3): a fresh task forced to fail at one phase, with `rollout restart` after its 2nd Job ended, is spawned exactly 3 times and then capped, with no further spawn within 10 min of the 3rd Job ending — evidence: the `spawned job for task <id>` log lines (glog V(2)) counted on the pre-restart pod (captured before the restart) plus the post-restart pod sum to exactly 3; the post-restart pod logs ≥1 `event=redrive_capped task=<id> streak=3 cap=3` line (primary, task-specific evidence); and `executor_redrive_capped_total` on the post-restart pod is ≥1 higher than its baseline read right after the restart (secondary — counters reset on restart and are not task-specific).
    - `deploy_check:` `kubectlnukedev -n dev get deploy/agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
    - `deploy_target:` `v0.30.8`

No new scenario: the cross-instance behaviour is reachable with two handler instances over a shared fake transport (AC7); the live run is the bug-workflow replay (AC13), not a scenario.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — exits 0
- `go test ./pkg/claim/... ./pkg/handler/...` — exits 0; AC1–AC10 specs green
- `grep -c 'The streak is in-memory, and that is load-bearing' pkg/handler/task_event_handler.go` — `0`
- `grep -n 'claims/redrive/' docs/claim-lease-protocol.md` — ≥2 lines
- `awk '/^## Unreleased/{f=1;next} /^## /{f=0} f' CHANGELOG.md | grep -c 1788bf3c` — ≥1

### Operator-executable (runs on the host after PR merge and release, spec verification ladder)

1. Deploy v0.30.8 to nuke dev; confirm the image tag with the `deploy_check:` above. Confirm claim mode and log level on the running Deployment: `kubectlnukedev -n dev get deploy/agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CLAIM_MODE")].value} {.spec.template.spec.containers[0].args}'` prints `on` and an arg `-v=2` or higher (V(2) is needed for the spawn line, V(1) for the cap line; `-v=2` as of 2026-10-23).
2. (After the restart in step 4, on the new pod) read the cap counter baseline: `kubectlnukedev -n dev get --raw "/api/v1/namespaces/dev/pods/<executor-pod>:9090/proxy/metrics" | grep '^executor_redrive_capped_total'`.
3. Create a **fresh** dev task in the personal vault whose agent fails deterministically at one phase (no `ref`, no `max_triggers`).
4. After the 2nd Job ends, save `kubectlnukedev -n dev logs deploy/agent-task-executor-personal | grep -c 'spawned job for task <id>'` (expect 2), then run `kubectlnukedev -n dev rollout restart deploy/agent-task-executor-personal`.
5. 10 min after the 3rd Job ends: `kubectlnukedev -n dev logs deploy/agent-task-executor-personal | grep -c 'spawned job for task <id>'` — expect 1 (sum with step 4 = 3); `… | grep 'event=redrive_capped task=<id>'` — ≥1 line with `streak=3 cap=3`; re-read the counter on the new pod — ≥1.
6. Set the task's `status: aborted` (a terminal state, which triggers the clear); confirm `GET /api/v1/refs?prefix=claims/redrive/<taskid>` on git-rest returns `[]` — the only runtime check of the terminal clear. Then delete the task file.

## Desired Behavior

1. When claim mode takes a claim (`on` or `shadow`), every successful spawn is recorded as one new ref under `claims/redrive/<taskid>/<phase>/<n>` in git-rest, where `n` is one more than the highest `n` already present for that task and phase. The ref is created by compare-and-swap with an empty expected sha; on 409 the executor re-lists and tries the next free `n`, for at most 3 attempts. Any non-409 error ends the record with no retry. The ref carries cluster `redrive-streak`, the task id, and `lease_until` = now + 24 h (informational only; nothing expires or renews it). A task with no phase uses the phase component `none`. Listed refs whose key does not parse as `claims/redrive/<taskid>/<phase>/<n>` (integer `n` ≥ 1) are ignored by count, record and reset.
2. Recording a spawn at phase P deletes the task's refs at every phase other than P (the phase-change reset). A task reaching a terminal state deletes all of its streak refs. A 409 or not-found on these deletes is ignored; any other error is logged as a warning and does not block the spawn or the terminal handling.
3. The streak is the number of the task's refs at its current phase; refs at any other phase never count, so a task that has just moved phase is not capped by its old streak. The cap check refuses a spawn when max(store count at the current phase, in-memory count at the current phase) ≥ 3. When the in-memory count alone already reaches 3, the store is not read. The refusal keeps today's signals: `executor_redrive_capped_total` increments and `event=redrive_capped task=<id> streak=<n> cap=3 phase="<phase>"` is logged at V(1), with `streak` showing the max used.
4. The in-memory streak is maintained exactly as today in every mode and acts as the floor. A failed or timed-out store read logs a warning containing `event=redrive_streak_read_failed task=<id>` and the decision uses the in-memory count alone; the dispatch pass continues. A bad read never retires a task memory has not capped and never deletes or resets stored refs. A failed store write logs a warning containing `event=redrive_streak_write_failed task=<id>` and the in-memory streak is still updated; the already-spawned Job is untouched.
5. When claim mode is `off`, no streak store is constructed and no git-rest request is made; the cap behaves exactly as before this change.
6. The lease manager's startup recovery skips every key under `claims/redrive/` regardless of its cluster field, and no other lease-manager path (renew, fence, takeover, release, audit) ever acts on a streak ref. A streak ref therefore never triggers a fence, which would delete the task's live Job. Streak traffic never increments any `executor_claim_total{result}` series.
7. A task id or phase that is not a valid claim key component (per the existing key validation) is not persisted: no request is sent, the executor logs `event=redrive_streak_write_failed task=<id>`, and the in-memory streak alone bounds that task.
8. Un-capping is explicit. An executor restart no longer clears a cap, and a phase round-trip (P → Q → P) with no spawn at Q does not reset it, because the reset happens only when a spawn is recorded at a new phase. A capped task spawns again only after (a) an operator deletes its refs under `claims/redrive/<taskid>/` through git-rest, or (b) the task moves to another phase and is actually spawned there, or (c) it reaches a terminal state.

## Constraints

- Claim protocol invariants in `docs/claim-lease-protocol.md` are unchanged for dispatch claims; the doc gains a section describing streak refs (key shape, sentinel cluster, 24 h informational lease, never recovered/fenced/renewed, how an operator clears one) and a Restart-recovery bullet stating `claims/redrive/` keys are skipped.
- git-rest accepts keys only under `claims/` and refuses an empty cluster field — hence the `claims/redrive/` prefix and the `redrive-streak` sentinel. Both values are frozen: changing either strands existing refs.
- Wiring: the streak store receives the SAME claim-ref client instance the lease manager receives, built once inside the existing one-time lease-manager construction; `TestLeaseManagerConstructsOnce` must still hold. No new git-rest client, URL or secret; the store inherits the client's 30 s request cap.
- Dispatch-claim specs that assert absolute Put/List/Delete call counts on the shared fake claim-ref client (e.g. `task_event_handler_claim_test.go` "AC6: on spawns after taking over an expired lease", `PutCallCount() == 2`) may be re-scoped to count only `claims/task/` and `claims/repo/` keys. This is a scope correction for the new shared transport, not a weakening: the asserted dispatch-claim behaviour stays identical.
- The cap value 3, the phase reset and the terminal clear are unchanged. The cap check stays inside the single spawn funnel the Kafka consumer, deferred-respawn loop and reconcile floor all pass through, and a spawn is recorded only after the Job create succeeded (spawns, not attempts).
- The `event=redrive_capped` log line format and `executor_redrive_capped_total` name/labels are unchanged.
- Repo conventions: Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, counterfeiter mocks for new interfaces, glog `V(n)` gating, injected clock for `lease_until` (no wall-clock reads in tests).
- All other existing tests in `pkg/claim` and `pkg/handler` pass without weakened assertions.
- The frontmatter-clobber rationale in the `maxConsecutiveRedrives` comment stays (it explains why frontmatter is rejected); the claims "in-memory by design" and "a restart empties the streak" are replaced by the store design, the fail-safe floor and the explicit un-cap rule (DB8).

## Failure Modes

Per-decision request budget: a cap check below the in-memory cap costs 1 List; a record costs 1 List + ≤3 Puts + one Delete per other-phase ref; a terminal clear costs 1 List + one Delete per ref. A capped task whose in-memory count is at 3 costs 0 requests per decision.

| Trigger | Expected behavior | Recovery | Detection |
|---|---|---|---|
| git-rest unreachable / 5xx on cap-check list | Decision uses in-memory count; spawn proceeds only if memory is below 3 | Automatic on the next decision once git-rest answers | `event=redrive_streak_read_failed` warning |
| git-rest throttles (429) on list or put | Treated as a read or write error: no retry beyond the 409 path; read → memory fallback, write → memory still counts | Automatic on later decisions | read-failed / write-failed warning |
| git-rest slow, approaching the 30 s request cap | One List per decision is bounded by 30 s, inside the 45 s default reconcile pass timeout; on timeout the decision falls back to memory and the pass continues with the next candidate. Same exposure as the existing dispatch claim call | Automatic | read-failed warning; pass duration in existing reconcile logs |
| git-rest write fails on record PUT | Job stays running; in-memory streak increments; the store count lags by one for the rest of that streak | Same-pod decisions still bounded by memory; cross-cluster bound weakens by one spawn for this streak | `event=redrive_streak_write_failed` warning |
| Two clusters record the same task/phase at once | One PUT wins at `n`; the loser gets 409, re-lists, writes `n+1`; both spawns counted | None needed; on 3 exhausted attempts → write-failed warning, memory still counts | none on normal path |
| Pod restarts between spawns 2 and 3 | New pod reads count 2 from the store; spawn 3 allowed, spawn 4 refused | None | `event=redrive_capped`, counter increment |
| Pod crashes after Job create, before record | That spawn is not counted (same as today) | Next spawn records normally; at most one uncounted spawn per crash | none |
| Task moves phase while an old-phase ref delete fails | Old-phase refs linger but never count while the task is at another phase; the next record retries the delete | Automatic on the next record | write-failed warning |
| Operator wants to retry a capped task | Task stays capped across restarts and across a phase round-trip with no spawn | Delete the refs under `claims/redrive/<taskid>/` via git-rest, or move the task to another phase and let it actually spawn there | git-rest List for prefix `claims/redrive/<taskid>/` returns empty; the next decision logs `spawned job for task <id>` |
| Task file deleted, never reaches terminal | At most 3 current-phase refs (plus any whose delete failed) linger with a past informational lease | Operator deletes `claims/redrive/<taskid>/` refs via git-rest; no automatic reaper (Non-goal) | refs visible in the git-rest `claims/redrive/` listing |
| Rollout version skew (old and new pods both dispatching) | Old pods neither record nor read the store; the store count lags by the old pods' spawns, bounded by the rollout window; old pods keep their own in-memory cap | Automatic once the rollout completes | none |
| Rollback to a pre-fix release | Streak refs remain in git-rest; the old binary never reads or clears them; dispatch is unaffected (old Recover only touches its own cluster name, which never equals `redrive-streak`) | Benign; operator deletes `claims/redrive/` refs via git-rest if desired | refs visible in git-rest listing |
| Executor started with `CLUSTER=redrive-streak` | Recover still skips streak refs by key prefix | None | none |
| Clock skew between clusters | No effect: `lease_until` is informational and never compared | None | none |

## Security / Abuse Cases

- Inputs to the key are the task id and the phase from vault frontmatter. Both pass the existing claim-component validation (no `/`, `..`, leading `.`, `.lock` suffix, characters outside `[A-Za-z0-9._-]`) before any request; an invalid value is never sent to git-rest (DB7, AC5).
- Anyone with git-rest write access can delete streak refs and un-cap a task, or add refs and cap one. This is the same trust boundary as the dispatch claims; the in-memory floor still caps within one pod.
- No retry loop is unbounded: create retries are capped at 3 attempts, and every request inherits the 30 s cap.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Streak store in `pkg/claim` over the existing ref client: count at phase, record with 409 retry and other-phase delete, clear, foreign-key ignore, key-component validation | 1, 2, 7 (store side) | AC1, AC2, AC3, AC4, AC5 | — |
| 2 | Lease-manager `Recover` skip of `claims/redrive/` keys and the claim-lease protocol doc section (streak refs + operator clear) | 6 | AC6 | — (must land before prompt 3) |
| 3 | Handler integration: one shared ref client from the existing one-time construction, store only in claim-taking modes, max(store, memory) cap check, record/clear wiring, read/write fail-safe, explicit un-cap rule, `maxConsecutiveRedrives` comment rewrite, CHANGELOG `## Unreleased` bullet | 3, 4, 5, 7 (handler side), 8 | AC7, AC8, AC9, AC10, AC11, AC12 | prompts 1, 2 |

Rationale: the store is self-contained and testable against the existing in-process fake git-rest; the Recover skip must exist before any executor writes streak refs into the namespace Recover scans; the handler change only consumes both. AC13 is the operator's post-release replay and belongs to no prompt.

## Do-Nothing Option

Tasks stuck at one phase keep spawning a Job every few minutes, alternating clusters, until an operator notices and parks them by hand — each attempt costs a Job and its model budget. The cap stays effective only on a single-cluster deployment with no restarts, which no longer describes production.
