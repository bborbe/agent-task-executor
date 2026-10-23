---
status: prompted
approved: "2026-10-23T12:13:49Z"
generating: "2026-10-23T12:15:20Z"
prompted: "2026-10-23T12:23:27Z"
branch: dark-factory/bug-held-claim-outlives-vanished-job
---

## Summary

- A held claim is released only when its Job's terminal event reaches the executor. A Job that disappears any other way leaves the claim held and renewed forever: deleted by its TTL, deleted by hand, or finished while the watch event was lost.
- For the four repo-keyed task types (update-go, release, pr-review, build-fix), one orphaned claim blocks every task of that repository on every cluster.
- Fix: the lease loop releases a held claim whose Job has not been running for 5 minutes.
- The existing release paths keep their own handling: success hold, pending release, pending Job deletion and the adoption check.
- After the release, the task becomes eligible again, and the normal dispatch respawns it.

## Problem

No code path frees a claim whose Job vanished without a terminal event. A unit-level reproduction on v0.30.7 (see Reproduction):

1. Hold a claim.
2. Report its Job as not running, and send no `Release`.
3. Tick for 15 minutes.

The expected outcome is that the claim is renewed every 60 s and never deleted. A second task's `Claim` and `Takeover` on the key then both return `ErrLeaseConflict`. Two reasons:

- `AuditMismatch` only counts mismatches, never releases, and has no production caller.
- The renew loop keeps the lease alive, so the expired-lease takeover never fires.

Context: on nuke prod (openclaw), 2026-10-23, pr-review task `ca0dc7eb` (bborbe/nuke#658) logged `event=claim_failed stage=takeover … error=lease conflict` for about 50 minutes and never ran. That is the bare `ErrLeaseConflict` sentinel from this executor's own held-claims map, so this executor held `claims/repo/bborbe-nuke`. At 11:39:21Z the holder was task `4684419b` (bborbe/nuke#662).

**Unverified:** whether 4684419b's Job was alive for the whole 50 minutes, because the kubectl log window starts only at 11:37Z. If it was alive, the wait was plain repo serialisation (sibling spec `bug-pr-review-claims-serialize-per-repo`). If it was not, this bug held the key.

## Goal

No held claim outlives its Job by more than about 6 minutes: 300 s grace, plus up to 60 s check spacing, plus one 10 s tick. Each of these exceptions stays on its existing path:

- a claim on the success hold, released at hold expiry;
- a claim with a pending release, retried until the fence;
- a claim with a pending Job deletion, handled by the fence path;
- an adopted claim whose running check has not answered yet.

## Non-goals

- Do NOT change lease timing: lease length, tick, renew interval, fence margin, fence lead, takeover grace and request cap all stay.
- Do NOT change the expired-lease takeover path.
- Do NOT change the concurrency cap.
- Do NOT wire `AuditMismatch` into production, and do not change it.
- Do NOT change claim keys. Per-PR keying is the sibling spec.
- Do NOT change dispatch or respawn logic.
- Do NOT make the jobless grace or check interval configurable. They are invariants like the constants in `docs/claim-lease-protocol.md`; a future need for variation is a separate spec.

## Reproduction

Repo `bborbe/agent-task-executor` at `82d7453` (`release v0.30.7`). dark-factory `v0.198.7`.

**Deterministic (unit level, v0.30.7):**

1. Build a lease manager over the fake git-rest, with a fake clock and a fake JobControl.
2. `Claim("claims/repo/bborbe-nuke", "4684419b")` succeeds.
3. Set `IsJobRunning("4684419b")` to `false`, and send no `Release` call. This simulates a terminal event that never arrived.
4. Tick every 10 s for 15 simulated minutes.
5. Expected on v0.30.7 (to be confirmed by AC 2): no `DELETE` is sent, the claim is renewed every 60 s, and `HeldClaims()` still lists it.
6. `Claim` and `Takeover` on the key for task `ca0dc7eb` both return `ErrLeaseConflict`.

**Runtime context (nuke prod openclaw, 2026-10-23):** the original log line is no longer retrievable, because the kubectl log window starts at 11:37Z. Reconstructed from the format string at `pkg/handler/claim_wiring.go:207` (`event=claim_failed stage=%s mode=%s task=%s error=%v`) and the reported fields:

```
event=claim_failed stage=takeover mode=on task=ca0dc7eb<full id not captured> error=lease conflict      # repeated ~50 min
```

**Dev runtime equivalent:** delete a running agent Job by hand. The Job informer registers no `DeleteFunc`, so no terminal event arrives, and v0.30.7 keeps the claim.

## Expected vs Actual

| | Expected | Actual (v0.30.7) |
|---|---|---|
| Job vanishes with no terminal event | Claim released within ~370 s of the first not-running check; the task is then respawned by normal dispatch | Claim renewed forever, and every respawn attempt fails with `lease conflict` |
| Job keeps running | Claim renewed, never released | Same (unchanged) |

## Why this is a bug

`docs/claim-lease-protocol.md:41` defines `released` as "the Job ended and the claim was deleted with the held sha", so a claim lives as long as its Job. A claim that the renew loop keeps alive after the Job is gone breaks that invariant. Takeover cannot reclaim it, because takeover requires an expired lease.

## Acceptance Criteria

1. [ ] **Released after the grace.** A Ginkgo spec holds a claim, sets `IsJobRunning` to `false` with no `Release` call, and ticks a fake clock every 10 s. It asserts:
   - `executor_claim_total{result="jobless_released"}` reads 0 before the run;
   - no `DELETE` reaches the fake git-rest while less than 300 s have passed since the first not-running check;
   - exactly one `DELETE` carrying the held sha arrives within 370 s of that check;
   - `HeldClaims()` no longer lists the key;
   - `jobless_released` and `released` each rose by exactly 1;
   - a following `Claim` on the same key by another task id succeeds (`claimed`).

   Evidence: `make test` exits 0 with the spec passing.
2. [ ] **Regression lock.** File `pkg/claim/lease_manager_jobless_test.go` must meet all of these:
   - every spec in it uses only test helpers present at v0.30.7, from `lease_manager_test_helpers_test.go` and `fake_git_rest_test.go` (such as `newTestManager`, `newFakeJobControl`, `SetRunning`, `tickAt`, `advance`, `snapshotClaims`, `expectClaimDelta`, `requestsMatching`);
   - every spec in it writes durations as literal values;
   - its top-level `Describe` text contains `jobless`.

   If any jobless spec needs a helper or identifier that is new in this change, it goes in another file, and this file holds only the AC 1 spec. Run against v0.30.7, the file fails on an assertion (no `DELETE` received, or key still held), not on compilation. Evidence: the operator command under Verification exits non-zero, and its output names the failed assertion.
3. [ ] **Never released while the Job runs.** With `IsJobRunning` `true` over 30 simulated minutes:
   - zero `DELETE`s;
   - at least 25 renew `PUT`s;
   - `IsJobRunning` called for that claim at most 31 times;
   - `jobless_released` unchanged.

   Evidence: `make test` exits 0.
4. [ ] **Grace starts at Job stop and resets on run.** Two specs. Evidence: `make test` exits 0 with both passing.
   - (a) The Job runs 10 min, then stops: no `DELETE` before stop + 300 s, and one `DELETE` before stop + 370 s.
   - (b) Not running for 200 s, running for one check, then stopped again: no `DELETE` before second-stop + 300 s.
5. [ ] **Other paths untouched.** With `IsJobRunning` `false` for a 10-minute window, `jobless_released` stays unchanged for each of four states. Evidence: `make test` exits 0.
   - A claim on the success hold: no `DELETE` before `holdUntil`, and the hold expiry still releases it with `released`.
   - A claim with a pending release.
   - A claim with a pending Job deletion.
   - A claim adopted with `needsRunningCheck`.
6. [ ] **Lookup errors keep the claim.** `IsJobRunning` returns an error for 10 simulated minutes. No `DELETE` is sent, the claim stays held, and `executor_claim_total{result="error"}` rises. Evidence: `make test` exits 0.
7. [ ] **DELETE outcomes and log line.** Evidence: `make test` exits 0 with both specs passing, and `grep -c 'event=claim_jobless_released key=%s task=%s jobless_since=%s' pkg/claim/*.go` totals 1.
   - (a) The jobless `DELETE` returns 500. The claim stays held with a pending release, and `error` rises by 1. A later tick's retry `DELETE` succeeds. Over the whole run, `jobless_released` and `released` each rise by exactly 1, and the fake git-rest receives exactly 2 `DELETE`s.
   - (b) The jobless `DELETE` returns 409. The claim is dropped, and `already_released` and `jobless_released` each rise by exactly 1.
8. [ ] **Migration: adopted repo-keyed claim freed.** Startup recovery lists `claims/repo/bborbe-nuke`, owned by this cluster, with its Job running, and adopts it. The Job then disappears with no terminal event. One `DELETE` for that key follows within 370 s of the first not-running check. Evidence: `make test` exits 0.
9. [ ] **Protocol doc.** `docs/claim-lease-protocol.md` has:
   - a constants row for the 300 s jobless grace;
   - a constants row for the 60 s check interval;
   - a state-machine entry for the jobless release, which notes that the task becomes eligible for respawn;
   - a series count of 14.

   Evidence: `grep -ciE 'jobless' docs/claim-lease-protocol.md` ≥ 3, and `grep -c '14 result series' docs/claim-lease-protocol.md` = 1.
10. [ ] **CHANGELOG.** Every line printed by `awk '/^## /{sec=$0} /jobless/{print "sits under: " sec}' CHANGELOG.md` is `sits under: ## Unreleased`, and at least one line is printed. Evidence: stdout match.
11. [ ] `make precommit` exits 0 at the repo root. Evidence: exit code.
12. [ ] **Post-Deploy (Rung-2):** on nuke dev, the operator deletes one running agent Job by hand. Within 370 s the executor releases its claim. Evidence: the log line quoted with its timestamp next to the delete time, plus both metric readings.
    - Operator steps:
      1. `kubectlnukedev -n dev get jobs` lists an active agent Job and its task id. Record the executor pod (`kubectlnukedev -n dev get pods | grep agent-task-executor-openclaw`) and its metrics port (`kubectlnukedev -n dev get pod <pod> -o jsonpath='{.spec.containers[0].ports[0].containerPort}'`).
      2. Before the delete, read `kubectlnukedev -n dev get --raw "/api/v1/namespaces/dev/pods/<pod>:<port>/proxy/metrics" | grep -E 'executor_claim_total\{result="(released|already_released|jobless_released)"\}'`.
      3. The operator runs `kubectlnukedev -n dev delete job <name>`. This is an operator-only destructive step; the agent never runs it.
      4. `kubectlnukedev -n dev logs deploy/agent-task-executor-openclaw --since=15m | grep 'event=claim_jobless_released'` returns a line with `task=<that id>`.
      5. Re-read the metrics from step 2. `jobless_released` has risen by 1, and `released` or `already_released` has risen by 1.
      6. Expect a new Job for the same task id to appear afterwards (`kubectlnukedev -n dev get jobs`). That is the normal respawn (Failure Modes row "Job deleted by an operator"): existing dispatch respawns a non-terminal task once its claim is free. It is not a failure.
    - `deploy_check:` `kubectlnukedev -n dev get deploy/agent-task-executor-openclaw -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
    - `deploy_target:` `v0.30.8`. This is the predicted next patch tag. The operator updates it to the tag actually deployed, if that differs.

No new scenario: the fake clock, fake git-rest and fake JobControl reach every behaviour, and AC 12 is the runtime proof.

## Verification

### Container-executable

```
make test
make precommit
grep -c 'event=claim_jobless_released key=%s task=%s jobless_since=%s' pkg/claim/*.go
grep -n '"jobless_released"' pkg/metrics/metrics.go
grep -ciE 'jobless' docs/claim-lease-protocol.md
awk '/^## /{sec=$0} /jobless/{print "sits under: " sec}' CHANGELOG.md
```

### Operator-executable (host, after merge)

```
# AC 2: regression lock. Expect a non-zero exit on an assertion.
cd ~/Documents/workspaces/agent-task-executor
git worktree add /tmp/ate-v0307 v0.30.7
cp ~/Documents/workspaces/agent-task-executor/pkg/claim/lease_manager_jobless_test.go /tmp/ate-v0307/pkg/claim/
cd /tmp/ate-v0307 && go test ./pkg/claim/ -run TestClaim -ginkgo.focus=jobless

# AC 12: Rung-2 on nuke dev (the delete is operator-only). Full steps in AC 12.
kubectlnukedev -n dev get deploy/agent-task-executor-openclaw -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'
kubectlnukedev -n dev logs deploy/agent-task-executor-openclaw --since=15m | grep 'event=claim_jobless_released'
```

## Desired Behavior

1. **Jobless release.** On each tick, the lease loop asks JobControl whether the Job of each held claim is running. It skips claims that are on another path: success hold set, pending release, pending Job deletion, or `needsRunningCheck`. A claim whose Job has been continuously not running for at least 300 s (`JoblessGrace`) is released with a compare-and-swap `DELETE` carrying the held sha.
2. **Check cadence and grace clock.** JobControl is asked at most once per 60 s per claim. The grace clock:
   - starts at the first check that sees the Job not running;
   - clears when a check sees the Job running;
   - is left unchanged by a check that errors, which keeps the claim and counts `executor_claim_total{result="error"}`.
3. **Release outcome and observability.** Each jobless release decision, once per claim:
   - logs one warning line `event=claim_jobless_released key=<key> task=<id> jobless_since=<RFC3339>`;
   - counts `executor_claim_total{result="jobless_released"}`;
   - sends the `DELETE`.

   The outcome follows existing release semantics:
   - success → dropped, `released`;
   - 409 → dropped, `already_released`;
   - any other error → kept with a pending release, which the existing retry path handles without counting `jobless_released` again.

   The `jobless_released` series is pre-initialised to 0.
4. **Adopted claims covered.** A claim adopted by startup recovery falls under behaviour 1 once its running check has answered. This includes `claims/repo/*` refs written before the sibling keying spec, so no manual ref cleanup is needed.

## Constraints

- All lease constants in `docs/claim-lease-protocol.md` are unchanged. `JoblessGrace` (300 s) and the 60 s check interval are new invariants: no flag, env var or CRD field.
- `ClaimTotal` labels stay a closed set. Keys and task ids are never label values.
- The success hold (v0.30.5 and v0.30.7) is unchanged.
- Fence, takeover, lost-response recovery and restart recovery behave as documented. The fence runs before the jobless check in a tick.
- `AuditMismatch` and `executor_claim_mismatch_total` are unchanged.
- Existing tests pass unmodified, with one exception: the metrics spec `pre-initializes all 13 claim result series` (`pkg/metrics/metrics_test.go:33`) becomes 14.

## Assumptions

- `IsJobRunning` reports Pending or Running Jobs as running, and a missing or terminal Job as not running, per its doc comment.
- nuke dev runs every executor with `CLAIM_MODE=on` (confirmed by coordinator), so a deleted dev Job holds a real claim.
- The next release from `fix/claim-reaper` is a patch bump to `v0.30.8`, because `## Unreleased` carries only `fix:` bullets.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection |
|---|---|---|---|
| Kubernetes API unavailable (`IsJobRunning` errors) | Claim kept, grace clock unchanged, `error` counted | Next check after 60 s; evidence: `claim_jobless_released` or a renew once the API answers | `executor_claim_total{result="error"}` rises |
| git-rest unavailable on the jobless `DELETE` | Claim kept with pending release, retried by the existing path until the fence | Fence's best-effort `DELETE`, then lease expiry + takeover grace; evidence: `released` or `fenced` rises | `claim_jobless_released` without a following `released` |
| Peer already removed or replaced the ref (409) | Dropped, `already_released` | none needed | `already_released` rises |
| Spawn window: claim taken, Job not yet created | Job appears within 300 s, is seen running, and the grace clock clears | n/a | none |
| Slow spawn past 300 s | Bound: on the reconcile path, SpawnJob runs under the 45 s pass deadline (`RECONCILE_PASS_TIMEOUT`, `main.go:84`). No explicit deadline on the event-path SpawnJob was found in this repo, so a create taking longer than 300 s there is **accepted**: the claim is released and the Job runs unclaimed until it ends. | none; the accepted residual risk | `claim_jobless_released` for a task whose Job then appears without a new claim |
| Succeeded Job whose terminal event was lost | No hold is set, so the claim is released after 300 s, the same window as the success hold. A respawn of the already-succeeded task is then possible until the vault reflects the result. | Existing anti-duplicate checks on the task phase | `claim_jobless_released`, then a new Job for a task whose phase becomes terminal |
| Job deleted by an operator (AC 12) | Claim released after the grace. The jobless release does not touch the task; existing dispatch (event path or reconcile pass) respawns the non-terminal task with a fresh claim, which now succeeds instead of failing with `lease conflict` | none needed | `claim_jobless_released`, then a new Job for the task id |
| Executor crash between the decision and the `DELETE` | Startup recovery lists the ref and releases it, because the Job is finished | automatic; evidence: `released` rises at startup | startup recovery log |
| Clock skew between clusters | No effect: the grace is measured on the holder's own clock | n/a | n/a |
| Many held claims | At most one JobControl call per claim per 60 s | n/a | n/a |

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Jobless release in the lease loop, the `jobless_released` metric, the protocol doc rows and state entry, and the CHANGELOG bullet | 1–4 | 1–11 | — |

Rationale: one cohesive change in `pkg/claim` plus a one-line metric addition; splitting it would separate the behaviour from its own tests. AC 12 is run by the operator after release.

## Do-Nothing Option

Every orphaned claim on a repo-keyed type blocks that repository's pr-review, update-go, release and build-fix tasks. The block lasts until an operator deletes the ref or restarts the executor, and a restart releases it only if the Job is already gone. A lost watch event is enough to trigger it. Not acceptable.
