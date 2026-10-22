---
status: prompted
approved: "2026-10-21T23:27:43Z"
generating: "2026-10-21T23:38:12Z"
prompted: "2026-10-21T23:48:08Z"
branch: dark-factory/claim-lease-lifecycle-wiring
---

## Summary

- Completes the claim wiring started by spec 011 (claim-mode-dispatch-wiring): a claim taken before a Job is created is released when that Job ends, when the task reaches a terminal status or the `human_review`/`done` phase, and when the Job creation that motivated it fails.
- Adds the two lifecycle pieces that make a live mode survivable: a restarted executor recovers the claims it held before any loop starts, and the lease manager's `Run` loop is one more entry in the service run list so held claims are renewed and fenced.
- Runs the held-claim / running-Job audit once per reconcile pass in `shadow` and `on`, and never in `off`.
- Adds no metric: every outcome lands on the existing closed `executor_claim_total` set and `executor_claim_mismatch_total`.

## Problem

The dispatch spec makes a cluster claim a key before it creates a Job. Without this spec that claim is never released: the key stays held until its lease expires and the fence deletes the Job of a task that already finished, and a re-driven task of the same repo-bound type finds its own executor's claim in the way. A restart is worse — the executor comes back holding nothing, so every lease it had granted itself lapses while its Jobs keep running, which is precisely the state the protocol exists to prevent.

The end points are not one place. A Job ends in the Job watcher, a task ends in the vault, a phase gate ends it a third way, and a failed spawn ends it before a Job ever exists. Hooking three of the four leaves a claim that outlives its work and is indistinguishable, on the counters, from a healthy one.

## Goal

Every claim the executor takes is released within one lease tick of the work it protects ending, whichever of the five end points ended it, and a failed release is retried by the lease loop rather than lost. An executor that restarts adopts the claims whose Jobs still run, releases the rest, and resumes renewing before it spawns anything new. In `shadow` and `on` each reconcile pass counts held claims with no running Job and running Jobs with no held claim.

## Non-goals

- No `CLAIM_MODE` parsing, no claim gate at dispatch, no takeover — spec 011 (claim-mode-dispatch-wiring) owns those and this spec assumes them.
- No `spec.rerunOnLostClaim` consequence wiring (park vs re-dispatch). The park path is a separate spec.
- No rollout, no nuke dev, prod or sun step, no `shadow` observation window — those are the anchored task's subtasks and need operator approval.
- No change to `pkg/claim`; its `Release`, `Recover`, `Run` and `AuditMismatch` are called as they are, and spec 009's tests pin their behaviour.
- No new metric, no new label value, no new alert.
- No release on a timer independent of the five end points: a claim whose Job is still running must stay held.

## Assumptions

- Spec 011 lands first: a claim exists before this spec's release hooks have anything to release, and the mode value that turns claims on lives there.
- `HeldClaims()` is the only claim index the wiring keeps. There is no second bookkeeping structure, so a claim the manager no longer holds is invisible to every hook — which is what makes a double release a no-op rather than an error.
- The five end points are exhaustive: every way a claim's work can end is one of them. If a sixth path is discovered, it is a bug in this spec, not a gap to paper over with a timer.

## Acceptance Criteria

All container-executable. Tests use the counterfeiter fake at `mocks/claim_ref_client.go` (`DeleteCallCount`, `DeleteArgsForCall`, `ListCallCount`) and a fake `JobControl`. Each AC names how a broken implementation is caught ("Discriminates").

- [ ] AC1 — **release at every end, and only for the right claim.** Two claims are held, one per task. Each of the five end points, exercised in turn, releases **only** the claim whose task ended: one `Delete` with that claim's sha, `released` +1 for it, and no `executor_claim_total` series increments beyond the `released` +1 attributable to the ended task (the survivor stays held and `DeleteCallCount` does not grow for its sha). The five points are (a) the Job watcher's `HandleJob` succeeding branch, (b) its failing branch, (c) the task's frontmatter status becoming `completed` or `aborted`, (d) the task's phase becoming `human_review` or `done`, and (e) `SpawnJob` returning an error after a successful claim. Evidence: five Go tests, each asserting the `Delete` count and sha, the per-series delta, and the surviving claim's presence in `HeldClaims()`. Discriminates: a wiring that hooks only the terminal-status path fails (a), (b), (d) and (e); one that releases every held claim on any end event fails the survivor assertion in all five.
- [ ] AC2 — **the wiring adds no retry of its own.** A `Delete` that fails with a non-`409` error at an end point leaves the claim held and the wiring returns without looping; the next lease tick retries it — the retry that succeeds sends exactly one further `Delete` and counts `released` +1. Releasing a key the manager does not hold sends no `Delete` and changes no counter. Evidence: two Go tests driving the end point then `Tick` on the fake clock, asserting exactly two `Delete` calls total in the failure case and zero in the unheld case. Discriminates: a wiring that wraps `Release` in its own retry loop sends more than two `Delete` calls in the failure case; one that drops the claim from its own bookkeeping on failure leaves nothing for the tick to retry, so `released` stays +0.
- [ ] AC3 — **restart recovery and the lease loop.** With two listed claims — one whose Job is still running, one whose Job is gone — `Recover` adopts the first (`adopted` +1, no `Delete`) and releases the second (`released` +1, one `Delete` with the listed sha). The lifecycle run funcs are returned by a factory constructor: an empty slice in `off`, one lease-loop entry in `shadow` and in `on`. Evidence: a Go test asserting the counter deltas and `Delete` calls, plus a `pkg/factory` test asserting `len(runFuncs) == 0` in `off` and `1` in `shadow`/`on`, plus a **plain `testing`** test in `main_internal_test.go` calling `a.runFuncs()` and asserting the slice it returns is what `main.go` passes into `service.Run`. Plain `testing` is required in both: measured 2026-10-22, Ginkgo specs declared in an internal test package of this repo never run — `go test .` reports 0 of the specs in `main_internal_test.go` — and `pkg/factory` has no test suite at all, so its first test file must not assume one exists. Discriminates: a wiring that starts the loop in `off` fails the `len == 0` assertion; one that re-creates instead of adopting fails the `adopted` delta and records a `Put` with an empty `expected_sha`.
- [ ] AC4 — **mismatch audit per pass, never in `off`.** With one running Job and no held claim, and one held claim and no running Job: one reconcile pass adds exactly 2 to `executor_claim_mismatch_total` and logs two `claim_mismatch case=` lines naming the task ids. A pass with matching sets adds 0. In `off` a pass leaves the counter unchanged. Evidence: three Go tests asserting the counter delta and the log lines, and a negative-evidence assertion that `git diff pkg/metrics/metrics.go` shows no new `NewCounter`/`NewCounterVec`/`NewGauge`/`NewHistogram` registration. Discriminates: a wiring that audits in `off` fails the third row; one that audits only one direction fails the delta of 2; one that adds a dedicated mismatch counter fails the diff assertion.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — lint, vet and the full test suite exit 0
- `go test -race -count=1 ./...` — exits 0
- `grep -rn 'Recover\|AuditMismatch' main.go pkg/ | grep -v _test.go` — exactly one `Recover` call site in `main.go`, before the `service.Run` list, and exactly one `AuditMismatch` call site, inside `ReconcileOnce`
- `grep -c 'promauto.New' pkg/metrics/metrics.go` — same count as before the change (the container has no git, so a `git diff` check is not container-executable; the diff form belongs to the host rung)

## Desired Behavior

1. A claim taken for a Job is released when that Job ends, whether it succeeded or failed, so a re-driven task of the same key can claim afresh.
2. A claim is released when the task it protects reaches `completed`/`aborted` in the vault, or `human_review`/`done` in its phase — the paths a task takes when no Job end is observed.
3. A claim whose Job was never created is released as soon as the spawn that motivated it fails; if that release fails, the lease loop retries it rather than the claim being abandoned.
4. Before any loop starts in `shadow` or `on`, the executor recovers the claims it held: it adopts those whose Job still runs and releases those whose Job is gone.
5. The lease loop runs as one more service run func in `shadow` and `on`, and is absent in `off`.
6. In `shadow` and `on` each reconcile pass audits held claims against running Jobs and counts both directions; in `off` no audit runs.
7. Every release names the claim it releases: an end event for one task never releases another task's claim.

## Constraints

- `pkg/claim` is called, never modified. `Release` (`pkg/claim/lease_manager.go:294`) already sets `releasePending` on a failed delete and `retryPendingReleases` (`pkg/claim/lease_manager_tick.go:112`) retries it on the next tick; the wiring must not duplicate that retry.
- `Release` for a key the manager does not hold returns nil without a counter change — a double release is a no-op, not an error, and must not be logged as one.
- The Job-watcher hooks live in `HandleJob` (`pkg/job_watcher.go:141`) at its two terminal branches, **not** inside `handleSucceeded` (`:200`) or `handleTerminal` (`:296`): both of those are reached only with a task-store entry and `handleTerminal` has a `logMissingTask` branch (`:308`) that carries no `lib.Task`. The claim is resolved by task id from `HeldClaims()`, so the hook needs no vault read and no Config; a Job whose task id matches no held claim is a no-op. The succeeded hook must sit **outside** `HandleJob`'s `if task, ok := w.taskStore.Load(taskID); ok` guard — placed inside it, every Job whose store entry was already evicted silently skips the release, and that mis-placement reads as correct in review.
- The other three hooks are `cleanupTerminalTask` (`pkg/handler/task_event_handler.go:328`), the phase gate (`applyPhaseGate` declared `:145`, used `:441`), and the claim's own failure branch in `spawnIfNeededClaimed` (declared `:693`; the `SpawnJob` error branch is `:779`).
- `Recover` is called once, before the service run list starts, and never from a loop.
- The lifecycle run funcs are produced by a `pkg/factory` constructor returning `[]run.Func`, and `main.go` appends them to its `service.Run` list — the list is extracted into an `a.runFuncs()` method so `main_internal_test.go` can assert what it contains.
- The audit runs inside `ReconcileOnce` (`:1076`), once per pass, not once per candidate.
- In `off` no lease manager is constructed, so none of these calls exist on that path.
- `docs/claim-lease-protocol.md` is the protocol reference these hooks implement; a hook that seems to contradict it is a bug in the hook.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection | Reversibility |
|---|---|---|---|---|
| Job ends but its claim release fails | the claim stays held, `error` +1, and the next tick retries | `retryPendingReleases` on the following tick; `released` +1 when it succeeds | `error` rising with `released` following one tick later | reversible — the ref is deleted on the retry |
| git-rest throttles the release `DELETE` | as above; the claim outlives its Job until the throttle clears, and the fence eventually deletes a Job that is already gone | the tick retries every 10 s; a sustained throttle is visible as `error` without `released` | `error` rising at a steady rate with `released` flat | reversible — no state is lost, only delayed |
| Executor restarts with Jobs still running | `Recover` adopts them and the lease loop resumes renewing before any spawn | automatic | `adopted` at startup with no `fenced` for those tasks | reversible |
| Executor restarts with Jobs gone | `Recover` releases them | automatic | `released` at startup | reversible |
| Task ends while its Job is still terminating | the claim is released on the task path; the Job's own end finds nothing to release and is a no-op | none needed | no `error` on the second release | reversible |
| Two end points fire for one claim | the first releases; the second finds nothing held and is a no-op | none needed | `released` +1, not +2 | reversible |
| Reconcile pass overlaps a Job ending | the audit may count a transient mismatch in either direction | the next pass counts it back; the counter is a rate signal, not a state | `executor_claim_mismatch_total` rising by 1 with no `claim_lost` | reversible |
| Two end points release one claim concurrently | the lease manager's mutex serialises them: one counts `released`, the other finds nothing held and returns nil | none needed | `released` +1 with no `error` | reversible |
| `Recover` cannot list claims at startup | the executor starts with no held claims; the leases it had lapse and peers may take over | tasks are re-driven by whichever cluster holds them | a startup `error` with no `adopted`/`released` | reversible — the task is re-run, not lost |

## Security / Abuse

No new network destination and no new credential: every call here goes to the same git-rest endpoint with the same gateway secret as the dispatch spec. The audit logs task ids, which are already logged throughout dispatch. Nothing in this spec can cause a task to run — it can only end a claim, and ending a claim early is safe by construction: the peer that takes over re-checks the vault before spawning.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Release on the Job-end branches of `HandleJob` and on the failed-spawn path, resolving the claim by task id from `HeldClaims()` | 1, 3, 7 | 1 (a, b, e), 2 | dispatch spec prompts 1-2 |
| 2 | Release on the vault-terminal and phase-gate paths | 2, 7 | 1 (c, d) | prompt 1 |
| 3 | Startup `Recover`, the factory's lifecycle run funcs wired into `a.runFuncs()`, and the per-pass audit | 4, 5, 6 | 3, 4 | prompts 1-2 |

Rationale: prompt 1 establishes the release handle and its idempotency, which every later hook reuses. Prompt 2 is two more call sites of the same handle and can land independently. Prompt 3 is lifecycle glue that needs claims to exist and be releasable before it is meaningful.

## Do-Nothing Option

A claim taken before a Job is created is never released. The key stays held until its lease expires, the fence then deletes the Job of a task that may already have finished, and every re-drive of a repo-bound task collides with its own executor's stale claim. A restart silently drops every lease while the Jobs keep running — the exact failure the protocol was built to prevent, and one that looks like normal operation on the counters.
