---
status: completed
spec: [011-bug-held-claim-outlives-vanished-job]
summary: Added a jobless-claim release to the lease loop with a new jobless_released claim result series, protocol docs, changelog bullet and Ginkgo specs covering every branch.
execution_id: agent-task-executor-claim-reaper-exec-022-spec-011-release-claim-when-job-vanished
dark-factory-version: v0.198.7
created: "2026-10-23T12:17:33Z"
queued: "2026-10-23T12:36:10Z"
started: "2026-10-23T12:36:12Z"
completed: "2026-10-23T12:44:17Z"
branch: dark-factory/bug-held-claim-outlives-vanished-job
---

<summary>
- A held claim is now released when its Job has been gone for five minutes, instead of being renewed forever.
- The lease loop asks, at most once a minute per claim, whether the claim's Job is still running.
- A Job that is missing, finished, or deleted by hand therefore frees its claim within roughly six minutes.
- Every other release path keeps its own handling: the success hold, a pending release, a pending Job deletion, and an adopted claim whose running check has not answered.
- A temporary Kubernetes lookup failure keeps the claim and is counted as an error; the next check tries again.
- The release logs one warning line and increments a new claim result counter, so operators can see it happen.
- A failed release falls back to the existing retry path; a "someone else already removed it" answer is counted separately.
- Claims adopted at startup are covered, including the old repository-keyed refs, so no manual ref cleanup is needed.
- The protocol document gains the two new invariants and the new state; the claim result series count goes from 13 to 14.
- The changelog gets one fix bullet under a new Unreleased section.
</summary>

<objective>
Add a jobless-claim release to the lease loop in `pkg/claim`: on each tick, for every held claim not owned by another release path, ask `JobControl.IsJobRunning` at most once per 60 s, and when the Job has been continuously not running for at least 300 s, send one compare-and-swap `DELETE` carrying the held sha and drop the claim. Ship it together with the `jobless_released` claim result series, the protocol-document rows and state entry, the changelog bullet, and the Ginkgo specs for every acceptance criterion. No lease timing, takeover, dispatch or wiring changes are part of this prompt.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for the container agent's execution rules (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, external `claim_test` package, coverage ≥80% for new code, license header on every Go file). This repo has no project-level `CLAUDE.md`.

Read these files fully before changing anything:
- `/workspace/pkg/claim/lease_manager.go` — the `LeaseDuration` / `TickInterval` / `RenewInterval` / `FenceMargin` / `FenceLead` / `TakeoverGrace` constant block, the `heldClaim` struct (fields `pendingJobDeletion`, `releasePending`, `needsRunningCheck`, `holdUntil`), `countClaim`, `requestContext`, `releaseLocked`, and `ReleaseAfterSuccess`.
- `/workspace/pkg/claim/lease_manager_tick.go` — `Tick` and its ordered phases (`retryPendingDeletions`, `evaluateFence`, `retryPendingReleases`, `releaseExpiredHolds`, `recheckRunning`, `renewDueClaims`), plus `releaseAfterRunningCheck` and `retryPendingReleases` as the templates for a release outcome switch.
- `/workspace/pkg/claim/job_control.go` — the `JobControl` interface: `IsJobRunning(ctx context.Context, taskID string) (bool, error)` and `DeleteJob(ctx context.Context, taskID string) error`.
- `/workspace/pkg/claim/lease_manager_test_helpers_test.go` — `newTestClock`, `advance`, `tickAt`, `snapshotClaims`, `expectClaimDelta`, `countSuccessfulPuts`, `newTestManager`, `fakeJobControl` (`runningFn`, `runningCalls`, `deleteFn`, `SetRunning`, `DeleteCallCount`, `Deleted`), `requestsMatching`.
- `/workspace/pkg/claim/fake_git_rest_test.go` — `newFakeGitRest`, `Seed(ref, sha, cluster, taskID, leaseUntil)`, `SetFault(func(fakeRequest) *fakeFault)`, `ResetRequests`, `Requests`, `fakeFault{Status, Body, Apply}`, and the fact that a `DELETE` whose `expected_sha` does not match answers 409.
- `/workspace/pkg/claim/lease_manager_restart_test.go` — the `fake.Seed("refs/claims/task/a", ...)` + `job.runningFn = func(taskID string, call int) (bool, error)` + `mgr.Recover(ctx)` pattern; mirror it for the adopted-claim spec.
- `/workspace/pkg/claim/lease_manager_release_test.go` — the `faultMethod(method string, status int)` helper and the release-outcome test style.
- `/workspace/pkg/metrics/metrics.go` — `ClaimTotal` and the `init()` block that pre-initialises the closed claim result set (13 entries today).
- `/workspace/pkg/metrics/metrics_test.go` — the spec `It("pre-initializes all 13 claim result series", ...)` at line 33.
- `/workspace/docs/claim-lease-protocol.md` — the `## Constants` table, the `## Claim state machine` list, and the line `all 13 result series are pre-initialised to 0`.
- `/workspace/CHANGELOG.md` — top of file; the newest section is either `## v0.30.7` or a `## Unreleased` section already created by the spec 012 prompt (run order is not fixed).

Relevant coding docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2, external test package, coverage ≥80% for new code.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `github.com/bborbe/errors`, never `fmt.Errorf`.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-precommit.md` — golines 100-char limit, funlen 80, nestif 4, license headers.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-doc-best-practices.md` — GoDoc comments start with the name and describe behaviour.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` placement and the required `fix:` prefix.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new code.

Verified facts (do not re-derive):
- `libtime.Second` is a `libtime.Duration` constant; the const block already writes `300 * libtime.Second`.
- `libtime.DateTime` conversions from `time.Time` are `libtime.DateTime(t)`, as used in `recoverLeaseUntil`; a zero value is `libtime.DateTime(stdtime.Time{})`.
- `libtime.DateTime` has `.Time()`, `.Add`, `.Sub`, and comparisons used in the files above.
- `ClaimRefClient.Delete(ctx, key, expectedSha)` sends `?expected_sha=<sha>` and maps 409 to `ErrLeaseConflict` (`pkg/claim/ref_client.go`).
- `NewLeaseManager(cluster, refClient, jobControl, currentDateTime, waiter)` and `claim.NewClaimRefClient(fake.URL(), "", "")` are the constructors the tests use.
</context>

<requirements>
1. Add two constants to the existing `const` block in `/workspace/pkg/claim/lease_manager.go`, after `TakeoverGrace`, with these exact names, values and doc comments:
   ```go
   // JoblessGrace is how long a held claim's Job must be continuously absent
   // before the lease loop releases the claim.
   JoblessGrace = 300 * libtime.Second
   // JoblessCheckInterval is how often the lease loop asks JobControl whether a
   // held claim's Job is still running.
   JoblessCheckInterval = 60 * libtime.Second
   ```
   They are invariants: no flag, environment variable, CRD field or function option may be added for either.

2. Add two fields to `heldClaim` in the same file, after `holdUntil`, each with a short comment:
   - `joblessSince libtime.DateTime` — the instant of the first consecutive check that saw the Job not running; the zero value means "not currently jobless".
   - `lastJoblessCheckAt libtime.DateTime` — the instant of the last `IsJobRunning` call made by the jobless pass; the zero value means "never checked".

3. In `/workspace/pkg/claim/lease_manager_tick.go`, add `m.releaseJoblessClaims(ctx)` to `Tick` immediately after `m.releaseExpiredHolds(ctx)` and before `m.recheckRunning(ctx)`. Do not move any existing phase: the fence and the success-hold release must run first, and a claim whose adopted running check has not answered must be skipped by the jobless pass this tick and picked up on a later tick.

4. Implement the jobless pass in `/workspace/pkg/claim/lease_manager_tick.go` exactly as follows (add `stdtime "time"` to that file's imports; `errors`, `libtime` and `glog` are already imported):
   ```go
   // releaseJoblessClaims releases held claims whose Job has been continuously
   // absent for at least JoblessGrace. Claims owned by another release path keep
   // their own handling.
   func (m *leaseManager) releaseJoblessClaims(ctx context.Context) {
       now := m.currentDateTime.Now()
       for _, key := range m.heldKeys() {
           held, ok := m.held[key]
           if !ok || !joblessEligible(held) {
               continue
           }
           if !held.lastJoblessCheckAt.Time().IsZero() &&
               now.Sub(held.lastJoblessCheckAt) < JoblessCheckInterval {
               continue
           }
           held.lastJoblessCheckAt = now
           checkCtx, cancel := m.requestContext(ctx)
           running, err := m.jobControl.IsJobRunning(checkCtx, held.taskID)
           cancel()
           if err != nil {
               countClaim("error")
               continue
           }
           if running {
               held.joblessSince = libtime.DateTime(stdtime.Time{})
               continue
           }
           if held.joblessSince.Time().IsZero() {
               held.joblessSince = now
           }
           if now.Sub(held.joblessSince) < JoblessGrace {
               continue
           }
           m.releaseJoblessClaim(ctx, key, held)
       }
   }

   // joblessEligible reports whether the jobless release owns this claim. The
   // success hold, a pending release, a pending Job deletion and an unanswered
   // adopted running check each keep their own path.
   func joblessEligible(held *heldClaim) bool {
       return !held.pendingJobDeletion &&
           !held.releasePending &&
           !held.needsRunningCheck &&
           held.holdUntil.Time().IsZero()
   }

   // releaseJoblessClaim releases one claim whose Job is gone, logging and
   // counting the decision once. A failed DELETE keeps the claim with a pending
   // release, which retryPendingReleases handles without counting again.
   func (m *leaseManager) releaseJoblessClaim(ctx context.Context, key string, held *heldClaim) {
       countClaim("jobless_released")
       glog.Warningf(
           "event=claim_jobless_released key=%s task=%s jobless_since=%s",
           key,
           held.taskID,
           held.joblessSince.Time().UTC().Format(stdtime.RFC3339),
       )
       m.releaseAfterRunningCheck(ctx, key, held)
   }
   ```
   Behaviour this must preserve: the grace clock is left untouched by an erroring check; it clears when a check sees the Job running; `jobless_released` is counted once per claim because the claim is skipped by `joblessEligible` as soon as `releasePending` is set; a 409 drops the claim; any other DELETE error keeps it.

5. Register the new label in `/workspace/pkg/metrics/metrics.go`: add `"jobless_released"` to the closed claim result slice inside `init()` (the slice that currently ends with `"error"`), so the series exists at 0 before any claim activity. Do not change the `ClaimTotal` help text, its label name, or any other metric.

6. Update `/workspace/pkg/metrics/metrics_test.go`: rename the spec `It("pre-initializes all 13 claim result series", ...)` to `It("pre-initializes all 14 claim result series", ...)` and add `"jobless_released"` to its `ContainElements` list. This is the only existing test the change may modify.

7. Create `/workspace/pkg/claim/lease_manager_jobless_test.go` (`package claim_test`) holding exactly one spec — the AC 1 spec. Constraints that make it a v0.30.7 regression lock:
   - the top-level `Describe` text must contain `jobless`;
   - every duration is a literal (`300 * libtime.Second`, `370 * libtime.Second`, …), never `claim.JoblessGrace` or `claim.JoblessCheckInterval`;
   - it uses only helpers that exist at v0.30.7: `newFakeGitRest`, `newTestClock`, `newFakeJobControl`, `newTestManager`, `SetRunning`, `tickAt`, `snapshotClaims`, `requestsMatching`, `leaseTestT0`;
   - it must not reference any symbol introduced by this change, so it compiles against v0.30.7 and fails on an assertion (no `DELETE` received);
   - read the new counter with `testutil.ToFloat64(metrics.ClaimTotal.WithLabelValues("jobless_released"))` (a map miss on `snapshotClaims` would read as 0 and hide the bug).
   Spec shape: `Claim("claims/repo/bborbe-nuke", "4684419b")`; `job.SetRunning(map[string]bool{})`; `fake.ResetRequests()`; capture `before := testutil.ToFloat64(metrics.ClaimTotal.WithLabelValues("jobless_released"))` (and the same for `released`) and assert deltas from it — Ginkgo shuffles top-level containers, so an absolute 0 flakes; the pre-initialised-to-0 property is proven by the metrics spec in requirement 6; `tickAt(mgr, clock, leaseTestT0.Add(300*libtime.Second))` and assert zero `requestsMatching(fake.Requests(), "DELETE", key)`; read the current sha with `mgr.HeldClaims()[0].Sha` (renewals replace the sha that `Claim` returned — the fake assigns a new sha on every PUT); `tickAt(mgr, clock, leaseTestT0.Add(370*libtime.Second))` and assert exactly one `DELETE` for the key whose `RawQuery` contains that sha; assert `mgr.HeldClaims()` is empty; assert `jobless_released` and `released` each rose by exactly 1 (direct per-series reads — `expectClaimDelta` asserts zero for every unlisted series, so it cannot be used in a window that also renews); then `mgr.Claim(ctx, key, "ca0dc7eb")` succeeds.

8. Create `/workspace/pkg/claim/lease_manager_jobless_extra_test.go` (`package claim_test`) with the remaining specs. Any new helper these need is defined in this file, never in `lease_manager_test_helpers_test.go` or `fake_git_rest_test.go`. Use direct per-series counter reads for all delta assertions. Every spec starts from a fresh `newFakeGitRest` / `newTestClock(leaseTestT0)` / `newFakeJobControl(clock)` / `newTestManager("self", claim.NewClaimRefClient(fake.URL(), "", ""), job, clock)` in `BeforeEach`, and closes the fake in `AfterEach`.
   - **Never released while the Job runs (AC 3).** `SetRunning` true for the task; tick to `leaseTestT0.Add(30 * libtime.Minute)`. Assert zero `DELETE`s, at least 25 successful `PUT`s (`countSuccessfulPuts`), `job.runningCalls[taskID] <= 31` (the fake is single-threaded here), and `jobless_released` delta 0.
   - **Grace starts at Job stop (AC 4a).** Hold a mutable `map[string]bool{taskID: true}` passed to `SetRunning`; tick to `leaseTestT0.Add(600 * libtime.Second)`; set the map entry to `false`; tick to `+900 s` and assert zero `DELETE`s; tick to `+970 s` and assert exactly one `DELETE`.
   - **Grace resets on a running check (AC 4b).** Start not running; tick to `+200 s`; set running; tick to `+260 s`; set not running; tick to `+320 s`; tick to `+600 s` and assert zero `DELETE`s; tick to `+680 s` and assert exactly one `DELETE`.
   - **Other paths untouched (AC 5).** Four specs, each with `IsJobRunning` false for a 10-minute window and `jobless_released` delta 0:
     (a) success hold — tick to `leaseTestT0.Add(200 * libtime.Second)` first, then call `mgr.ReleaseAfterSuccess(ctx, key, clock.Now())` (so an unguarded jobless release, due at +310 s, would fire inside the hold); tick to `+490 s` and assert zero `DELETE`s and `jobless_released` delta 0, then tick to `+560 s` and assert one `DELETE` plus `released` delta 1;
     (b) pending release — tick to `leaseTestT0.Add(250 * libtime.Second)` first, then `fake.SetFault(faultMethod("DELETE", 500))` (the helper exists in `lease_manager_release_test.go`, same package), call `mgr.Release` (expect an error), keep the fault active, tick to `+600 s`, assert `jobless_released` delta 0;
     (c) pending Job deletion — `Claim` the key first (a create PUT under the fault fails), then set `job.deleteFn` to return an error and `fake.SetFault(faultMethod("PUT", 500))` so renewals fail and the lease is not extended; tick to `leaseTestT0.Add(240 * libtime.Second)` so `fenceClaim` sets `pendingJobDeletion`, then tick 10 more minutes and assert `jobless_released` delta 0;
     (d) adopted `needsRunningCheck` — seed `refs/claims/repo/bborbe-nuke` with `cluster "self"`, a task id and `rfc3339(leaseTestT0.Add(1000*libtime.Second))`, set `job.runningFn` to return `(false, stderrors.New("kube api unavailable"))`, call `mgr.Recover`, then tick to `leaseTestT0.Add(600 * libtime.Second)` with the same erroring function and assert `jobless_released` delta 0 and `job.runningCalls[taskID]` equal to exactly 61 (1 from `Recover` + one `recheckRunning` call per tick, 60 ticks). The call count is the assertion that can fail: with an always-erroring check an unguarded jobless pass never releases, but it adds one `IsJobRunning` call per 60 s.
   - **Lookup errors keep the claim (AC 6).** Claim, set `job.runningFn` to return an error, tick 10 minutes. Assert zero `DELETE`s, `HeldClaims()` still lists the key, `error` rose, `jobless_released` delta 0.
   - **Erroring check keeps the grace clock (DB 2).** Claim; `job.runningFn` returns `(false, stderrors.New("kube api unavailable"))` on calls 3 and 4 (the +130 s and +190 s checks) and `(false, nil)` otherwise. Tick to `leaseTestT0.Add(300 * libtime.Second)` and assert zero `DELETE`s; tick to `+370 s` and assert exactly one `DELETE`, `error` delta 2 and `jobless_released` delta 1. An implementation that resets the clock on error would not release before +550 s.
   - **DELETE 500 then retry (AC 7a).** Claim, `SetRunning` false, install a fault that answers the first `DELETE` with 500 (a counter in the closure; `Apply` stays false). Tick to `leaseTestT0.Add(310 * libtime.Second)` (the decision tick). Assert exactly one `DELETE`, the claim is still held, `jobless_released` delta 1, `error` delta 1, `released` delta 0. Then tick to `leaseTestT0.Add(320 * libtime.Second)` (the fault answered only the first DELETE, so the retry succeeds), and assert the claim is gone, `released` delta 1, exactly two `DELETE`s in total and `jobless_released` still 1.
   - **DELETE 409 (AC 7b).** Claim, `SetRunning` false, fault every `DELETE` with 409. Tick to `+370 s`. Assert the claim is gone, `already_released` delta 1, `jobless_released` delta 1, exactly one `DELETE`.
   - **Adopted repository-keyed claim freed (AC 8).** Seed `refs/claims/repo/bborbe-nuke` for cluster `self` with `rfc3339(leaseTestT0.Add(1000*libtime.Second))`; `SetRunning` true; `mgr.Recover`; assert `HeldClaims()` lists the key; `fake.ResetRequests()`; set the map entry to `false`; tick to `leaseTestT0.Add(370 * libtime.Second)` and assert exactly one `DELETE` for the key and `jobless_released` delta 1.

9. Update `/workspace/docs/claim-lease-protocol.md`:
   - add a `## Constants` row `| Jobless grace | 300 s | A held claim whose Job has been continuously not running for this long is released. |` and a row `| Jobless check interval | 60 s | How often the lease loop asks whether a held claim's Job is still running. |`;
   - add a state-machine bullet for the jobless release: the claim is deleted with the held sha after the grace, the task becomes eligible for respawn, and the success hold, pending release, pending Job deletion and unanswered adopted running check are excluded;
   - change the count in `all 13 result series are pre-initialised to 0` from `13` to `14` — the phrase spans a line break (lines 49–50: `all 13 result series` / `are pre-initialised to 0`), so edit the `13` on line 49 rather than matching the full phrase.
   The word `jobless` must appear at least three times in the file, and the exact phrase `14 result series` exactly once.

10. Update `/workspace/CHANGELOG.md`: add exactly one bullet whose prefix is `fix:` and which contains the word `jobless` under `## Unreleased`. If a `## Unreleased` section already exists (the spec 012 prompt also creates one), append the bullet to it and do not add a second heading; otherwise create `## Unreleased` immediately below the preamble, before the newest `## v…` section. Describe what was implemented — a held claim is now released when its Job has been continuously absent for 300 s, so a Job deleted by hand or whose terminal event was lost no longer blocks the repository forever — not what was verified. Do not add a bullet for the documentation change.

11. Keep all new production code paths covered: `releaseJoblessClaims`, `joblessEligible`, `releaseJoblessClaim` (success, 409, error branches) and the running / erroring / grace-expiry branches must each be exercised by the specs above. New code must reach ≥80% statement coverage.

12. Before finishing, re-run every `<verification>` command and confirm each expected output, then walk AC 1–11 of spec 011 against the change.
</requirements>

<constraints>
- Do NOT change lease timing: lease length, tick, renew interval, fence margin, fence lead, takeover grace and the request cap all stay as they are.
- Do NOT change the expired-lease takeover path, the concurrency cap, dispatch or respawn logic.
- Do NOT wire `AuditMismatch` into production and do not change it.
- Do NOT change claim keys.
- Do NOT make the jobless grace or the check interval configurable — no flag, env var, CRD field, function option or struct field. They are invariants like the other lease constants.
- `ClaimTotal` labels stay a closed set; keys and task ids are never label values.
- The success hold, pending-release, pending-Job-deletion and adopted-running-check paths must behave exactly as before.
- The fence runs before the jobless check in a tick.
- Existing tests pass unmodified, with the single exception of the metrics spec named in requirement 6. Do not add `jobless_released` to the `claimResults` slice in `lease_manager_test_helpers_test.go` and do not add helpers to `lease_manager_test_helpers_test.go` or `fake_git_rest_test.go`.
- The log format string `event=claim_jobless_released key=%s task=%s jobless_since=%s` must appear exactly once across `pkg/claim/*.go` — do not repeat it in test files or comments.
- No new scenario file: the fake clock, fake git-rest and fake JobControl reach every behaviour.
- Do NOT commit — dark-factory handles git.
- Every Go file keeps the existing two-line license header.
</constraints>

<verification>
Run from the repo root:

```
ROOTDIR=/workspace make test
ROOTDIR=/workspace make precommit
cat pkg/claim/*.go | grep -c 'event=claim_jobless_released key=%s task=%s jobless_since=%s'
grep -n '"jobless_released"' pkg/metrics/metrics.go
grep -ciE 'jobless' docs/claim-lease-protocol.md
grep -c '14 result series' docs/claim-lease-protocol.md
awk '/^## /{sec=$0} /jobless/{print "sits under: " sec}' CHANGELOG.md
test "$(grep -c '^## Unreleased' CHANGELOG.md)" = 1
```

Expected: `make test` and `make precommit` exit 0; the first grep prints `1`; the second prints the line in the pre-init slice; the third prints a number ≥ 3; the fourth prints `1`; the last prints at least one line and every printed line reads `sits under: ## Unreleased`.
</verification>
