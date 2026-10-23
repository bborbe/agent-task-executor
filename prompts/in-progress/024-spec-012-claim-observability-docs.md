---
status: approved
spec: [012-bug-pr-review-claims-serialize-per-repo]
created: "2026-10-23T12:26:59Z"
queued: "2026-10-23T12:36:11Z"
branch: dark-factory/bug-pr-review-claims-serialize-per-repo
---

# Log claim acquisition and release, and document the key shapes

<summary>
- Every successful claim now leaves one info log line, whether it was taken by a fresh claim or by a takeover after a conflict.
- A claim that cannot be taken logs nothing new, so an operator sees exactly the acquisitions that succeeded.
- Every path that drops a claim now leaves one `claim_released` line per removal, naming the key, the task and the reason (a jobless removal also logs its own `claim_jobless_released` line).
- The release line is produced by one helper, so the four reasons (released, already released, fenced, claim lost) cannot drift apart.
- The existing counters keep their exact values; only the log lines are added.
- The lease-protocol document gains a claim-key section: the three key shapes, the standalone-URL rule, the fallback, and the two new log lines.
- The changelog gains one fix bullet describing the per-pull-request key.
- Existing tests and all lease timing are unchanged.
- This is the observability and documentation half of the fix; the key-derivation change itself ships in the previous prompt.
</summary>

<objective>
Make claim concurrency observable at runtime: log one `event=claim_acquired` line on each successful spawn claim, and route every claim-removal decision through one helper that logs `event=claim_released key=<key> task=<id> result=<label>` while preserving the existing `executor_claim_total` counters. Document the three claim-key shapes, the standalone-URL rule, the fallback and the two log lines in `docs/claim-lease-protocol.md`, and add the changelog bullet.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for the container agent's execution rules (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, external test packages, coverage ≥80% for new code, license header on every Go file). This repo has no project-level `CLAUDE.md`.

Read these files fully before changing anything:
- `pkg/handler/claim_wiring.go` — `acquireSpawnClaim`: the `Claim` success branch and the `Takeover` success branch are the only two places that may log `claim_acquired`; `claimFailureDecision` must not log it.
- `pkg/handler/claim_key_body_internal_test.go` — created by the previous prompt; it defines `captureGlogStderr(t *testing.T, fn func()) string`, `casClaimRefClient`, `prReviewClaimTask`, and the key/fallback tests. Reuse `captureGlogStderr`; do not define another copy in `package handler`.
- `pkg/handler/claim_gate_internal_test.go` — `newGateHandler`, `gateTask`, `takeoverClient`, `takeoverClaimRef`, `snapshotGateClaims`, `expectGateClaimDelta`. The takeover helpers build the two success/failure cases for the acquisition test.
- `pkg/claim/lease_manager.go` — `countClaim`, `Release`, `ReleaseAfterSuccess`, `releaseLocked`, `recoverFence`, `recoverAdopt`, and the `heldClaim` fields (`key`, `taskID`, `sha`, `releasePending`, `pendingJobDeletion`, `pendingDeletionPath`). `countClaim` stays for every non-removal result.
- `pkg/claim/lease_manager_tick.go` — `fenceClaim`, `retryPendingReleases`, `retryPendingDeletions`, `releaseExpiredHolds`, `releaseAfterRunningCheck`, `claimLost`. Every site that currently counts `released`, `already_released`, `fenced` or `claim_lost` is in scope.
- `pkg/claim/lease_manager_test_helpers_test.go` — `newFakeGitRest`, `newTestClock`, `newFakeJobControl`, `newTestManager`, `SetRunning`, `tickAt`, `rfc3339`, `leaseTestT0`, `claimResults`, `snapshotClaims`, `expectClaimDelta`.
- `pkg/claim/fake_git_rest_test.go` — `newFakeGitRest`, `Seed`, `SetFault`, `faultMethod`, `ResetRequests`, `Requests`.
- `pkg/claim/lease_manager_release_test.go` — the release-outcome test style and the `faultMethod` helper.
- `pkg/claim/lease_manager_tick_test.go` — the fence and lost-claim test shapes (`Seed` a foreign sha, `clock.SetNow`, `Tick`).
- `pkg/claim/success_grace.go` — `SuccessGracePeriod` (300 s) and the hold semantics.
- `pkg/handler/task_view_test.go` — the glog capture pattern the internal helper mirrors.
- `docs/claim-lease-protocol.md` — the `## Constants`, `## Claim state machine`, `## Fence`, `## Takeover` and `## Restart recovery` sections. There is no key section today.
- `CHANGELOG.md` — the preamble, then either `## v0.30.7` or a `## Unreleased` section already created by the spec 011 prompt (run order is not fixed).

Verified facts (do not re-derive):
- `pkg/claim` already imports `glog` (`github.com/golang/glog`) and `pkg/metrics`.
- `countClaim(result string)` increments `metrics.ClaimTotal.WithLabelValues(result)`.
- `pkg/handler/claim_wiring.go` already imports `glog`; the previous prompt added the `pkg/metrics` import.
- The `ClaimTotal` label set is closed (13 values at v0.30.7, 14 once the spec 011 prompt adds `jobless_released`) and `executor_claim_total` is pre-initialised in `pkg/metrics/metrics.go`; do not add or rename a label.
- `fakeJobControl.IsJobRunning` returns `false, nil` by default, so `Release` and `releaseLocked` proceed to the ref `DELETE`.
- `mgr.ReleaseAfterSuccess(ctx, key, clock.Now())` sets the hold to `now + SuccessGracePeriod`; stepping with `tickAt` to `leaseTestT0 + 310 s` releases it (renewals keep the lease ahead of the fence); a single `SetNow(+310 s)` + `Tick` fences it instead.
- `pkg/handler/claim_gate_internal_test.go` imports the standard `time` package as `stdtime`; use `stdtime.Now()` and `stdtime.Second` in the new handler test.

Relevant coding docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2, external `_test` package, coverage ≥80% for new code.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-doc-best-practices.md` — GoDoc comments start with the name and describe behaviour.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-precommit.md` — golines 100-char limit, funlen 80, nestif 4, license headers.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new code.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` placement and the required `fix:` prefix.

</context>

<requirements>
1. Add one acquisition helper to `pkg/handler/claim_wiring.go` and call it from both success branches of `acquireSpawnClaim`:
   ```go
   // logClaimAcquired records one successful spawn claim, whether it was taken by
   // a fresh Claim or by a Takeover after a conflict.
   func logClaimAcquired(key string, taskID lib.TaskIdentifier) {
       glog.Infof("event=claim_acquired key=%s task=%s", key, taskID)
   }
   ```
   Call `logClaimAcquired(key, task.TaskIdentifier)` immediately before `return true` after `manager.Claim(...)` returns nil, and immediately before `return true` after `manager.Takeover(...)` returns nil. Do not log when both fail. The format string must appear exactly once in the file.

2. Add one release helper to `pkg/claim/lease_manager.go`, immediately after `countClaim`:
   ```go
   // countClaimRemoval increments executor_claim_total{result} and logs the one
   // release line for a claim this manager stops holding. Every path that counts
   // released, already_released, fenced or claim_lost calls it. The pending
   // Job-deletion retry does not: its fence was already counted and logged when
   // the deletion was first attempted.
   func countClaimRemoval(key string, taskID string, result string) {
       metrics.ClaimTotal.WithLabelValues(result).Inc()
       glog.Infof("event=claim_released key=%s task=%s result=%s", key, taskID, result)
   }
   ```
   (Requirement 3 extends this signature with a `sha` argument and a WARNING level for `claim_lost`; the removal is logged at decision time, and `retryPendingDeletions` deletes the ref without calling the helper again.)
   The format string must appear exactly once across `pkg/claim/*.go` (non-test files).

3. Replace every `countClaim("released")`, `countClaim("already_released")`, `countClaim("fenced")` and `countClaim("claim_lost")` in non-test code with `countClaimRemoval(...)`, passing the key and the held or listed task id:
   - `releaseLocked` (in `lease_manager.go`): both outcomes use `held.taskID`.
   - `retryPendingReleases` (in `lease_manager_tick.go`): both outcomes use `held.taskID`.
   - `fenceClaim` (in `lease_manager_tick.go`): all three `fenced` sites — the `releasePending` branch, the failed-`DeleteJob` branch, and the success branch — use `held.taskID`.
   - `recoverFence` (in `lease_manager.go`): both `fenced` sites use `ref.TaskID`.
   - `recoverAdopt` (in `lease_manager.go`): both outcomes use `ref.TaskID`.
   - `releaseAfterRunningCheck` (in `lease_manager_tick.go`): both outcomes use `held.taskID`.
   - `claimLost` (in `lease_manager_tick.go`): use `held.taskID`, and delete the existing `glog.Warningf("claim lost key=%s task_id=%s sha=%s", ...)` block so the removal leaves one `claim_released` line. Keep the WARNING level: the helper call on the claim_lost path must log at WARNING level and include `sha=`; do not downgrade it to INFO. To do this, give `countClaimRemoval` a fourth parameter `sha string` and append ` sha=%s` to its format string (the `event=claim_released key=%s task=%s result=%s` prefix is unchanged), build the line once, and emit it with `glog.Warning` when `result == "claim_lost"` and `glog.Info` otherwise; every caller passes the claim's sha (`held.sha` / `ref.Sha`).
   Leave every `countClaim("error")` and every other non-removal result (`claimed`, `conflict`, `renewed`, `recovered`, `renew_failed`, `taken_over`, `takeover_too_early`, `adopted`) unchanged. Leave `retryPendingDeletions` without any counter or log call: it completes a fence that was already counted and logged, and the existing spec-009 tests assert no counter change at that retry. Do not change any counter value or label.

4. Add the release-log test in a NEW file `pkg/claim/claim_released_log_test.go` (`package claim_test`, Ginkgo, license header). Add a local `captureGlogStderr(fn func()) string` helper modelled on `pkg/handler/task_view_test.go`'s `captureStderr` (set `logtostderr` true, swap `os.Stderr` for a pipe, run, `glog.Flush()`, restore, read). One `It` per label, each asserting the captured output contains `event=claim_released key=<key> task=<task> result=<label>`:
   - **released** — `mgr.Claim(ctx, key, task)`, then capture `mgr.Release(ctx, key)`; the Job is not running by default, so the ref `DELETE` succeeds.
   - **already_released** — `fake.SetFault(faultMethod(http.MethodDelete, http.StatusConflict))`, then `Claim`, then capture `Release`; it returns nil.
   - **fenced** — `Claim`, then capture the `Tick` at `leaseTestT0 + 240 s` (`clock.SetNow(...)` then `mgr.Tick(ctx)`); the fence deletes the Job and sends the best-effort ref `DELETE`.
   - **claim_lost** — `Claim`, then `fake.Seed("refs/"+key, "S-other", "other", "t-other", rfc3339(leaseTestT0.Add(claim.LeaseDuration)))`, then capture the `Tick` at `leaseTestT0 + 60 s`; the renew's compare-and-swap returns 409 and the claim is dropped.
   - **hold expiry** — `Claim`, `mgr.ReleaseAfterSuccess(ctx, key, clock.Now())`, then `tickAt(mgr, clock, leaseTestT0.Add(290 * libtime.Second))` outside the capture (the 10 s ticks renew the lease, so the fence never comes due; the fake's new sha per PUT is tracked in `held.sha`), then capture `tickAt(mgr, clock, leaseTestT0.Add(310 * libtime.Second))`; `releaseExpiredHolds` releases it through `releaseLocked` at +300 s. Assert the output contains `result=released` and does NOT contain `result=fenced`. Never jump with one `SetNow` + `Tick` here: with no renewals the lease ends at +300 s and the fence runs first.
   Use one key and one task id per `It`, and keep the existing counter assertions untouched (this file asserts logs only). Do not add these specs to an existing file.
   The new file needs its own imports (imports are per file): `context`, `flag`, `io`, `net/http`, `os`, `libtime "github.com/bborbe/time"`, `"github.com/golang/glog"`, `. "github.com/onsi/ginkgo/v2"`, `. "github.com/onsi/gomega"`, and `"github.com/bborbe/agent-task-executor/pkg/claim"`. Import only what the file uses.

5. Add the acquisition-log test in a NEW file `pkg/handler/claim_acquired_internal_test.go` (`package handler`, plain Go tests, license header), reusing `captureGlogStderr` from `claim_key_body_internal_test.go`:
   - **Claim success** — `newGateHandler(pkg.ClaimModeOn, &recordingClaimRefClient{})`; capture `acquireSpawnClaim(ctx, gateTask("", ""))`; assert it returned true and the output contains `event=claim_acquired key=claims/task/tid-gate-1 task=tid-gate-1`.
   - **Takeover success** — `newGateHandler(pkg.ClaimModeOn, takeoverClient(takeoverClaimRef("expired-sha", stdtime.Now().UTC().Add(-31*stdtime.Second))))`; capture `acquireSpawnClaim(ctx, gateTask("", ""))`; assert it returned true and the output contains `event=claim_acquired key=claims/task/tid-gate-1 task=tid-gate-1`.
   - **Both fail** — `newGateHandler(pkg.ClaimModeOn, takeoverClient(takeoverClaimRef("live-sha", stdtime.Now().UTC().Add(5*stdtime.Minute))))`; capture `acquireSpawnClaim(ctx, gateTask("", ""))`; assert it returned false and the output does not contain `event=claim_acquired`.
   The new file imports `context`, `strings`, `testing`, `stdtime "time"`, `pkg "github.com/bborbe/agent-task-executor/pkg"`, and `"github.com/bborbe/agent-task-executor/pkg/claim"` only if it names `claim` directly (the takeover helpers return `claim.ClaimRef` through `takeoverClaimRef`); drop any import it does not use.

6. Add a `## Claim keys` section to `docs/claim-lease-protocol.md`, placed after `## Constants` and before `## Claim state machine`. It must document:
   - the three key shapes: `claims/task/<task id>` for task-id-keyed types; `claims/repo/<owner>-<repo>` for `update-go`, `release`, `build-fix`, and the `pr-review` fallback; `claims/pr/<owner>-<repo>-<n>` for `pr-review` with a usable PR link;
   - the standalone-URL rule: the first body line that, after trimming surrounding whitespace, is exactly `https://github.com/<o>/<r>/pull/<n>`, where `<o>/<r>` matches the task repository case-insensitively, `<n>` is 1–10 digits with a value of at least 1, and nothing follows `<n>`;
   - the fallback: no qualifying line means `claims/repo/<owner>-<repo>`, one `event=claim_key_fallback task=<id> repo=<owner>/<repo>` warning, and one increment of `executor_claim_key_fallback_total`;
   - the two log lines: `event=claim_acquired key=<key> task=<id>` on a successful `Claim` or `Takeover`, and `event=claim_released key=<key> task=<id> result=<label>` on every removal, with `result` one of `released`, `already_released`, `fenced`, `claim_lost`;
   - that owner and repo are lowercased in every key, and that keys and task ids are never metric label values.
   The file must contain each of the literal strings `claims/task/`, `claims/repo/`, `claims/pr/`, `claim_key_fallback`, `claim_acquired` and `claim_released`. Keep the existing `all N result series` sentence (N is 13, or 14 if the spec 011 prompt already landed) and every existing section intact; do not change that count.

7. Add exactly one `fix:` bullet containing the literal `claims/pr/` under `## Unreleased` in `CHANGELOG.md`. If a `## Unreleased` section already exists (the spec 011 prompt also creates one), append the bullet to it and do not add a second heading; otherwise create `## Unreleased` immediately after the preamble and before the newest `## v…` section. Describe what shipped: a `pr-review` task is now claimed per pull request (`claims/pr/<owner>-<repo>-<n>`), so reviews of different pull requests in one repository run in parallel while two tasks for the same pull request still exclude each other; a body with no usable PR link falls back to the repository key with a warning and a counter; the other repo-keyed types are unchanged. Do not add a bullet for the documentation change and do not describe what was verified.

8. Cover every new branch: `logClaimAcquired` on both success paths and its absence on failure; `countClaimRemoval` for all four labels through the five paths in requirement 4. New code must reach ≥80% statement coverage.
</requirements>

<constraints>
- `DeriveClaimKey`, `claimOwnerRepo`, `pkg/claim/key_test.go` and `pkg/handler/claim_owner_repo_internal_test.go` are unchanged (the previous prompt already added the body-aware entry point; do not change it here).
- Do NOT change lease timing, the concurrency cap, the fence, takeover, recovery or the success hold.
- Do NOT change any `executor_claim_total` label, its pre-initialisation, or the value it counts. Only the log lines are new.
- Metric labels never carry keys or task ids. The `executor_claim_key_fallback_total` counter has no labels.
- Do NOT add a `pr_number` frontmatter key, a new flag, environment variable, CRD field or function option.
- Do NOT release orphaned claims; that is a sibling spec.
- All existing tests pass unmodified; this prompt may only add new test files.
- The two log format strings appear exactly once each in non-test code: `event=claim_acquired` in `pkg/handler/claim_wiring.go`, `event=claim_released` in `pkg/claim/lease_manager.go`.
- Do NOT commit — dark-factory handles git.
- Every Go file keeps the existing two-line BSD license header.
</constraints>

<verification>
Run from the repo root:

```
ROOTDIR=/workspace make test
ROOTDIR=/workspace make precommit
grep -c 'event=claim_acquired' pkg/handler/claim_wiring.go
grep -c 'event=claim_released' pkg/claim/lease_manager.go
! grep -nE 'countClaim\("(released|already_released|fenced|claim_lost)"\)' pkg/claim/*.go | grep -v _test.go
for t in 'claims/task/' 'claims/repo/' 'claims/pr/' claim_key_fallback claim_acquired claim_released; do grep -q "$t" docs/claim-lease-protocol.md || echo "MISSING: $t"; done
awk '/^## /{sec=$0} /claims\/pr\//{print "sits under: " sec}' CHANGELOG.md
test "$(grep -c '^## Unreleased' CHANGELOG.md)" = 1
```

Expected: `make test` and `make precommit` exit 0; the first grep prints `1`; the second prints `1`; the negated grep pipeline exits 0 with no output (no literal removal label reaches `countClaim`); the loop prints nothing; the `awk` prints at least one line and every printed line is `sits under: ## Unreleased`.

Before finishing, re-run the block above and walk each acceptance criterion (AC 5, 6, 7, 8) against the change.
</verification>
