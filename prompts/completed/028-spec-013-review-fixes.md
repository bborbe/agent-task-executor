---
status: completed
spec: [013-bug-held-claim-outlives-vanished-job]
summary: Bounded the jobless pass by a 10s injected-clock budget with oldest-check-first ordering, stopped logging a release for pending fences (fence-pending line plus deferred release), wrapped per-PR key validation errors, and converted release logging to format-string glog calls.
execution_id: agent-task-executor-claim-reaper-exec-028-spec-013-review-fixes
dark-factory-version: v0.198.7
created: "2026-10-23T13:18:19Z"
queued: "2026-10-23T13:26:38Z"
started: "2026-10-23T13:29:20Z"
completed: "2026-10-23T13:38:26Z"
---

<summary>
- One jobless-release pass starts no new Job lookups once 10 seconds have passed, so the pass holds the lease lock for at most 10 seconds plus one lookup's timeout instead of minutes.
- Claims the pass did not reach are checked first on the following ticks, so a slow Kubernetes API delays checks but never starves any claim.
- A pass whose context is already cancelled makes no Job lookups and counts no errors.
- A fence whose Job deletion fails no longer logs a release that did not happen. It logs a "fence pending" line and still counts the fence once.
- The release line for that fence appears once, when a later tick deletes the Job and the ref. That tick does not count the fence again.
- Errors from the pull-request claim-key validation now carry context saying where they came from.
- The claim release log line is written with format-string logging. The text it prints does not change.
- No timing constant, metric label or claim key changes.
</summary>

<objective>
When this prompt is done, the spec-013 claim code on this branch has four properties it lacks today:
- The jobless pass can no longer hold the lease lock for minutes. The lock blocks `Claim`, `Release` and the rest of the tick.
- The jobless pass visits due claims oldest-check-first, so a slow Kubernetes API cannot starve any claim.
- A fence whose Job deletion failed no longer logs a release. Operators reading `event=claim_released` must be able to trust that the claim is gone.
- Per-PR key validation errors say where they came from.

Release logging also uses format-string glog calls. Nothing else changes behaviour.
</objective>

<context>
Read `CLAUDE.md` for project conventions.

Read these files in full before changing anything (line numbers are hints taken from the current branch; anchor on the function names):
- `pkg/claim/lease_manager_tick.go`:
  - `Tick` (lines 38–58);
  - `retryPendingDeletions` (lines 62–77);
  - `fenceClaim` (lines 95–112, failure branch 102–108);
  - `releaseJoblessClaims` (lines 169–200);
  - `joblessEligible` (lines 205–210);
  - `heldKeys` lives in `lease_manager.go` (lines 217–224) and returns the held keys sorted.
- `pkg/claim/lease_manager.go`:
  - the `const` block (lines 23–43), including `JoblessGrace` and `JoblessCheckInterval`;
  - the `heldClaim` struct (lines 116–138);
  - `countClaim` (lines 151–153);
  - `countClaimRemoval` (lines 155–174);
  - `requestContext` (lines 250–254);
  - `recoverFence` (lines 610–638, failure branch 621–634).
- `pkg/claim/ref_client.go`: `const RequestTimeout = 30 * libtime.Second` (line 24).
- `pkg/claim/key.go`:
  - `DeriveClaimKeyWithBody` (lines 57–86), per-PR branch lines 72–83;
  - `validateClaimComponent` (lines 136–159).
- Test helpers in `pkg/claim/lease_manager_test_helpers_test.go`:
  - `leaseTestT0`;
  - `snapshotClaims`;
  - `expectClaimDelta`;
  - `newTestClock`;
  - `advance`;
  - `newTestManager`;
  - `fakeJobControl`, with its unexported fields `runningFn func(taskID string, call int) (bool, error)` and `deleteFn func(taskID string, call int) error`. Tests in package `claim_test` set these directly, e.g. `job.deleteFn = ...`;
  - `requestsMatching`.
- `pkg/claim/claim_released_log_test.go`: `captureGlogStderr` and the existing `"logs one line with result=fenced"` spec.
- `pkg/claim/lease_manager_tick_test.go`: the `"retries a failed Job deletion on the fence path"` spec. It must keep passing unchanged: fence tick deltas `fenced: 1, error: 1`, retry tick delta `{}`.
- `pkg/claim/lease_manager_restart_test.go`: the `"keeps a fence claim pending when its Job deletion fails"` spec. It must keep passing unchanged.
- `pkg/claim/success_grace_internal_test.go`. It shows the convention for internal tests in package `claim`: plain Go `testing` tests, not Ginkgo, because Ginkgo specs in the internal package are not discovered. It also defines the reusable `stepClock` type: `Now() libtime.DateTime` (line 89), `get() time.Time` (line 95) and `set(at time.Time)` (line 101).
- `pkg/claim/key_body_test.go`: `Describe("DeriveClaimKeyWithBody", ...)` with its `DescribeTable`.
- `pkg/claim/lease_manager_jobless_test.go`. Spec 013 AC 2 requires this file to hold only the AC 1 spec. Do NOT add specs to it.
- `docs/claim-lease-protocol.md`: the "Two log lines make claims observable at runtime" list (around line 47).
- `CHANGELOG.md`: the `## Unreleased` section. Line 8 is the existing `fix: **a held claim is now released when its Job has been continuously absent for 300 s.**` bullet.

Coding guides (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-time-injection.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
</context>

<requirements>
1. **Per-pass time budget and fair ordering in `releaseJoblessClaims` (`pkg/claim/lease_manager_tick.go`).**

   The pass runs under `m.mu` (taken in `Tick`). It makes one `IsJobRunning` call per due held claim, and each call can take up to `RequestTimeout` (30 s), so with many claims it holds the lock for minutes. A budget alone is not enough: under sorted-key order a claim the budget skips keeps its old `lastJoblessCheckAt`, so when each call takes 10 s or more the checks go a,b,c,a,b and d,e are never reached. The pass therefore also visits due claims oldest-check-first.

   a. Add an unexported constant directly above `releaseJoblessClaims`, with this doc comment:
   ```go
   // joblessPassBudget stops one jobless pass from starting further Job checks
   // once this much time has passed since the pass began; a check already
   // started may still run up to RequestTimeout.
   const joblessPassBudget = 10 * libtime.Second
   ```

   b. Add `"sort"` to the import block of `lease_manager_tick.go`, and add this method directly below `releaseJoblessClaims`:
   ```go
   // joblessDueKeys returns the keys of held claims the jobless pass owns and
   // whose check is due at now. Never-checked claims come first, then the oldest
   // lastJoblessCheckAt; ties keep heldKeys order, which is sorted by key.
   func (m *leaseManager) joblessDueKeys(now libtime.DateTime) []string {
   	keys := make([]string, 0, len(m.held))
   	for _, key := range m.heldKeys() {
   		held := m.held[key]
   		if !joblessEligible(held) {
   			continue
   		}
   		if !held.lastJoblessCheckAt.Time().IsZero() &&
   			now.Sub(held.lastJoblessCheckAt) < JoblessCheckInterval {
   			continue
   		}
   		keys = append(keys, key)
   	}
   	sort.SliceStable(keys, func(i, j int) bool {
   		return m.held[keys[i]].lastJoblessCheckAt.Time().Before(
   			m.held[keys[j]].lastJoblessCheckAt.Time(),
   		)
   	})
   	return keys
   }
   ```
   A zero `lastJoblessCheckAt` is before every non-zero instant, so never-checked claims sort first without a special case.

   c. Rewrite `releaseJoblessClaims` to the following. The eligibility and interval checks have moved into `joblessDueKeys`. The `ctx.Err()` check and the budget check are new. Every line from `held.lastJoblessCheckAt = now` down is unchanged:
   ```go
   func (m *leaseManager) releaseJoblessClaims(ctx context.Context) {
   	now := m.currentDateTime.Now()
   	for _, key := range m.joblessDueKeys(now) {
   		if ctx.Err() != nil {
   			return
   		}
   		if m.currentDateTime.Now().Sub(now) >= joblessPassBudget {
   			return
   		}
   		held, ok := m.held[key]
   		if !ok {
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
   ```
   - The budget is measured with the injected clock, `m.currentDateTime.Now()`. Never use `time.Now()`.
   - Due claims are visited oldest-check-first, as ordered by `joblessDueKeys`. A claim the pass stops in front of keeps its old `lastJoblessCheckAt`, so on the next tick it sorts ahead of every claim checked more recently.
   - `now` (pass start) stays the value written to `lastJoblessCheckAt` and `joblessSince`. Do not change that.
   - Do NOT change `JoblessGrace`, `JoblessCheckInterval`, `TickInterval`, `RequestTimeout` or any other existing constant.

   d. **Budget tests.** Create the new Ginkgo file `pkg/claim/lease_manager_jobless_budget_test.go` in package `claim_test`, with the standard copyright header. Do NOT put it in `lease_manager_jobless_test.go`.
   - Set up as in `lease_manager_jobless_extra_test.go`:
     - `ctx = context.Background()`;
     - `fake = newFakeGitRest()`;
     - `clock = newTestClock(leaseTestT0)`;
     - `job = newFakeJobControl(clock)`;
     - `mgr = newTestManager("self", claim.NewClaimRefClient(fake.URL(), "", ""), job, clock)`;
     - `fake.Close()` in `AfterEach`.
   - Claim 5 keys at `leaseTestT0`. Their sort order must be deterministic: `claims/task/budget-a` … `claims/task/budget-e`, with tasks `t-budget-a` … `t-budget-e`.
   - Set `job.runningFn`:
     - It records each `taskID` into a `checked []string` slice. The fake calls `runningFn` while holding its own mutex, so appending there is safe.
     - It advances the clock by 4 s with `advance(clock, 4*libtime.Second)`.
     - It returns `true, nil`.
   - Tick 1: `clock.SetNow(leaseTestT0.Add(10 * libtime.Second))`, then `Expect(mgr.Tick(ctx)).To(Succeed())`. Assert both:
     - `len(checked) <= 3`;
     - `checked` equals `[]string{"t-budget-a", "t-budget-b", "t-budget-c"}`. Elapsed is 0, 4 and 8 s before calls 1–3, and 12 s ≥ 10 s before call 4.
   - Tick 2: `advance(clock, 10*libtime.Second)`, then `Expect(mgr.Tick(ctx)).To(Succeed())`. Assert `checked` equals all five task ids in order, a–e. Claims a–c are inside `JoblessCheckInterval` and are skipped; d and e are checked.
   - Assert `mgr.HeldClaims()` still has length 5. Nothing is released while the Jobs run.
   - Add a second spec in the same file, `It("does not starve late claims when only one check fits per tick", ...)`:
     - same setup and the same 5 claims at `leaseTestT0`;
     - `job.runningFn` records `taskID` into `checked`, advances the clock by 12 s (`advance(clock, 12*libtime.Second)`) and returns `true, nil`;
     - five times: `advance(clock, claim.TickInterval)`, then `Expect(mgr.Tick(ctx)).To(Succeed())`;
     - assert `checked` equals `[]string{"t-budget-a", "t-budget-b", "t-budget-c", "t-budget-d", "t-budget-e"}`. Under plain key order the result would be a,b,c,a,b: by the fourth tick claim a is due again and sorts ahead of the never-checked d.

   e. **Cancelled-context test.** Create the new file `pkg/claim/jobless_pass_internal_test.go` in package `claim`. Make it a plain Go `testing` test, not Ginkgo, following `success_grace_internal_test.go`, with the copyright header.
   - Define a small JobControl fake, e.g. `ctxAwareJobControl`, that counts `IsJobRunning` calls. It returns `false, ctx.Err()` when `ctx.Err() != nil`, else `true, nil`. `DeleteJob` returns `nil`. Returning the context error is what makes the test discriminating: without the new check the pass would make 1 call and count 1 `error`.
   - Build the manager directly:
     - `clock := &stepClock{now: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)}` — reuse the existing `stepClock` type; do not redeclare it.
     - `m := &leaseManager{cluster: "self", jobControl: job, currentDateTime: clock, held: map[string]*heldClaim{"claims/task/ctx": {key: "claims/task/ctx", taskID: "t-ctx", leaseUntil: libtime.DateTime(clock.get().Add(300 * time.Second))}}}`.
   - Read the error metric before and after with `testutil.ToFloat64(metrics.ClaimTotal.WithLabelValues("error"))`. Imports: `github.com/prometheus/client_golang/prometheus/testutil` and `github.com/bborbe/agent-task-executor/pkg/metrics`.
   - Create `ctx, cancel := context.WithCancel(context.Background())`, then call `cancel()`, then `m.releaseJoblessClaims(ctx)`.
   - Assert all three:
     - zero `IsJobRunning` calls;
     - error delta `0`;
     - `m.held["claims/task/ctx"].lastJoblessCheckAt.Time().IsZero()` is true.

2. **No false release line for a pending fence.**

   Today, when `DeleteJob` fails, `fenceClaim` (`lease_manager_tick.go`, failure branch around line 104) and `recoverFence` (`lease_manager.go`, failure branch around line 623) both call `countClaimRemoval(key, …, "fenced", sha)`. That logs `event=claim_released … result=fenced` while the claim stays held with `pendingJobDeletion`.

   a. In `pkg/claim/lease_manager.go`, add a helper next to `countClaimRemoval`:
   ```go
   // countClaimFencePending counts a fence whose Job deletion failed. The claim
   // stays held in pending Job deletion, so it logs the fence-pending line
   // instead of a release line. retryPendingDeletions logs the release line,
   // without counting again, once a later tick deletes the Job and the ref.
   func countClaimFencePending(key string, taskID string) {
   	countClaim("fenced")
   	glog.Warningf("event=claim_fence_pending key=%s task=%s", key, taskID)
   }
   ```
   The literal `claim_fence_pending` must appear exactly once in non-test Go code: in this `glog.Warningf` format string. Comments must not spell it; refer to it as "the fence-pending line".

   b. In `fenceClaim`, failure branch: replace `countClaimRemoval(key, held.taskID, "fenced", held.sha)` with `countClaimFencePending(key, held.taskID)`. Keep `countClaim("error")`, `held.pendingJobDeletion = true`, `held.pendingDeletionPath = "fence"` and the `return`. The `releasePending` branch and the success path keep their `countClaimRemoval(..., "fenced", ...)` calls unchanged.

   c. In `recoverFence`, inside `if err := m.jobControl.DeleteJob(ctx, ref.TaskID); err != nil { ... }`: replace `countClaimRemoval(key, ref.TaskID, "fenced", ref.Sha)` with `countClaimFencePending(key, ref.TaskID)`. Keep `countClaim("error")`, the `m.held[key] = &heldClaim{...}` assignment and the `return`. The final `countClaimRemoval(key, ref.TaskID, "fenced", ref.Sha)` after `bestEffortDelete` stays.

   d. In `retryPendingDeletions` (`lease_manager_tick.go`), inside `if held.pendingDeletionPath == "fence" { ... }`, after `m.bestEffortDelete(ctx, key, held.sha)`, add:
   ```go
   glog.Infof(
   	"event=claim_released key=%s task=%s result=fenced sha=%s",
   	key,
   	held.taskID,
   	held.sha,
   )
   ```
   - Do not call `countClaim` or `countClaimRemoval` here: the fence was already counted.
   - The trailing `sha=%s` keeps the line identical in shape to every other `event=claim_released` line documented in `docs/claim-lease-protocol.md`.
   - The `"lost"` path (`pendingDeletionPath == "lost"`) is unchanged and logs nothing new.

   e. Update the doc comment of `countClaimRemoval`. Replace the sentences "The pending Job-deletion retry does not: its fence was already counted and logged when the deletion was first attempted." with: "A fence whose Job deletion fails is counted by countClaimFencePending instead; retryPendingDeletions logs its release line later without counting again."

   f. **Test.** Add one spec to the existing `Describe("LeaseManager claim release log", ...)` in `pkg/claim/claim_released_log_test.go`. Add imports `stderrors "errors"` and `"strings"`.
   ```go
   It("logs claim_fence_pending, then one result=fenced line once the Job is deleted", func() {
   	key := "claims/task/log-fence-pending"
   	task := "t-log-fence-pending"
   	job.deleteFn = func(_ string, call int) error {
   		if call == 1 {
   			return stderrors.New("delete failed")
   		}
   		return nil
   	}
   	_, err := mgr.Claim(ctx, key, task)
   	Expect(err).NotTo(HaveOccurred())
   	before := snapshotClaims()

   	first := captureGlogStderr(func() {
   		clock.SetNow(leaseTestT0.Add(240 * libtime.Second))
   		Expect(mgr.Tick(ctx)).To(Succeed())
   	})
   	Expect(first).To(ContainSubstring("event=claim_fence_pending key=" + key + " task=" + task))
   	Expect(first).NotTo(ContainSubstring("claim_released"))
   	Expect(mgr.HeldClaims()).To(HaveLen(1))

   	second := captureGlogStderr(func() {
   		clock.SetNow(leaseTestT0.Add(250 * libtime.Second))
   		Expect(mgr.Tick(ctx)).To(Succeed())
   	})
   	Expect(strings.Count(
   		second,
   		"event=claim_released key="+key+" task="+task+" result=fenced",
   	)).To(Equal(1))
   	Expect(mgr.HeldClaims()).To(BeEmpty())
   	Expect(snapshotClaims()["fenced"] - before["fenced"]).To(Equal(1.0))
   })
   ```

   g. In `docs/claim-lease-protocol.md`, in the "Two log lines make claims observable at runtime" list:
   - change "Two log lines make claims observable at runtime:" to "These log lines make claims observable at runtime:";
   - append this bullet after the `event=claim_released` bullet: "`event=claim_fence_pending key=<key> task=<id>` — warning, logged when the fence cannot delete the claim's Job. `fenced` is counted then and the claim stays held; the `event=claim_released … result=fenced` line follows once a later tick deletes the Job and the ref, without counting again."

   Make no other doc changes.

3. **Wrap per-PR key validation errors (`pkg/claim/key.go`).**

   In `DeriveClaimKeyWithBody`, inside `if found { ... }` (lines 72–83), change each of the three `return "", false, err` statements to:
   ```go
   return "", false, errors.Wrapf(ctx, err, "derive pr claim key")
   ```
   - `errors` is already imported as `github.com/bborbe/errors`.
   - Do not touch `DeriveClaimKey`, the fallback branch or `validateClaimComponent`.
   - The existing `DescribeTable` in `pkg/claim/key_body_test.go` asserts errors only with `HaveOccurred()`, and its entries stay unchanged. If any other test you find compares the exact message of an error from this branch, change that assertion to `errors.Is` or substring matching (`ContainSubstring`) — except in the two files listed under constraints, which must not be edited.
   - Add one spec to `pkg/claim/key_body_test.go`, inside `Describe("DeriveClaimKeyWithBody", ...)`, after the `DescribeTable`:
   ```go
   It("wraps a per-PR validation error with its origin", func() {
   	_, _, err := claim.DeriveClaimKeyWithBody(
   		context.Background(),
   		"pr-review",
   		"abc123",
   		".hidden",
   		"agent",
   		"https://github.com/.hidden/agent/pull/1",
   	)
   	Expect(err).To(HaveOccurred())
   	Expect(err.Error()).To(ContainSubstring("derive pr claim key"))
   })
   ```
   The URL names the task's own repository, so the per-PR branch is taken, and owner `.hidden` fails `validateClaimComponent` on its leading dot.

4. **Logging cleanup in `countClaimRemoval` (`pkg/claim/lease_manager.go`, lines 160–174).**

   Replace the body with:
   ```go
   metrics.ClaimTotal.WithLabelValues(result).Inc()
   if result == "claim_lost" {
   	glog.Warningf(
   		"event=claim_released key=%s task=%s result=%s sha=%s",
   		key,
   		taskID,
   		result,
   		sha,
   	)
   	return
   }
   glog.Infof(
   	"event=claim_released key=%s task=%s result=%s sha=%s",
   	key,
   	taskID,
   	result,
   	sha,
   )
   ```
   `fmt.Sprintf` at line 162 is the only use of `fmt` in that file, so remove the `"fmt"` import. The printed text is byte-identical to today's.

5. **CHANGELOG (`CHANGELOG.md`, `## Unreleased`).**

   a. Extend the existing jobless bullet (line 8, starting `- fix: **a held claim is now released when its Job has been continuously absent for 300 s.**`) by appending one sentence at its end: "Each jobless pass starts no new check after 10 s, or as soon as the context is done; claims it did not reach are checked first on the following ticks."

   b. `## Unreleased` has no bullet covering the fence log line, so add one bullet directly after the jobless bullet: "- fix: **a fence whose Job deletion fails no longer logs a release.** It logs `event=claim_fence_pending` and counts `fenced` once; the `event=claim_released … result=fenced` line follows when a later tick deletes the Job and the ref."

   c. No CHANGELOG entry for fixes 3 and 4: they are internal.

6. **Coverage.** Run:
   ```
   go test -mod=mod -coverprofile=/tmp/claim.cov ./pkg/claim/
   go tool cover -func=/tmp/claim.cov
   ```
   Each of these functions must report ≥ 80%: `releaseJoblessClaims`, `joblessDueKeys`, `countClaimFencePending`, `retryPendingDeletions`, `fenceClaim`, `recoverFence`, `countClaimRemoval`, `DeriveClaimKeyWithBody`. If one is below, add a focused test for the uncovered branch. Do not put it in the files listed under constraints.

7. **Self-check.** Before finishing, run every command in `<verification>` and confirm each passes. Then walk the four fixes against the change:
   - (1) the budget and the cancelled context both stop the pass, and due claims are visited oldest-check-first, with tests;
   - (2) no `claim_released` line while the fence is pending, one line after the retry, `fenced` counted once;
   - (3) three wrapped returns, and a test that sees the wrap;
   - (4) no `fmt` import in `lease_manager.go`.
</requirements>

<constraints>
- No other behaviour change. Metric names and labels, claim keys, existing log text and the order of the `Tick` phases stay as they are.
- The jobless timing constants (`JoblessGrace = 300 * libtime.Second`, `JoblessCheckInterval = 60 * libtime.Second`) and every other lease constant (`LeaseDuration`, `TickInterval`, `RenewInterval`, `FenceMargin`, `FenceLead`, `TakeoverGrace`, `RequestTimeout`) stay unchanged. Do NOT make any of them, or `joblessPassBudget`, configurable.
- Do NOT add a budget to `recheckRunning` or `releaseExpiredHolds` in this prompt. They stay unbounded; bounding them is a separate follow-up.
- Measure time only through the injected `m.currentDateTime`. Never use `time.Now()` in `pkg/claim` production code.
- Do NOT edit `pkg/claim/key_test.go` or `pkg/handler/claim_owner_repo_internal_test.go`.
- Do NOT add specs to `pkg/claim/lease_manager_jobless_test.go`. Spec 013 AC 2 requires it to hold only the AC 1 spec.
- Do NOT wire `AuditMismatch` into production, and do not change the expired-lease takeover path, the concurrency cap, dispatch or respawn logic.
- Coverage of new and changed code must be at least 80% per function listed in requirement 6.
- Use `github.com/bborbe/errors` for wrapping (`errors.Wrapf(ctx, err, "...")`). Never use `fmt.Errorf`.
- Existing tests must still pass unchanged, in particular:
  - `"retries a failed Job deletion on the fence path"` in `lease_manager_tick_test.go`;
  - `"keeps a fence claim pending when its Job deletion fails"` in `lease_manager_restart_test.go`;
  - `"logs one line with result=fenced"` in `claim_released_log_test.go`.
- Do NOT commit — dark-factory handles git.
</constraints>

<verification>
Run `ROOTDIR=/workspace make test` — must pass.

Run `ROOTDIR=/workspace make precommit` — must pass.

`claim_fence_pending` appears exactly once in non-test Go code (expect `1`):
```
find /workspace/pkg /workspace/main.go -name '*.go' ! -name '*_test.go' -exec grep -o 'claim_fence_pending' {} + | wc -l
```

The new constant exists, and the fence paths no longer log a release on failure:
```
grep -n 'joblessPassBudget = 10 \* libtime.Second' /workspace/pkg/claim/lease_manager_tick.go
grep -n 'range m.joblessDueKeys(now)' /workspace/pkg/claim/lease_manager_tick.go
grep -c 'countClaimFencePending(' /workspace/pkg/claim/lease_manager_tick.go /workspace/pkg/claim/lease_manager.go
```
Expect `lease_manager_tick.go:1` and `lease_manager.go:2` (the declaration plus the `recoverFence` call).

No `fmt` import left in the lease manager (no output):
```
! grep -n '"fmt"' /workspace/pkg/claim/lease_manager.go
```

Three wrapped returns in the per-PR branch (expect `3`):
```
grep -c 'errors.Wrapf(ctx, err, "derive pr claim key")' /workspace/pkg/claim/key.go
```

The jobless spec file still holds only its one spec (expect `1`):
```
grep -c 'It(' /workspace/pkg/claim/lease_manager_jobless_test.go
```

CHANGELOG placement (every line must print `sits under: ## Unreleased`):
```
awk '/^## /{sec=$0} /claim_fence_pending|Each jobless pass starts no new check/{print "sits under: " sec}' /workspace/CHANGELOG.md
```

Coverage per function, each ≥ 80%:
```
cd /workspace && go test -mod=mod -coverprofile=/tmp/claim.cov ./pkg/claim/ && go tool cover -func=/tmp/claim.cov | grep -E 'releaseJoblessClaims|joblessDueKeys|countClaimFencePending|retryPendingDeletions|fenceClaim|recoverFence|countClaimRemoval|DeriveClaimKeyWithBody'
```
</verification>
