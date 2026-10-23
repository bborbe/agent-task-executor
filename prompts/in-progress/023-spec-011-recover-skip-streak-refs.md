---
status: approved
spec: [011-bug-redrive-streak-lost-across-clusters-and-restarts]
created: "2026-10-23T09:27:00Z"
queued: "2026-10-23T09:42:55Z"
branch: dark-factory/bug-redrive-streak-lost-across-clusters-and-restarts
---

<summary>
- Startup recovery stops treating redrive-streak refs as claims, so a restart can never fence, adopt, renew or release one.
- A streak ref is skipped by key prefix before the cluster comparison, so even an executor configured with the sentinel cluster name leaves it alone.
- Without this, the very first restart after the streak store lands could delete a task's live Job by fencing its streak ref.
- The claim-lease protocol document gains a dedicated section describing streak refs: their key shape, the sentinel cluster, the informational 24 h lease, and that no lease-manager path touches them.
- The Restart recovery section gains the explicit skip rule.
- The document also records how an operator un-caps a task by deleting its streak refs through git-rest.
- One new spec seeds a streak ref whose cluster equals the manager's own and whose lease is inside the fence window, runs recovery, and proves nothing happened.
</summary>

<objective>
Make `leaseManager.Recover` skip every key under `claims/redrive/` regardless of its cluster field, and document streak refs plus the operator un-cap command in `docs/claim-lease-protocol.md`. This must land before the handler starts writing streak refs (prompt 3), or the first restart could fence a task's live Job.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for the container agent's execution rules (Ginkgo/Gomega v2, external test packages, coverage ≥80% for new code, BSD license header). This repo has no project-level `CLAUDE.md`.

Read these files fully before changing anything:
- `/workspace/pkg/claim/lease_manager.go` — `Recover` lists `claims/` and, for each entry whose `Cluster` equals `m.cluster`, calls `recoverOne`, which fences (deletes the task's Job) when the lease is inside the fence window or adopts/releases it otherwise. `recoverFence` calls `m.jobControl.IsJobRunning` / `DeleteJob`. This is the only production call site of `List(ctx, "claims/")`.
- `/workspace/pkg/claim/recover_startup.go` — `RecoverAtStartup` and `RunClaimRecovery`; no change here, but the doc comment explains why recovery runs on every boot.
- `/workspace/pkg/claim/lease_manager_restart_test.go` — the existing restart-recovery specs, `seedRestartFixture`, `setupRestartJob`, and `requestsMatching(fake.Requests(), http.MethodPut, "")`.
- `/workspace/pkg/claim/lease_manager_test_helpers_test.go` — `newTestManager(cluster, client, job, clock)`, `newTestClock`, `rfc3339`, `fakeJobControl.DeleteCallCount(taskID)`.
- `/workspace/pkg/claim/fake_git_rest_test.go` — `newFakeGitRest`, `Seed(ref, sha, cluster, taskID, leaseUntil)` takes a FULL ref (`refs/claims/...`), `Ref(ref)`, `Requests()`.
- `/workspace/docs/claim-lease-protocol.md` — the whole file. Note the `## Constants` table, `## Restart recovery` bullets, and `## Lost-response recovery`.

Facts already verified, do not re-derive:
- `strings` is already imported in `lease_manager.go`.
- `RedriveStreakPrefix` is added by prompt 1 (`claims/redrive/`); this prompt depends on it and must not compile until prompt 1 has landed. Fail loud in the completion report rather than duplicating the constant.
- A lease is inside the fence window when `now > lease_until − 70 s` (`fenceAt = lease_until − 60 s`, fires at `fenceAt − 10 s`). With `leaseTestT0` as `now`, seeding `lease_until = rfc3339(leaseTestT0.Add(30*libtime.Second))` is inside the window.
- `fakeJobControl.DeleteCallCount(taskID string) int` and `Deleted() []deleteCall` exist.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2, external `_test` package.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-doc-best-practices.md` — comments describe behaviour.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-precommit.md` — golines 100-char limit, license headers.
</context>

<requirements>
1. In `/workspace/pkg/claim/lease_manager.go`, change `Recover`'s loop so a streak ref is skipped before any other decision. After the `List` error check and before the `ref.Cluster != m.cluster` comparison, add:
   ```go
   if strings.HasPrefix(strings.TrimPrefix(ref.Ref, "refs/"), RedriveStreakPrefix) {
       continue
   }
   ```
   The skip must be by key prefix, not by the cluster field: an executor started with `CLUSTER=redrive-streak` must still skip. Do not change `recoverOne`, `recoverFence`, `recoverAdopt`, `Claim`, `Takeover`, `Release`, `ReleaseAfterSuccess`, `Tick`, `AuditMismatch`, or any lease constant. No other lease-manager path lists or holds a streak ref, so no further guard is needed.

2. Add one spec to `/workspace/pkg/claim/lease_manager_restart_test.go` (same `Describe("LeaseManager restart recovery")` block) or to a new `pkg/claim/redrive_streak_recovery_test.go` in `package claim_test`. It must:
   - Seed `refs/claims/redrive/t-streak/ai_review/1` with sha `"S-STREAK"`, cluster `"self"` (the manager's own cluster), task id `"t-streak"`, and `rfc3339(leaseTestT0.Add(30*libtime.Second))` — inside the fence window.
   - Seed one more streak ref with cluster `"other"` to prove the skip is not cluster-based.
   - Capture `before := snapshotClaims()`, then `Expect(mgr.Recover(ctx)).To(Succeed())`.
   - Assert `requestsMatching(fake.Requests(), http.MethodDelete, "")` is empty.
   - Before `Recover`, set `job.runningFn` (same test package) to count its calls and return `true`, so an unskipped fence would call `IsJobRunning` and `DeleteJob`. Assert that counter is 0, `job.DeleteCallCount("t-streak") == 0`, `job.Deleted()` is empty, `requestsMatching(fake.Requests(), http.MethodPut, "")` is empty, and `mgr.HeldClaims()` is empty.
   - Assert `fake.Ref("refs/claims/redrive/t-streak/ai_review/1")` still exists with its original sha.
   - Assert `expectClaimDelta(before, snapshotClaims(), map[string]float64{})` — recovery must not count a claim result for a skipped ref.
   - Keep the fixture free of ordinary claims: the existing restart-recovery specs already prove that normal claims are still adopted, released or fenced, and mixing them here would make the empty-delta assertion false for an unrelated reason.

3. Update `/workspace/docs/claim-lease-protocol.md`:
   - In `## Restart recovery`, add a bullet stating that keys under `claims/redrive/` are skipped before the cluster comparison, so a streak ref is never adopted, renewed, fenced or released regardless of its `cluster` value.
   - Add a new section named exactly `## Streak refs` (place it after `## Restart recovery`). It must document:
     - the key shape `claims/redrive/<taskid>/<phase>/<n>` with `n` an integer ≥ 1;
     - the sentinel cluster field `redrive-streak`;
     - `lease_until` = create time + 24 h, informational only — nothing renews or expires it;
     - that no lease-manager path (recovery, renew, fence, takeover, release, audit) acts on a streak ref, and that a streak ref therefore never triggers a fence;
     - that a task with no phase uses the phase component `none`;
     - that the streak counts consecutive spawns at one phase and resets when a spawn is recorded at a new phase, which deletes the old phase's refs;
     - the operator un-cap command: list `GET /api/v1/refs?prefix=claims/redrive/<taskid>` and delete each entry with `DELETE /api/v1/refs/claims/redrive/<taskid>/<phase>/<n>?expected_sha=<sha>`. Include the literal string `DELETE /api/v1/refs/`.
   - Do not rewrite the dispatch-claim sections or the constants table.

4. Do not touch `pkg/handler`, `pkg/factory`, `main.go`, or `CHANGELOG.md` in this prompt.

5. Before finishing, re-run every `<verification>` command and confirm each passes; walk each acceptance criterion listed above against the change.
</requirements>

<constraints>
- Claim protocol invariants in `docs/claim-lease-protocol.md` are unchanged for dispatch claims; the doc gains a streak-refs section and a Restart-recovery bullet only.
- Do NOT change the dispatch claim keys (`claims/task/...`, `claims/repo/...`), the lease constants, or the fence/takeover behaviour for those keys.
- The skip must be by key prefix before the cluster comparison; relying on the `redrive-streak` sentinel alone would fence streak refs on an executor whose `CLUSTER` is `redrive-streak`.
- Repo conventions: Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, license headers, golines 100-char limit.
- New code needs ≥80% statement coverage.
- Do NOT commit — dark-factory handles git.
- All existing tests in `pkg/claim` must still pass without weakened assertions.
</constraints>

<verification>
Run `go test ./pkg/claim/...` — must exit 0.

Confirm the documentation:
`awk '/^## /{sec=$0} /claims\/redrive\//{print sec}' docs/claim-lease-protocol.md | grep -qx '## Restart recovery'` — must exit 0
`awk '/^## /{sec=$0} /claims\/redrive\//{print sec}' docs/claim-lease-protocol.md | grep -qx '## Streak refs'` — must exit 0
`grep -q 'DELETE /api/v1/refs/' docs/claim-lease-protocol.md` — must exit 0

Run `ROOTDIR=/workspace make precommit` once at the end — must exit 0.
</verification>
