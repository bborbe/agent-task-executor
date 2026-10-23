---
status: draft
spec: [015-dual-acquire-claim-keys]
created: "2026-10-23T19:40:00Z"
branch: dark-factory/dual-acquire-claim-keys
---

# Dual claim keys: hold and release the pair as one unit

<summary>
- Every release path now removes both refs of a dual claim, not just the per-repo one: plain release, success hold, fence, jobless release, release-after-running-check and takeover.
- A release that removes one ref and fails on the other leaves the whole claim pending-release, so the next tick retries the ref that is still there instead of silently half-releasing the pair.
- A lost claim drops both refs from the held set at once and deletes the Job, without contacting the ref store.
- A renew tick renews both refs in one pass with a single lease deadline, so the two leases never drift apart by more than one renew interval.
- A fence deletes both refs and drops the pair together.
- A claim is never left held with only one of its two refs.
- The metric result set is unchanged, and each tick counts exactly one renew outcome for a claim, not one per ref.
</summary>

<objective>
Make every path that renews, fences, releases or loses a held claim act on both refs of a dual claim as a single unit, so a mixed fleet can never see one ref still leased by this cluster while the other is gone. End state: all seven release paths delete both refs (or drop both when the claim is lost), a partial delete leaves the claim pending-release and retried, and one renew tick advances both refs to the same `lease_until`.
</objective>

<context>
Read these files fully before editing:

- `pkg/claim/lease_manager.go` — `heldClaim` (prompt 1 added `secondKey`/`secondSha`), `Release`, `ReleaseAfterSuccess`, `releaseLocked`, `findHeld`, `countClaimRemoval`, `countClaimFencePending`, `bestEffortDelete`, `requestContext`.
- `pkg/claim/lease_manager_tick.go` — `Tick`'s phase order, `retryPendingDeletions`, `evaluateFence`, `fenceClaim`, `retryPendingReleases`, `releaseExpiredHolds`, `releaseJoblessClaim`, `recheckRunning`, `releaseAfterRunningCheck`, `renewDueClaims`, `renewClaim`, `claimLost`, `renewReadBack`.
- `pkg/claim/lease_manager_test_helpers_test.go` — `newTestManager`, `newTestClock`, `advance`, `tickAt`, `expectClaimDelta`, `snapshotClaims`, `requestsMatching`, `newFakeJobControl`, `leaseTestT0`, `rfc3339`.
- `pkg/claim/fake_git_rest_test.go` — `SetFault`, `Seed`, `Ref`, `Requests`, `ResetRequests`, `fakeRequest`, `fakeFault`, `fakePutBody`.
- `pkg/claim/lease_manager_release_test.go`, `lease_manager_tick_test.go`, `lease_manager_restart_test.go`, `success_grace_internal_test.go` — the existing single-ref expectations that must keep passing.
- `docs/claim-lease-protocol.md` — the Claim state machine, Fence and pending-release sections. Do not edit it in this prompt; prompt 4 documents the pair.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`.

Prompt 1 (executed before this one) made acquisition write both refs and made `HeldClaims()` return two rows for a pair, but deliberately left `Release` and the tick phases single-ref. This prompt closes that gap. The spec is `specs/in-progress/015-dual-acquire-claim-keys.md`; this prompt covers Desired Behaviours 4 and 5 and Acceptance Criteria AC4, AC5 and AC6.
</context>

<requirements>
1. In `Release` and `ReleaseAfterSuccess` (`pkg/claim/lease_manager.go`), resolve the held claim with `m.findHeld(key)` instead of `m.held[key]`, so a caller passing either the primary or the secondary key finds the pair. Keep the existing success-hold precedence and logging unchanged. `releaseLocked` must always use `held.key` and `held.sha` for the primary, regardless of which key the caller passed. Drop the now-redundant `key` parameter from `releaseLocked` (make it `releaseLocked(ctx context.Context, held *heldClaim) error`) and update its three call sites — `Release`, `ReleaseAfterSuccess` and `releaseExpiredHolds` — so no unread parameter remains.

2. Rewrite `releaseLocked` to delete both refs as one unit:
   1. Keep the `IsJobRunning` check and its `release_deferred_job_running` early return exactly as today.
   2. `DELETE` the primary (`held.key`, `held.sha`). A non-`ErrLeaseConflict` error: set `held.releasePending = true`, `countClaim("error")`, return the error.
   3. When `held.secondKey == ""`, delete the held entry and call `countClaimRemoval(held.key, held.taskID, result, held.sha)` with `result` `"already_released"` when the primary delete returned `ErrLeaseConflict` and `"released"` otherwise. Return nil.
   4. `DELETE` the secondary (`held.secondKey`, `held.secondSha`). A non-`ErrLeaseConflict` error: set `held.releasePending = true`, `countClaim("error")`, return the error. The primary ref is already gone at this point; the pair stays held so the retry pass can finish the secondary.
   5. Delete the held entry and call `countClaimRemoval` once with `"already_released"` only when **both** deletes returned `ErrLeaseConflict`, otherwise `"released"`. Do not count one removal per ref.

3. Rewrite `retryPendingReleases` (`pkg/claim/lease_manager_tick.go`) for the pair: for each held claim with `releasePending`, delete the primary and then the secondary. A `409` on either means that ref is already gone and the pass continues; a non-`409` error counts `"error"` and leaves the claim held for the next tick. Delete the held entry and call `countClaimRemoval` once only when both refs are gone.

4. Rewrite `fenceClaim` for the pair:
   - The `releasePending` branch best-effort deletes both refs, drops the pair and calls `countClaimRemoval(..., "fenced", held.sha)` once.
   - The normal branch deletes the Job once (unchanged failure handling and `pendingJobDeletion` state), then best-effort deletes both refs, drops the pair and calls `countClaimRemoval(..., "fenced", held.sha)` once.
   - Do not call `DeleteJob` twice for a pair.

5. Rewrite the fence branch of `retryPendingDeletions`: after `DeleteJob` succeeds for a claim whose `pendingDeletionPath == "fence"`, best-effort delete both refs, log the existing `event=claim_released key=%s task=%s result=fenced sha=%s` line once with the primary key and sha, and drop the pair.

6. Rewrite `releaseAfterRunningCheck` for the pair: delete the primary then the secondary. A `409` on either is treated as gone; a non-`409` error sets `held.releasePending = true` and counts `"error"` (returning without dropping the claim). Drop the pair and call `countClaimRemoval` once only when both refs are gone.

7. `claimLost` keeps its semantics for a pair: count `"claim_lost"` once with the primary sha, delete the Job once, drop the pair, and send **no** ref `DELETE`. If the Job deletion fails, keep the pair in `pendingJobDeletion` with `pendingDeletionPath = "lost"` exactly as today.

8. Rewrite `renewClaim` so one tick renews both refs with a single deadline and counts exactly one renew outcome for the claim:
   1. Compute `newLease := now.Add(LeaseDuration)` and `newLeaseStr` once. Do not re-read the clock between the two writes. Set `held.lastRenewAt = now` and `held.renewNextTick = false` once at the start.
   2. Start with `result := "renewed"`.
   3. `PUT` the primary with `held.sha`.
      - Success: adopt the returned sha.
      - `ErrLeaseConflict`: `claimLost` (which counts `"claim_lost"`) and return.
      - Other error: `sha, readResult, ok := m.renewReadBack(ctx, held, newLeaseStr)`. `!ok` means the claim was lost (already counted) — return. Otherwise adopt `sha` and set `result = readResult`.
   4. When `held.secondKey == ""`: set `held.leaseUntil`/`held.leaseUntilStr` to the new lease only when `result` is `"renewed"` or `"recovered"`. When `result` is `"renew_failed"`, leave both at their previous value — today's `renewReadBack` behaves this way, and a failed renew must not advance the expiry the fence reads. Then `countClaim(result)` once and return.
   5. When a secondary exists, `PUT` it with `held.secondSha` and the same `newLease`.
      - Success: adopt the sha. Set `held.leaseUntil`/`held.leaseUntilStr` to the new lease only when the primary renew also succeeded (`result` is `"renewed"` or `"recovered"`); when the primary renew failed (`result` is `"renew_failed"`), leave both at their previous value so the pair's effective expiry stays at the older lease. `countClaim(result)` once. Return.
      - `ErrLeaseConflict`: `claimLost` and return.
      - Other error: set `held.renewNextTick = true`, `countClaim("renew_failed")`, and leave `held.leaseUntil` at its previous value, so the effective expiry of the pair is the older of the two leases. Return.

9. Change `renewReadBack` to this signature. It must no longer mutate `leaseUntil`/`leaseUntilStr` and must no longer call `countClaim`; `renewClaim` owns the single count:

   ```go
   // renewReadBack decides a failed primary renew from the listed ref, after
   // re-running the fence. It returns the sha to adopt, the result label the
   // caller must count, and whether the claim is still held. It never updates
   // leaseUntil and never counts.
   func (m *leaseManager) renewReadBack(
       ctx context.Context,
       held *heldClaim,
       newLeaseStr string,
   ) (string, string, bool)
   ```

   Branches: after re-running `evaluateFence`, a claim no longer in the map returns `("", "", false)`; a read-back error returns `(held.sha, "renew_failed", true)`; an own triple returns `(ref.Sha, "recovered", true)`; a listing still at `held.sha` returns `(held.sha, "renew_failed", true)`; anything else calls `claimLost` and returns `("", "", false)`. The existing single-ref recovery tests must keep their deltas: a failed renew that reads back its own write still counts `"recovered"` once, and a read-back error still counts `"renew_failed"` once.

10. Tests in `pkg/claim/dual_release_test.go` (package `claim_test`, Ginkgo). Wrap the specs in `Describe("Dual claim release paths", ...)` and name the AC4 table exactly `"every release path removes both refs"` — the verification below focuses on both strings. Use a helper that claims the pair `claims/repo/bborbe-nuke` + `claims/pr/bborbe-nuke-658` for task `t-dual` and returns the manager plus both held shas. Assert `DELETE` requests with `requestsMatching(fake.Requests(), http.MethodDelete, ...)` and read `expected_sha` from `RawQuery` (`ContainSubstring("expected_sha="+sha)`).
    - **AC4**: a `DescribeTable` named `"every release path removes both refs"` with one `Entry` per path. Each row drives the path on a freshly claimed pair and asserts the two `DELETE`s carry the two held shas. Rows:
      1. `plain Release` — `mgr.Release(ctx, primary)`; two `DELETE`s, held empty, one `"released"` count.
      2. `ReleaseAfterSuccess` — `mgr.ReleaseAfterSuccess(ctx, primary, clock.Now())`, then `advance(clock, libtime.Duration(claim.SuccessGracePeriod))` and `Tick`; two `DELETE`s, held empty.
      3. `fenced` — `fake.SetFault` answering 500 to every `PUT`, `clock.SetNow(leaseTestT0.Add(240*libtime.Second))`, `Tick`; two `DELETE`s, held empty, one `"fenced"` count.
      4. `jobless release` — `job.SetRunning(map[string]bool{})`, advance past `claim.JoblessGrace` with `tickAt`, `Tick`; two `DELETE`s, held empty.
      5. `release-after-running-check` — seed both refs with `cluster="self"` and a running check that errors once and then answers `false`; `Recover`, then one `Tick`; two `DELETE`s, held empty.
      6. `takeover` — seed an expired pair owned by `"other"`, set the clock to `leaseTestT0.Add(claim.TakeoverGrace)`, `TakeoverPair` the pair (two `PUT`s), then `Release`; two `DELETE`s carrying the two new shas, held empty.
      7. `claim-lost` — claim the pair, `fake.Seed` a competing primary ref owned by another cluster, advance `claim.RenewInterval`, `Tick`; held empty and **zero** ref `DELETE`s (the Job is deleted once).
    - **AC5**: after claiming the pair, `fake.ResetRequests()` so the claim's own `PUT`s are not counted, then `advance(clock, claim.RenewInterval)`, `Tick`; assert exactly two `PUT`s whose `expected_sha` are the two held shas and whose decoded `lease_until` values are equal, and assert the `"renewed"` delta is exactly 1. Then set every `PUT` to 500, advance to `leaseTestT0.Add(240*libtime.Second)`, `Tick`; assert two `DELETE`s and `HeldClaims()` empty.
    - **AC5 second case**: make the primary `PUT` fail with a non-409 while the secondary `PUT` succeeds, `Tick`, and assert `HeldClaims()` still reports the **older** `LeaseUntil` for the pair — a failed renew must not advance the expiry the fence reads.
    - **AC6**: claim the pair; `fake.SetFault` returning `&fakeFault{Status: http.StatusInternalServerError}` for `DELETE` requests whose `Path` contains `claims/pr/`; `Release(ctx, primary)` returns an error; `HeldClaims()` still has two rows (both keys present). Clear the fault, advance one tick, `Tick`; assert the fake records a `DELETE` for `claims/pr/bborbe-nuke-658` and the held set is empty. The primary `DELETE` on that retry answers `409` and must not drop the pair before the secondary is retried.

11. Add an HTML comment at the top of the AC4 table explaining the two rows that deviate from the spec's blanket "two DELETEs" sentence: the takeover row deletes both after the takeover succeeds, and the claim-lost row deletes none because a lost claim is dropped without contacting git-rest. Do not silently assert two `DELETE`s for claim-lost.
</requirements>

<constraints>
- The constants in `docs/claim-lease-protocol.md` stay invariants. Do not add a knob, flag or opt-out for lease length, tick, fence, takeover or jobless grace.
- The claim ref API is unchanged: `PUT`/`DELETE /api/v1/refs/claims/<key>` with `expected_sha`, `409` on lease mismatch, never retried.
- Keys and task ids remain forbidden as metric label values. Do not add a metric series.
- A claim is not held while only one of its refs is held; a release that removes one ref but not the other leaves the claim pending-release.
- Both refs share one effective expiry; a partial renew leaves the effective expiry at the older value.
- Never use `fmt.Errorf`; wrap with `errors.Wrapf(ctx, err, ...)` from `github.com/bborbe/errors`.
- Do NOT edit the spec file. Do NOT commit — dark-factory handles git.
- Existing tests must still pass, including every single-ref release, fence, jobless, takeover and recovery expectation.
</constraints>

<verification>
Run from the repository root:

```bash
set -o pipefail
go test ./pkg/claim/ -race -v -ginkgo.v -ginkgo.focus='Dual claim release paths' 2>&1 | tee /tmp/dual-release.log
grep -q 'every release path removes both refs' /tmp/dual-release.log
```

The `grep` proves the table was selected and ran rather than the run exiting 0 with zero specs. Then the full package suite and the final gate:

```bash
GOFLAGS=-buildvcs=false go test ./pkg/claim/ -race
GOFLAGS=-buildvcs=false make precommit VERSION=dev
```

`make precommit` must exit 0.

Before you finish, re-run the commands above and confirm every one passes. Walk AC4, AC5 and AC6 against the code and the tests, and confirm the single-ref expectations in `lease_manager_release_test.go`, `lease_manager_tick_test.go` and `lease_manager_recovery_test.go` still hold.
</verification>
