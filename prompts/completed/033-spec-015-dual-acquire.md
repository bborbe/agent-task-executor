---
status: completed
spec: [015-dual-acquire-claim-keys]
summary: Added dual claim-key derivation and pair acquisition (ClaimPair/TakeoverPair) so mixed-scheme clusters cannot spawn the same repo-keyed task twice, with handler wiring, tests, and changelog.
execution_id: agent-task-executor-dual-acquire-exec-033-spec-015-dual-acquire
dark-factory-version: v0.198.7
created: "2026-10-23T19:25:00Z"
queued: "2026-10-23T19:51:10Z"
started: "2026-10-23T19:51:12Z"
completed: "2026-10-23T20:08:40Z"
branch: dark-factory/dual-acquire-claim-keys
---

# Dual claim keys: derive the pair and acquire both refs

<summary>
- A claim key derivation now returns a primary and an optional secondary key: the per-repo key is always primary, and a `pr-review` task with a usable pull-request URL also gets the per-PR key as secondary.
- Acquiring a repo-keyed claim writes both refs, per-repo first; the task spawns only when both writes succeed.
- If the secondary write conflicts, the primary is released again and the task is treated as claimed by another cluster, so no Job is created.
- A task whose two derivations are equal — every task-id-keyed type and a `pr-review` task with no usable PR URL — still writes exactly one ref, so its behaviour is unchanged.
- Two clusters running different key schemes can no longer both acquire the same task: whichever scheme runs second gets a lease conflict on the ref the first one wrote.
- During the rollout window, reviews of different pull requests in one repository serialize again on the shared per-repo key; this is an accepted consequence of the dual write and ends when the dual write is removed in a later release.
- Takeover of an expired claim now takes both refs or neither, releasing a primary it just took if the secondary cannot be taken.
- Successful dual acquires log one info line naming both keys; a secondary conflict logs one warning line.
</summary>

<objective>
Make a claim for a repo-keyed task own both the per-repo ref and, when the body qualifies, the per-PR ref, so a mixed fleet where one cluster derives `claims/repo/<owner>-<repo>` and another derives `claims/pr/<owner>-<repo>-<n>` can never spawn the same task twice. End state: `ClaimPair` writes the primary then the secondary and succeeds only if both are held; a secondary conflict releases the primary and returns `ErrLeaseConflict`; tasks whose two derivations are equal take the existing single-ref path byte-for-byte.
</objective>

<context>
Read these files fully before editing:

- `pkg/claim/key.go` — current derivation. `DeriveClaimKey` (task-id or per-repo) and `DeriveClaimKeyWithBody` (per-PR with a per-repo fallback) are the functions you change. `pullRequestNumberFromBody`, `pullRequestNumberFromLine`, `validateClaimComponent` and `isSafeClaimRune` stay as they are.
- `pkg/claim/lease_manager.go` — `LeaseManager` interface, `heldClaim` struct, `Claim`, `claimLocked`, `storeClaim`, `readBack`, `ownTriple`, `Takeover`, `takeoverExpired`, `HeldClaims`. `countClaim` / `countClaimRemoval` are the only metric writers; the result label set is closed at 14 series (see `claimResults` in `pkg/claim/lease_manager_test_helpers_test.go`) and MUST NOT grow.
- `pkg/claim/lease_manager_tick.go` — read only, so you understand which fields of `heldClaim` the tick phases consume. Do not change tick behaviour in this prompt.
- `pkg/handler/claim_wiring.go` — `acquireSpawnClaim` is the only production caller of derivation, `Claim` and `Takeover`.
- `pkg/claim/lease_manager_test_helpers_test.go` and `pkg/claim/fake_git_rest_test.go` — the fake records every request in arrival order in `Requests()`; `fakeRequest` has `Method`, `Path`, `RawQuery`, `Body`, `Status`; `fakePutBody` has `ExpectedSha`, `Cluster`, `TaskID`, `LeaseUntil`; `SetFault(func(fakeRequest) *fakeFault)` injects one-off answers.
- `pkg/claim/claim_suite_test.go` — the package's Ginkgo entry point is `TestClaim`. A plain Go test named `TestDualAcquireKeys` is selected by `-run TestDualAcquireKeys`; a Ginkgo `It` is not.
- `pkg/claim/key_body_test.go` and `pkg/handler/claim_key_body_internal_test.go` — the derivation and handler tests you must update.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md`, `go-testing-guide.md`, `go-error-wrapping-guide.md` — project conventions for constructors, error wrapping (`errors.Wrapf` from `github.com/bborbe/errors`, never `fmt.Errorf`) and tests.

The spec is `specs/in-progress/015-dual-acquire-claim-keys.md`. This prompt covers its Desired Behaviours 1, 2, 3 and 7, and Acceptance Criteria AC1, AC2, AC3 and AC8.
</context>

<requirements>
1. In `pkg/claim/key.go`, replace `DeriveClaimKeyWithBody` with this function and update every caller (there are exactly two: `pkg/handler/claim_wiring.go` and `pkg/claim/key_body_test.go`):

   ```go
   // DeriveClaimKeysWithBody maps a task to the claim refs it uses. The first
   // return value is the primary key and is always written first. The second is
   // the secondary key and is empty when the two derivations are equal. The
   // third reports that a pr-review task fell back to the repository key; it is
   // meaningful only when the error is nil.
   func DeriveClaimKeysWithBody(
       ctx context.Context,
       taskType string,
       taskID string,
       owner string,
       repo string,
       body string,
   ) (string, string, bool, error)
   ```

   Behaviour, in this order:
   - `taskType != "pr-review"`: `primary, err := DeriveClaimKey(ctx, taskType, taskID, owner, repo)`; return `primary, "", false, err`.
   - `taskType == "pr-review"` and `pullRequestNumberFromBody(owner, repo, body)` finds a number: validate owner, repo and the number exactly as the current function does (wrapping with `"derive pr claim key"`), then return `"claims/repo/"+owner+"-"+repo`, `"claims/pr/"+owner+"-"+repo+"-"+number`, `false`, `nil`.
   - `taskType == "pr-review"` and no number: `primary, err := DeriveClaimKey(ctx, taskType, taskID, owner, repo)`; return `primary, "", true, err`.
   - Owner and repo are lower-cased before use, exactly as today.

2. In `pkg/claim/lease_manager.go`, add the pair to the held state without disturbing the existing single-ref fields:

   ```go
   type heldClaim struct {
       key           string
       taskID        string
       sha           string
       leaseUntil    libtime.DateTime
       leaseUntilStr string
       // secondKey and secondSha are the secondary ref of a dual claim. They are
       // empty when the derivation produced one key. The pair is one claim: a
       // release, fence or lost path acts on both refs together.
       secondKey string
       secondSha string
       // ... keep every existing field and comment unchanged ...
   }
   ```

3. Add a lookup helper on `*leaseManager` and use it wherever a held key is resolved:

   ```go
   // findHeld returns the held claim that owns key, whether key is its primary
   // or its secondary.
   func (m *leaseManager) findHeld(key string) (*heldClaim, bool)
   ```

   It checks `m.held[key]` first, then scans `m.held` for `held.secondKey == key`. The caller must hold `m.mu`.

4. Change `HeldClaims()` so a dual claim yields **two** `HeldClaim` rows: the primary row (`Key` = `held.key`, `Sha` = `held.sha`) and, when `held.secondKey != ""`, a second row with the secondary key and sha, the same `TaskID` and the same `LeaseUntil`. Sort the result by `Key` as today. Existing single-ref claims yield exactly one row, unchanged. Do not add fields to the exported `HeldClaim` struct.

5. Change `storeClaim` to record the pair and return the primary view:

   ```go
   func (m *leaseManager) storeClaim(
       primaryKey string,
       secondaryKey string,
       taskID string,
       primarySha string,
       secondarySha string,
       leaseUntil libtime.DateTime,
       leaseUntilStr string,
       now libtime.DateTime,
   ) *HeldClaim
   ```

   It sets `key`/`sha`/`secondKey`/`secondSha` and the existing fields, and returns `&HeldClaim{Key: primaryKey, TaskID: taskID, Sha: primarySha, LeaseUntil: leaseUntil}`. Rename `claimLocked` to `claimPairLocked` (requirement 7) and update its callers: `Claim` delegates to `ClaimPair`, and the absent-key branch of `TakeoverPair` calls `claimPairLocked(ctx, primary, secondary, taskID)`. Update the existing `storeClaim` calls in `claimPairLocked` and `takeoverExpired` to pass `""` for the secondary.

6. Add `ClaimPair` to the `LeaseManager` interface. `Claim`'s signature and single-key semantics stay exactly as they are; its locking moves into `ClaimPair`:

   ```go
   // ClaimPair creates both refs of a claim. The primary is written first; the
   // secondary is written only when it is non-empty. The claim is held only when
   // every write succeeded, or when a failed primary write was read back as this
   // manager's own. A secondary conflict releases the primary and returns
   // ErrLeaseConflict.
   ClaimPair(ctx context.Context, primary string, secondary string, taskID string) (*HeldClaim, error)
   ```

   `Claim(ctx, key, taskID)` drops its own `m.mu` lock and becomes exactly
   `func (m *leaseManager) Claim(ctx context.Context, key, taskID string) (*HeldClaim, error) { return m.ClaimPair(ctx, key, "", taskID) }`.
   `ClaimPair` takes `m.mu` exactly once, as `TakeoverPair` does in requirement 9. Do not keep
   the lock in `Claim` and then call `ClaimPair` — `sync.Mutex` is not reentrant and that deadlocks.

7. Rename `claimLocked` to `claimPairLocked(ctx, primary, secondary, taskID)` and extend it as `Claim`'s body today, with these additions. Exactly one `countClaim` call happens per `ClaimPair` call, at the end, with this result:
   1. If `findHeld(primary)` or (`secondary != ""` and `findHeld(secondary)`) reports a held claim, `countClaim("conflict")`, return `ErrLeaseConflict` without any request.
   2. Read `now := m.currentDateTime.Now()`, `leaseUntil := now.Add(LeaseDuration)`, `leaseUntilStr := formatLeaseUntil(leaseUntil)` once for the whole pair.
   3. `primarySha, err := m.refClient.Put(ctx, primary, "", m.cluster, taskID, leaseUntil)`.
      - `err == nil`: continue to step 4 with `primarySha`.
      - `errors.Is(err, ErrLeaseConflict)`: `countClaim("conflict")`, return `err`.
      - any other error: read back `primary` with `m.readBack`. A read-back error: `countClaim("error")` and return the wrapped read-back error (same message as today). `m.ownTriple(ref, taskID, leaseUntilStr)` true: continue to step 4 with `ref.Sha` and remember that the primary was recovered. Otherwise: `countClaim("error")` and return the original `err`.
   4. If `secondary == ""`: store the single claim, `countClaim("claimed")` (or `countClaim("recovered")` when the primary was read back), return its view.
   5. `secondarySha, err := m.refClient.Put(ctx, secondary, "", m.cluster, taskID, leaseUntil)`.
      - `err == nil`: store the pair, log the dual-acquire line from requirement 8, `countClaim("claimed")` (or `"recovered"`), return the primary view.
      - `errors.Is(err, ErrLeaseConflict)`: read `secondary` back with `listed, readErr := m.readBack(ctx, secondary)`. When `readErr == nil` and the listed ref exists, its `lease_until` parses, and it is at or before `now.Add(-TakeoverGrace)` (the same expiry rule `Takeover` applies), displace it with one compare-and-swap `m.refClient.Put(ctx, secondary, listed.Sha, m.cluster, taskID, leaseUntil)`; on success store the pair, log the dual-acquire line from requirement 8, `countClaim("taken_over")`, return the primary view; on `ErrLeaseConflict` fall through to the release below. Otherwise (no ref, unparseable lease, or a live lease) best-effort release the primary with `m.refClient.Delete(ctx, primary, primarySha)` (ignore its error), log the dual-conflict line from requirement 8, `countClaim("conflict")`, return `err`. A secondary ref left behind by a crashed or claim-lost predecessor is otherwise never displaced by any cluster, because `ClaimPair` always creates with an empty `expected_sha` and `TakeoverPair` is only reached after a primary conflict — the task would stay unclaimable forever.
      - any other error: best-effort release the primary the same way, `countClaim("error")`, return the error.
   6. Do not read back the secondary and do not retry either write.

8. Add the two log helpers and call them only from the pair path:

   ```go
   func logDualAcquired(primary, secondary, taskID string) {
       glog.Infof("event=claim_dual_acquired key=%s second=%s task=%s", primary, secondary, taskID)
   }

   func logDualConflict(secondary, taskID string) {
       glog.Warningf("event=claim_dual_conflict key=%s task=%s", secondary, taskID)
   }
   ```

   Log the acquired line only when `secondary != ""` and both writes succeeded. Log the conflict line only when a non-empty secondary write conflicted.

9. Add `TakeoverPair` to the interface, keeping `Takeover` as `return m.TakeoverPair(ctx, key, "", taskID)`:

   ```go
   TakeoverPair(ctx context.Context, primary string, secondary string, taskID string) (*TakeoverResult, error)
   ```

   `TakeoverPair` takes `m.mu` exactly once, as `Takeover` does today; `Takeover` must delegate to it without taking the lock itself, and the single-key path through `takeoverExpired` must stay under that same lock (no nested locking).

   Behaviour:
   1. If `findHeld(primary)` or (`secondary != ""` and `findHeld(secondary)`) reports a held claim, `countClaim("conflict")`, return `ErrLeaseConflict`.
   2. `primaryRef, err := m.readBack(ctx, primary)`; a read error: `countClaim("error")`, return it.
   3. `primaryRef == nil`: delegate to `m.claimPairLocked(ctx, primary, secondary, taskID)` and return a `TakeoverResult` with `PreviousTaskID: ""`. If that returns an error, return it unchanged (the pair path already counted).
   4. `secondary == ""`: `return m.takeoverExpired(ctx, primary, taskID, primaryRef)` unchanged. This keeps single-key takeover byte-identical and keeps `takeoverExpired` used. The remaining steps apply only to a dual claim.
   5. Read back the secondary first. If it exists and its lease is not yet expired (`now < secondaryLeaseUntil+TakeoverGrace`), `countClaim("takeover_too_early")` and return `ErrLeaseNotExpired` **without writing anything**.
   6. Parse the primary `LeaseUntil`. An unparseable value or `now < leaseUntil+TakeoverGrace`: `countClaim("takeover_too_early")`, log the existing warning on the unparseable branch, return `ErrLeaseNotExpired`.
   7. `newLease := now.Add(LeaseDuration)`, `newLeaseStr := formatLeaseUntil(newLease)` once for the pair. Write the primary with `expected_sha = primaryRef.Sha`. On `ErrLeaseConflict`: `countClaim("conflict")`, return. On another error: read back and continue only when `m.ownTriple(ref, taskID, newLeaseStr)` holds; otherwise `countClaim("error")` and return the error.
   8. Write the secondary with `expected_sha` equal to the existing secondary ref's sha, or `""` when it is absent. On any error: best-effort `Delete(primary, newPrimarySha)`, `countClaim("conflict")` for a 409 or `countClaim("error")` otherwise, and return the error.
   9. Store the pair, log the dual-acquire line, `countClaim("taken_over")` once, and return a `TakeoverResult` with `PreviousTaskID` from `primaryRef.TaskID`.

10. Update the two hand-written `LeaseManager` stubs so the package compiles: `pkg/factory/claim_lifecycle_internal_test.go` (`stubLeaseManager`) and `pkg/claim/recover_startup_internal_test.go` (`recoverStub`). Add `ClaimPair` and `TakeoverPair` methods that delegate to the existing `Claim`/`Takeover` stubs (both return zero values).

11. In `pkg/handler/claim_wiring.go`, `acquireSpawnClaim` calls `claim.DeriveClaimKeysWithBody` and then:

    ```go
    _, claimErr := manager.ClaimPair(ctx, primary, secondary, string(task.TaskIdentifier))
    if claimErr == nil {
        logClaimAcquired(primary, task.TaskIdentifier)
        return true
    }
    if stderrors.Is(claimErr, claim.ErrLeaseConflict) {
        _, takeoverErr := manager.TakeoverPair(ctx, primary, secondary, string(task.TaskIdentifier))
        if takeoverErr == nil {
            logClaimAcquired(primary, task.TaskIdentifier)
            return true
        }
        return h.claimFailureDecision(task, "takeover", takeoverErr)
    }
    return h.claimFailureDecision(task, "claim", claimErr)
    ```

    Keep the fallback metric increment and warning exactly as they are. Do not add any new metric.

12. Tests in `pkg/claim/dual_acquire_test.go` (package `claim_test`). The file may mix one plain Go test with Ginkgo specs, but the plain test must not call any helper that uses Ginkgo `Expect` (`newTestManager`, `expectClaimDelta`, `tickAt` do). Safe helpers: `newFakeGitRest`, `newTestClock`, `advance`, `leaseTestT0`, `rfc3339`, `requestsMatching`, `countSuccessfulPuts`, `putExpectedSha`, `newFakeJobControl`, `snapshotClaims`. Build managers with `claim.NewLeaseManager(cluster, claim.NewClaimRefClient(fake.URL(), "", ""), job, clock, libtime.NewWaiterDuration())`.
    - `func TestDualAcquireKeys(t *testing.T)` — plain, no Ginkgo. Two managers over one `fakeGitRest`, clusters `"old"` and `"new"`. Case A: `old.Claim(ctx, "claims/repo/bborbe-nuke", "t-1")` succeeds, then `new.ClaimPair(ctx, "claims/repo/bborbe-nuke", "claims/pr/bborbe-nuke-658", "t-1")` returns an error and `stderrors.Is(err, claim.ErrLeaseConflict)` is true. Case B (fresh fake): `new.ClaimPair(...)` succeeds first, then `old.Claim(ctx, "claims/repo/bborbe-nuke", "t-1")` returns an error and `stderrors.Is(err, claim.ErrLeaseConflict)` is true. Use `t.Fatalf`, not `Expect`.
    - AC2 Ginkgo spec: claim the pair; assert `fake.Requests()` contains exactly two `PUT`s, in order `claims/repo/bborbe-nuke` then `claims/pr/bborbe-nuke-658`, both with an empty `expected_sha`.
    - AC3 Ginkgo spec: `fake.SetFault` returning `&fakeFault{Status: http.StatusConflict}` for `PUT` requests whose `Path` contains `claims/pr/`. Claim the pair, expect `stderrors.Is(err, claim.ErrLeaseConflict)`. Assert the recorded requests are, in order: `PUT claims/repo/...` (200), `PUT claims/pr/...` (409), `DELETE claims/repo/...` (200 with `expected_sha` equal to the primary sha from the first PUT). Filter with `requestsMatching(fake.Requests(), http.MethodPut|http.MethodDelete, ...)`, because the conflict path now issues a read-back `GET` on the secondary between the second `PUT` and the `DELETE`. Assert `mgr.HeldClaims()` is empty.
    - AC8(a) Ginkgo spec: `ClaimPair(ctx, "claims/task/t1", "", "t1")` records exactly one `PUT` and no `DELETE`; `HeldClaims()` has one row.
    - AC8(b) Ginkgo spec: derive with `DeriveClaimKeysWithBody(ctx, "pr-review", "t2", "bborbe", "nuke", "no pull request url here")`; assert the secondary is empty and `fallback` is true; then `ClaimPair(ctx, primary, secondary, "t2")` records exactly one `PUT` (`claims/repo/bborbe-nuke`) and no `DELETE`. This case is load-bearing: writing the per-repo key twice would conflict against its own first write and every `pr-review` task without a bare URL would stop running.
    - A Ginkgo spec that captures stderr with the existing `captureGlogStderr` helper and asserts the successful dual acquire emits `event=claim_dual_acquired` with both `key=claims/repo/bborbe-nuke` and `second=claims/pr/bborbe-nuke-658`, and that the conflict case emits `event=claim_dual_conflict`.

13. Update `pkg/claim/key_body_test.go`: the `DescribeTable` now calls `DeriveClaimKeysWithBody` and asserts four values. Add a `wantSecondary` column. Expectations: every per-PR entry gets `claims/repo/bborbe-nuke` as primary and `claims/pr/bborbe-nuke-658` (or the canonicalised number) as secondary; every fallback entry, every other repo-keyed type and `claude` gets an empty secondary; the error entries assert empty primary and empty secondary.

14. Update `pkg/handler/claim_key_body_internal_test.go`:
    - `TestAcquireSpawnClaimKeysCloneURLPRReviewPerPullRequest`: expect two `putCalls`, in order `claims/repo/bborbe-nuke` then `claims/pr/bborbe-nuke-658`.
    - `TestAcquireSpawnClaimKeysPRReviewsPerPullRequest`: rewrite for the dual write. The first task claims the pair, so `HeldClaims()` has two rows: `claims/repo/bborbe-nuke` and `claims/pr/bborbe-nuke-658`, both owned by `tid-658`. The second task (PR 662, same repository) is now refused because the shared per-repo primary is held: assert `acquireSpawnClaim` returns false, the held set is still those two rows, `client.puts` did not change, and the claim delta is `{"conflict": 2}` (ClaimPair and TakeoverPair both find the key held locally and send no request). Rename the test or its comments to state that different pull requests in one repository serialize during the dual-write era.
    - `TestAcquireSpawnClaimFallsBackToRepoKey`: unchanged expectation of one `Put`, because the fallback derivation has an empty secondary.

15. Do not change `Release`, `ReleaseAfterSuccess`, `Takeover`'s existing single-key behaviour, `Tick`, `Recover`, `Run`, `AuditMismatch`, or any metric series in this prompt. Prompts 2 and 3 extend the release and recovery paths.
</requirements>

<constraints>
- The claim ref API is unchanged: `PUT /api/v1/refs/claims/<key>` with `expected_sha`, `409` on lease mismatch, never retried.
- Keys and task ids remain forbidden as metric label values. Do not add a metric series, a metric label, a config knob, a flag or an opt-out.
- The four repo-keyed types and the task-id-keyed types keep their current key shapes. Do not change the per-PR key shape.
- Behaviour is byte-identical for every task whose two derivations are equal: one `PUT`, the same metric result, the same log line.
- Never use `fmt.Errorf`; wrap with `errors.Wrapf(ctx, err, ...)` from `github.com/bborbe/errors`. Never use `context.Background()` inside `pkg/`.
- Do NOT edit the spec file. Do NOT commit — dark-factory handles git.
- Existing tests must still pass, except the two handler tests this prompt explicitly rewrites.
- The `executor_claim_total` result set stays at the 14 series listed in `claimResults`; one `countClaim` per `ClaimPair` call.
</constraints>

<verification>
Run from the repository root:

```bash
set -o pipefail
go test ./pkg/claim/ -run TestDualAcquireKeys -v 2>&1 | tee /tmp/dual-acquire.log
grep -q -- '--- PASS: TestDualAcquireKeys' /tmp/dual-acquire.log
```

That is AC1's exact command; the `grep` proves the test was selected and passed rather than the run exiting 0 vacuously. Then:

```bash
GOFLAGS=-buildvcs=false go test ./pkg/claim/ ./pkg/handler/ -race
GOFLAGS=-buildvcs=false make precommit VERSION=dev
```

`make precommit` must exit 0.

Before you finish, re-run the commands above and confirm every one passes. Walk AC1, AC2, AC3 and AC8 against the code and the tests, including AC8(b) — a `pr-review` task with no bare PR URL must still write exactly one ref.
</verification>

<!--
OPEN QUESTION FOR THE AUDITOR / REVIEWER (not an executor step):
The spec's Non-goals accept doubled write volume but do not mention that the per-repo
primary re-serialises reviews of different pull requests in one repository for the whole
dual-write period. That is inherent to "per-repo key is primary and is written first"
(spec Desired Behaviour 1 and AC2). This prompt makes the consequence explicit in the
handler test rather than hiding it. If the reviewer intended per-PR tasks to keep running
in parallel, the primary/secondary order in AC2 would have to change, which contradicts
the spec as written.
-->
