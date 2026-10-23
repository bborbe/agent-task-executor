---
status: approved
spec: [015-dual-acquire-claim-keys]
created: "2026-10-23T19:55:00Z"
queued: "2026-10-23T19:51:10Z"
branch: dark-factory/dual-acquire-claim-keys
---

# Dual claim keys: restart recovery adopts and repairs the pair

<summary>
- After a restart, a running Job whose claim left both refs on the shared origin gets both refs re-adopted as one claim.
- A claim that left only the per-PR ref — the shape a pre-upgrade cluster wrote — is adopted and its per-repo partner is derived and written, so the pair is repaired instead of inherited half-written.
- A claim that left only the per-repo ref is adopted as a single claim, because the per-PR partner cannot be derived from a repository key.
- Fencing or releasing an adopted pair removes both refs and deletes the Job once.
- A repair whose partner write conflicts with another cluster releases the adopted ref rather than holding half a pair.
- The existing skip rules for other clusters and for streak refs are unchanged.
</summary>

<objective>
Extend restart recovery from one ref per claim to a pair: group the listed own-cluster refs by task, decide once per group whether to adopt, fence or release, and repair a per-PR-only claim by deriving and writing its per-repo partner. End state: a restarted executor holds both refs of every dual claim its predecessor left, and a half-written pair is completed rather than left half-owned.
</objective>

<context>
Read these files fully before editing:

- `pkg/claim/lease_manager.go` — `Recover`, `recoverOne`, `recoverLeaseUntil`, `recoverFence`, `recoverAdopt`, `heldClaim`, `findHeld`, `bestEffortDelete`, `storeClaim`, `RedriveStreakPrefix` usage in `Recover`.
- `pkg/claim/key.go` — `repoKeyedTaskTypes`, `DeriveClaimKey`, `DeriveClaimKeysWithBody`, `pullRequestNumberFromLine`, `validateClaimComponent`. Prompt 1 renamed the body-aware derivation and added the pair; this prompt adds one derivation helper here.
- `pkg/claim/lease_manager_restart_test.go` and `pkg/claim/redrive_streak_recovery_test.go` — the existing single-ref recovery expectations that must keep passing. `seedRestartFixture` seeds five single-ref claims; every one of them must keep its current outcome.
- `pkg/claim/fake_git_rest_test.go` — `Seed(ref, sha, cluster, taskID, leaseUntil)`, `Ref`, `Requests`, `SetFault`, `fakeRequest`.
- `pkg/claim/lease_manager_test_helpers_test.go` — `newTestManager`, `newTestClock`, `advance`, `expectClaimDelta`, `snapshotClaims`, `requestsMatching`, `rfc3339`, `newFakeJobControl`, `leaseTestT0`.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`, `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`.

Prompts 1 and 2 (executed before this one) made acquisition and the release/renew/fence paths pair-aware. This prompt covers the last lifecycle path. The spec is `specs/in-progress/015-dual-acquire-claim-keys.md`; this prompt covers Desired Behaviours 4 and 6 and Acceptance Criteria AC6 (held-set retention across ticks) and AC7.
</context>

<requirements>
1. Add one derivation helper to `pkg/claim/key.go`:

   ```go
   // repoKeyFromPRKey returns the per-repository key for a per-pull-request key
   // of the shape claims/pr/<owner>-<repo>-<n>, where <n> is the trailing
   // all-digit segment. It returns false for any other shape, so a caller never
   // guesses a partner key.
   func repoKeyFromPRKey(key string) (string, bool)
   ```

   Implementation: require the `claims/pr/` prefix; take the substring after it; find the last `-`; require a non-empty prefix before it and a non-empty trailing segment of 1 to 10 ASCII digits whose value is at least 1; return `"claims/repo/" + prefix`. Do not parse owner and repo separately — the PR number is the last dash-separated segment, so an owner or repository that itself ends in a digit-containing segment stays intact.

2. Rewrite `Recover` to group before deciding. Keep the `List(ctx, "claims/")` call, the streak-ref skip (`strings.HasPrefix(strings.TrimPrefix(ref.Ref, "refs/"), RedriveStreakPrefix)`) applied **before** the cluster comparison, and the own-cluster filter exactly as today. Group the surviving refs by `ref.TaskID` into `map[string][]ClaimRef`, then iterate the task ids in sorted order so recovery is deterministic. Within a task id, a repository key (`claims/repo/...`) and a per-PR key (`claims/pr/...`) form one pair; any other combination is processed as separate single claims in sorted key order. A task with one ref is a single claim.

3. Replace `recoverOne` with `recoverGroup(ctx context.Context, refs []ClaimRef)` and delete the old function:
   1. Order the refs: the primary is the repository key and the secondary is the per-PR key. `recoverGroup` is called only with a pair of that shape or with a single ref; it never pairs two refs of the same shape.
   2. Compute the effective lease as the earliest `recoverLeaseUntil(now, ref)` across the group. An unparseable `lease_until` already counts `"error"` and logs in `recoverLeaseUntil`; it yields a past lease and therefore fences the group.
   3. `fenceDue(now, effectiveLease)` → `recoverFenceGroup`; otherwise `recoverAdoptGroup`.

4. Replace `recoverFence` with `recoverFenceGroup(ctx context.Context, primary ClaimRef, secondary *ClaimRef, leaseUntil libtime.DateTime)` and delete the old function:
   - One `IsJobRunning(ctx, primary.TaskID)` call. On a running Job (or a running-check error) delete the Job once. If the deletion fails, call `countClaimFencePending(primaryKey, primary.TaskID)` exactly as `recoverFence` does today (`pkg/claim/lease_manager_restart_test.go` asserts `fenced: 1` on this path), store one pending-deletion `heldClaim` carrying both refs (`key`, `sha`, `secondKey`, `secondSha`, `pendingJobDeletion = true`, `pendingDeletionPath = "fence"`) and return.
   - Otherwise best-effort delete each ref with its listed sha, drop the claim, and call `countClaimRemoval(primaryKey, primary.TaskID, "fenced", primary.Sha)` once.
   - Do not call `DeleteJob` twice for a pair.

5. Replace `recoverAdopt` with `recoverAdoptGroup(ctx context.Context, primary ClaimRef, secondary *ClaimRef, leaseUntil libtime.DateTime, now libtime.DateTime)` and delete the old function:
   - One `IsJobRunning(ctx, primary.TaskID)` call.
   - Running-check error: store the pair (or single) held with `needsRunningCheck = true`, `renewNextTick = true`, `lastRenewAt = now`, and `countClaim("error")` as today.
   - Job running: store the pair (or single) held with `renewNextTick = true`, `lastRenewAt = now`, and `countClaim("adopted")` once.
   - Job not running: delete each listed ref with its listed sha, applying the existing outcome rules (`nil` → `"released"`, `ErrLeaseConflict` → `"already_released"`, any other error → `"error"` and do not hold), and call `countClaimRemoval` once.
   - **Half-pair repair** applies only in the two adopt branches, only when `secondary == nil`, and only when the single key starts with `claims/pr/`:
     1. `derived, ok := repoKeyFromPRKey(singleKey)`; when `!ok`, adopt the single claim as today.
     2. `PUT(ctx, derived, "", m.cluster, primary.TaskID, now.Add(LeaseDuration))`.
     3. Success: with `newLease := now.Add(LeaseDuration)`, store one held claim with `key = derived`, `sha = returned sha`, `secondKey = singleKey`, `secondSha = primary.Sha`, `leaseUntil = newLease`, `leaseUntilStr = formatLeaseUntil(newLease)`, `renewNextTick = true`, `lastRenewAt = now`, and `countClaim("adopted")` once. Derive the string from the value written to git-rest, never from the listed ref's lease — `ownTriple` compares against `held.leaseUntilStr` on every failed renew, so a mismatch makes a later read-back unrecognisable and `claimLost` deletes a healthy Job.
     4. `ErrLeaseConflict`: `countClaim("conflict")`, best-effort delete the listed ref with its sha, and do not hold the claim (another cluster owns the per-repo key, so the pair is not ours).
     5. Any other error: `countClaim("error")` and adopt the single listed ref so its Job keeps being renewed and fenced.
   - When the single key is a repository or task key, adopt it as a single claim with no partner write. A repository key does not say whether the task was a `pr-review` and cannot yield the PR number, so it is not derivable; do not guess one.

6. Keep `recoverLeaseUntil`'s unparseable-lease logging and `"error"` count. Do not change `Recover`'s never-return-an-error-on-a-single-claim behaviour: a per-claim failure is counted and handled, and only the initial `List` error is returned. Do not leave `recoverOne`, `recoverFence` or `recoverAdopt` behind as unused functions — the linter rejects them.

7. Tests in `pkg/claim/dual_restart_test.go` (package `claim_test`, Ginkgo). Wrap the specs in `Describe("Dual claim restart recovery", ...)` and name the half-pair repair spec exactly `"repairs a per-PR-only pair"` — the verification below focuses on both strings. Seed with `fake.Seed("refs/claims/repo/bborbe-nuke", "SP", "self", "t-dual", rfc3339(leaseTestT0.Add(1000*libtime.Second)))` and the matching `refs/claims/pr/bborbe-nuke-658`. Assert held rows with `HeldClaims()` (two rows for a pair) and requests with `requestsMatching`.
   - **AC7(a)**: both refs seeded, cluster `"self"`, running Job → `Recover`; `HeldClaims()` has two rows (both keys, same task id, the listed shas); no `PUT` was sent; the claim delta is `{"adopted": 1}`.
   - **AC7(b)**: only `refs/claims/pr/bborbe-nuke-658` seeded, cluster `"self"`, running Job → `Recover`; exactly one `PUT` is recorded for `claims/repo/bborbe-nuke` with an empty `expected_sha`; `HeldClaims()` has two rows, sorted by key — so `claims/pr/bborbe-nuke-658` first and `claims/repo/bborbe-nuke` second (`'p' < 'r'`), matching prompt 1's sort rule.
   - A row for the documented limitation: only the repository ref seeded → `Recover` adopts one row and sends no `PUT`.
   - A row for the repair conflict: only the PR ref seeded and every `PUT` answering 409 → `Recover` sends the partner `PUT`, records a `DELETE` for the PR ref, and holds nothing.
   - A row for fencing a pair: both refs seeded with a lease inside the fence window, running Job → `Recover`; two `DELETE`s with the two listed shas, one `DeleteJob`, `HeldClaims()` empty.
   - A row for releasing a pair: both refs seeded, Job not running → `Recover`; two `DELETE`s, held empty.
   - Keep every existing case in `lease_manager_restart_test.go` and `redrive_streak_recovery_test.go` green without editing their expectations.
</requirements>

<constraints>
- The claim ref API is unchanged: `PUT`/`DELETE /api/v1/refs/claims/<key>` with `expected_sha`, `409` on lease mismatch, never retried.
- Restart recovery never sends a create `PUT` for a ref that already exists with the manager's own cluster, except the half-pair repair in requirement 5, which writes only the missing partner.
- Keys and task ids remain forbidden as metric label values. Do not add a metric series.
- A claim is not held while only one of its refs is held.
- Never use `fmt.Errorf`; wrap with `errors.Wrapf(ctx, err, ...)` from `github.com/bborbe/errors`.
- Do NOT edit the spec file. Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run from the repository root:

```bash
set -o pipefail
go test ./pkg/claim/ -race -v -ginkgo.v -ginkgo.focus='Dual claim restart recovery' 2>&1 | tee /tmp/dual-restart.log
grep -q 'repairs a per-PR-only pair' /tmp/dual-restart.log
```

The `grep` proves the new specs were selected and ran rather than the run exiting 0 with zero specs. Then:

```bash
GOFLAGS=-buildvcs=false go test ./pkg/claim/ -race
GOFLAGS=-buildvcs=false make precommit VERSION=dev
```

`make precommit` must exit 0.

Before you finish, re-run the commands above and confirm every one passes. Walk AC7 against the code and the tests, and confirm the existing single-ref restart and streak-ref expectations still hold.
</verification>

<!--
OPEN QUESTION FOR THE AUDITOR / REVIEWER (not an executor step):
The spec's Failure Modes table has a row "Primary written, process dies before the
secondary" whose recovery is "restart recovery finds one ref, adopts it, re-derives and
writes the other". Because the per-repo key is written first (AC2), that crash leaves a
per-repo-only ref, and a per-repo key cannot yield the per-PR key: it does not say whether
the task was a pr-review and it does not carry the PR number. The derivable direction is
the opposite one — claims/pr/<owner>-<repo>-<n> yields claims/repo/<owner>-<repo> by
stripping the trailing numeric segment — which is also the shape a pre-upgrade (v0.30.9)
cluster left behind. This prompt therefore repairs the per-PR-only case (AC7's tested
case) and adopts a per-repo-only claim as a single claim. If the reviewer wants the crash
window repaired too, the manager would need the task body at recovery time (a new
injection into pkg/claim) or a third marker ref, both outside this spec's constraints.
-->
