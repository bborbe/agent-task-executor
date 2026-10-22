---
status: draft
spec: [011-claim-mode-dispatch-wiring]
created: "2026-10-21T23:30:14Z"
branch: dark-factory/claim-mode-dispatch-wiring
---

# Attempt one lease takeover on a claim conflict

<summary>
- A `409` on the claim is no longer the end of the decision: the wiring makes exactly one takeover attempt against the listed claim.
- A lease that is still valid, or that expired less than 30 seconds ago, is not takeable and the attempt is skipped.
- An expired lease is replaced with a single compare-and-swap write carrying the expired claim's sha and this executor's cluster name.
- In `on` a successful takeover is what lets the Job proceed; in `shadow` the Job proceeds either way and only the counters differ.
- A lost takeover race, a live lease, and a takeover error all skip in `on` and proceed in `shadow`.
- No retry, no second takeover, no re-claim, no change to the claim library.
- Tests cover the expired case, the still-valid case, the inside-grace case, and the lost race.
</summary>

<objective>
Complete the conflict branch so a task whose previous lease expired is taken over and dispatched, while a live lease still blocks `on` and the takeover attempt is recorded on the existing claim counters.
</objective>

<context>
Read these files before changing anything:

- `pkg/handler/claim_wiring.go` — `acquireSpawnClaim` and `claimFailureDecision` from prompt 2.
- `pkg/handler/task_event_handler.go` — `spawnIfNeededClaimed` and the claim gate inserted by prompt 2.
- `pkg/claim/lease_manager.go` — `Takeover`, `takeoverExpired`, `TakeoverGrace`, `ErrLeaseNotExpired`, and the `taken_over` / `takeover_too_early` / `conflict` / `error` increments.
- `pkg/claim/ref_client.go` — `ClaimRef`, `ErrLeaseConflict`, and the `Put` argument order `(ctx, key, expectedSha, cluster, taskID, leaseUntil)`.
- `pkg/claim/key.go` — `DeriveClaimKey`.
- `pkg/handler/claim_gate_internal_test.go` — the internal harness and `recordingClaimRefClient` from prompt 2.
- `pkg/handler/task_event_handler_claim_test.go` — the external harness from prompts 1 and 2.
- `docs/claim-lease-protocol.md` — the takeover and grace rules.

Coding docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md`
</context>

<requirements>
1. In `acquireSpawnClaim` (`pkg/handler/claim_wiring.go`), replace the single error path after `manager.Claim` with this decision sequence. Add `stderrors "errors"` to `claim_wiring.go`'s imports (the handler package uses that alias elsewhere, but imports are per file). The `claim` package is already imported there.

```go
_, claimErr := manager.Claim(ctx, key, string(task.TaskIdentifier))
if claimErr == nil {
	return true
}
if stderrors.Is(claimErr, claim.ErrLeaseConflict) {
	// One takeover attempt against the listed claim. A live lease (including
	// one inside the takeover grace) and a lost compare-and-swap race both
	// leave the key with its current owner; the claim package has already
	// counted conflict / takeover_too_early / taken_over / error.
	_, takeoverErr := manager.Takeover(ctx, key, string(task.TaskIdentifier))
	if takeoverErr == nil {
		return true
	}
	return h.claimFailureDecision(task, "takeover", takeoverErr)
}
return h.claimFailureDecision(task, "claim", claimErr)
```

   Requirements on that branch:
   - Exactly one `Takeover` call per `Claim` conflict. No loop, no retry, no second `Claim`.
   - A successful takeover returns `true` in both `shadow` and `on`.
   - Any takeover error, including `claim.ErrLeaseNotExpired` and `claim.ErrLeaseConflict`, goes through `claimFailureDecision`, so `on` skips and `shadow` proceeds.
   - The wiring increments no counter and adds no metric.

2. Keep the pre-existing paths unchanged: mode `off` returns `true` immediately; a lease-manager or key-derivation error still goes through `claimFailureDecision` with stages `lease_manager` and `derive_key`.

3. Extend `pkg/handler/claim_gate_internal_test.go` (package `handler`) with takeover specs using `recordingClaimRefClient`. The fake's `Put` must return `claim.ErrLeaseConflict` when `expectedSha == ""` and a new sha when `expectedSha` is the listed claim's sha. The fake's `List` must return one `claim.ClaimRef` whose `Ref` is `"refs/" + key` (for a non-repo task keyed by id, `refs/claims/task/<taskID>`), with the listed `Sha` and an RFC3339 `LeaseUntil`. Build lease deadlines from `time.Now().UTC()`.
   - expired by more than the grace (`now - 31s`): `acquireSpawnClaim` returns true; `taken_over` +1 and `conflict` +1, every other series +0; the second `Put` (index 1) carries `expectedSha == "expired-sha"` and `cluster == "self"`.
   - still in the future (`now + 5m`): returns false in mode `on`; `takeover_too_early` +1 and `conflict` +1, every other series +0; `len(client.putCalls) == 1` (the takeover never writes).
   - expired 5 seconds ago (inside the 30 s grace): returns false in mode `on`; `takeover_too_early` +1 and `conflict` +1; `len(client.putCalls) == 1`.
   - takeover race lost: `Put` returns `ErrLeaseConflict` for both the empty and the expired sha; mode `on` returns false; `conflict` +2.
   - takeover returns a generic error: mode `on` returns false, mode `shadow` returns true; in both `conflict` +1 (the initial claim) and `error` +1, every other series +0.
   - in mode `shadow`, the expired-lease case still returns true and creates the same counter deltas.

4. Extend `pkg/handler/task_event_handler_claim_test.go` (package `handler_test`) with the same cases driven through `ConsumeMessage` and `mocks.FakeClaimRefClient`:
   - **AC3 `shadow` live lease**: `PutStub` returns `claim.ErrLeaseConflict` when `expectedSha == ""`, otherwise a sha; `ListReturns` one ref with `LeaseUntil` five minutes in the future. Consume → `SpawnJobCallCount() == 1`; `conflict` +1, `takeover_too_early` +1, every other series +0.
   - **AC4 `on` live lease**: same client in mode `on` → `SpawnJobCallCount() == 0`; same counter deltas.
   - **AC6 expired lease in `on`**: `ListReturns` one ref whose `LeaseUntil` is 31 seconds in the past and whose `Sha` is `expired-sha`; `PutStub` returns `claim.ErrLeaseConflict` for `expectedSha == ""` and `("new-sha", nil)` for `expectedSha == "expired-sha"`. Consume → `SpawnJobCallCount() == 1`; `taken_over` +1, `conflict` +1, every other series +0. Read `PutArgsForCall(1)` and assert the key argument is the derived key, `expectedSha` is `expired-sha`, and `cluster` is `self`.
   - **AC6 inside the grace in `on`**: same fixture with `LeaseUntil` 5 seconds in the past → `SpawnJobCallCount() == 0`; `takeover_too_early` +1, `conflict` +1.
   - **AC6 still valid in `on`**: `LeaseUntil` five minutes in the future → `SpawnJobCallCount() == 0`; `takeover_too_early` +1, `conflict` +1.
   - Keep the prompt-2 specs green, including the `off` AC2 spec and the `shadow` 200 spec (which must still show `expected_sha == ""` on the first `Put`).

5. Add a `CHANGELOG.md` entry under `## Unreleased`:
   `- feat: attempt one lease takeover on a 409 claim conflict and proceed on an expired lease`

6. Before finishing, walk every acceptance criterion above against the change and re-run `<verification>`.
</requirements>

<constraints>
- `pkg/claim` is called, never modified. Its constants, fence arithmetic, grace rule and tests are frozen by spec 009.
- Exactly one takeover attempt per conflict; no retry loop and no second claim.
- The claim stays inside `spawnIfNeededClaimed`, after every existing gate and before `h.jobSpawner.SpawnJob`, under the per-assignee spawn lock.
- Keys come from `claim.DeriveClaimKey`; the wiring invents no key.
- No new metric and no new label value. `executor_claim_total` is incremented only inside `pkg/claim`.
- No claim release, restart recovery, lease `Run` loop, mismatch audit, or `spec.rerunOnLostClaim` wiring. The claim is deliberately left held after the Job is created.
- No new endpoint configuration and no new secret; the claim client reuses the executor's git-rest URL and gateway secret.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
```
ROOTDIR=/workspace make precommit
go test -race -count=1 ./...
grep -n 'Takeover(' pkg/handler/claim_wiring.go
grep -c 'manager.Takeover(' pkg/handler/claim_wiring.go
```
- `make precommit` exits 0.
- `go test -race -count=1 ./...` exits 0.
- The `Takeover(` grep returns the single call site inside `acquireSpawnClaim`.
- The count grep prints `1`.
- `! grep -q 'ClaimTotal.WithLabelValues' pkg/handler/claim_wiring.go pkg/handler/task_event_handler.go` exits 0 (the wiring increments nothing).
</verification>
