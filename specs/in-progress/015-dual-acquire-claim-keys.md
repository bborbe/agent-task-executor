---
status: prompted
approved: "2026-10-23T19:15:49Z"
generating: "2026-10-23T19:18:10Z"
prompted: "2026-10-23T19:41:09Z"
branch: dark-factory/dual-acquire-claim-keys
---

## Summary

- A claim key is derived from the task, and the derivation changed shape between v0.30.7 (`claims/repo/<owner>-<repo>`) and v0.30.9 (`claims/pr/<owner>-<repo>-<n>`). A cluster on the old scheme and a cluster on the new one both acquire successfully and neither can see the other's ref.
- Measured 2026-10-23 on a real rolling upgrade: three `pr-review` task ids ran concurrently on nuke prod and sun inside the mixed window, out of 85 distinct ids sampled — and none outside it.
- The lease mechanism is not weakened during such a window, it is silently absent, and it reports success. The same failure recurs at every future key-scheme change.
- Fix: while the fleet is mixed, a repo-keyed claim acquires **both** refs and the task spawns only if it wins both. One lease still excludes the other cluster, whatever scheme it runs.

## Problem

`DeriveClaimKeyWithBody` (`pkg/claim/key.go`) maps a `pr-review` task to `claims/pr/<owner>-<repo>-<n>` when its body carries a bare pull-request URL, and falls back to `claims/repo/<owner>-<repo>` otherwise. The per-PR scheme arrived in v0.30.9. During a rolling upgrade the fleet runs both versions for as long as the rollout takes, and each version derives only its own key. Two clusters therefore hold two different refs for the same task, both succeed, and the task runs twice — the exact outcome a leased claim exists to prevent.

The fallback cuts both ways: because a v0.30.9+ client still derives the per-repo key for a `pr-review` body with no bare URL, the two schemes already coexist inside a single client. A guard keyed on "my version's scheme" would therefore be wrong.

## Goal

A key-scheme change can be rolled out one cluster at a time without ever creating a window in which two clusters honour different refs for the same work. A task is spawned only by the cluster that holds **every** claim ref its key derivation produces.

## Non-goals

- Removing the per-repo fallback. It stays; this spec makes it safe.
- Changing the per-PR key shape, the four repo-keyed task types, or the task-id-keyed types.
- Tuning claim write volume. Doubling writes for repo-keyed tasks is a real, accepted cost of this design; it is watched outside this repo, by the `Claim Pushes Stay Within GitHub Limits Under Load` task in the claim-scheduling goal.
- Removing the dual write. That is a later, separate release once every cluster runs this one.
- Restoring per-repo serialisation of `pr-review` claims. Writing the per-repo ref for every repo-keyed task means two reviews of the same repo cannot run concurrently even when they are different PRs — the property `A pr-review Repo Claim Is Held Only While a Review of That Repo Runs` removed. This spec reintroduces it for the duration of the dual write; that is the accepted price of the migration, and dropping the dual write restores the previous behaviour.

## Assumptions

- git-rest on the shared vault origin is the only coordination point between clusters. No cluster reads another's memory or config.
- Both clusters point at the same vault origin, so a ref written by one is visible to the other.
- A second claim ref write per repo-keyed task is affordable. The Non-goals row above owns the risk; this spec does not measure it.
- The rollout is operator-paced and may span hours. The spec must be correct at every intermediate fleet composition, including "one cluster upgraded, two not".

## Acceptance Criteria

- [ ] AC1 — two executors, one deriving `claims/pr/…` and one deriving `claims/repo/…` for the same task, cannot both acquire it: the second `Claim` returns `ErrLeaseConflict`. Evidence: `go test ./pkg/claim/ -run TestDualAcquireKeys -v` passes, where `TestDualAcquireKeys` is a **plain Go test**, not a Ginkgo `It` — the package's Ginkgo entry point is `TestClaim`, so a `-run TestDualAcquireKeys` against an `It` selects zero tests and exits 0 vacuously. The test asserts `stderrors.Is(err, ErrLeaseConflict)` on the second acquire.
- [ ] AC2 — when the two derivations differ, acquiring writes both refs, per-repo first. Evidence: the fake git-rest records exactly two `PUT`s, in order `claims/repo/<owner>-<repo>` then `claims/pr/<owner>-<repo>-<n>`; the test asserts that ordered slice.
- [ ] AC3 — when the second write conflicts, the first is released and no Job is created. Evidence: the fake records `PUT A`, `PUT B → 409`, `DELETE A` in that order, and the spawn path returns false.
- [ ] AC4 — **every** release path removes both refs. Evidence: a table-driven test with one case per path — plain `Release`, `ReleaseAfterSuccess` (success hold), fenced, jobless release, release-after-running-check, takeover, and claim-lost — each asserting the fake records two `DELETE`s carrying the two held shas. One documented exception: claim-lost sends no ref `DELETE` at all by design (the claim already belongs to another cluster), so its row asserts zero ref `DELETE`s and one Job delete. A path that deletes only the primary fails its own row.
- [ ] AC5 — a renew tick renews both refs, and a fence deletes both. Evidence: after one renew tick the fake records two `PUT`s whose `expected_sha` are the two held shas and whose `lease_until` are equal; after a fence it records two `DELETE`s and the held set is empty.
- [ ] AC6 — a partial release is retried, not dropped. Evidence: with the second `DELETE` returning a non-`409` error, the held set still contains both keys and the next tick records the retried `DELETE`; the claim is not removed from `held`.
- [ ] AC7 — restart recovery re-adopts both refs for a running Job, and repairs a half-written pair. Evidence: a listing carrying both keys with the manager's own cluster yields two adopted claims; a per-PR-only listing adopts the PR ref and records a `PUT` for the derived per-repo partner; a per-repo-only listing is adopted as a single claim with no `PUT`, because a repo key carries neither the task type nor the PR number and the partner is therefore not derivable.
- [ ] AC8 — a task whose two derivations are equal acquires exactly one ref, in both shapes. Evidence: two cases. (a) A task-id-keyed type records exactly one `PUT` and no `DELETE`. (b) A `pr-review` task whose body carries no bare PR URL records exactly one `PUT` and no `DELETE` — the fallback shape named in Problem. Case (b) is load-bearing: an implementation that special-cases only task-id types writes the per-repo key twice, the second `PUT` conflicts against its own first, and per DB2 the primary is released and no Job spawns, so every `pr-review` task without a bare URL stops running.
- [ ] AC9 — the protocol document states pair handling. Evidence: `grep -n 'dual acquire' docs/claim-lease-protocol.md` returns at least one line, and the same file's Claim keys, Fence, Takeover and Restart recovery sections each name the pair.
- [ ] **Post-Deploy (Rung-2):** **AC10** — after one `pr-review` task with a bare PR URL is dispatched in the window, the dual write is live on nuke dev: the executor logs one `event=claim_dual_acquired` line naming both keys. Evidence: `kubectlnukedev -n dev logs deploy/agent-task-executor-openclaw --since=30m | grep 'event=claim_dual_acquired'` returns ≥1 line containing both `key=claims/repo/` and `second=claims/pr/`. A window with no repo-keyed dispatch proves nothing and does not satisfy this.
  - `deploy_check:` `kubectlnukedev -n dev get deploy/agent-task-executor-openclaw -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `v0.34.0` — predicted next minor, since the spec's CHANGELOG bullet is a `feat:`. Override with the tag the releaser actually cuts if it differs.
- [ ] **Post-Deploy (Rung-3):** **AC11** — across a sampled rollout window spanning nuke prod and sun, zero task ids have two Job starts whose run intervals intersect, against positive evidence that the window was busy and mixed. Evidence: sample both clusters every 10 min into one file, recording per pass each cluster's running Job image and every `pr-review` Job's `creationTimestamp`/`completionTimestamp`; score by joining on the **8-character task-id prefix** in the Job name (shape `<assignee>-<id[:8]>-<timestamp>`, per `pkg/spawner/job_spawner.go`) and intersecting `[creation, completion]` across clusters — a full-UUID join matches nothing and passes vacuously. The record names the window start and end, both clusters' images per pass, at least 85 distinct `pr-review` ids claimed (the pre-fix sample carried 3 collisions in 85, so a smaller window cannot discriminate), and zero intersecting pairs.
  - `deploy_check:` `kubectlnukeprod -n prod get deploy/agent-task-executor-openclaw -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `v0.34.0` — predicted next minor; override with the tag the releaser actually cuts if it differs. Sun is deliberately unchecked here: a mixed window requires sun on the pre-fix image, so a sun freshness gate would contradict the criterion.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — lint and vet clean
- `go test ./pkg/claim/ -race` — the claim package suite, covering AC1–AC8
- `grep -n 'dual acquire' docs/claim-lease-protocol.md` — AC9

### Operator-executable (runs on the host after PR merge)

- `make build` — the image builds
- the rollout of the released tag across nuke dev, nuke prod and sun — AC10, AC11

## Desired Behavior

1. **Derive the pair.** For a repo-keyed task, derivation yields the per-repo key always and the per-PR key when the body qualifies. The per-repo key is the primary and is written first; the per-PR key is the secondary. When the two are equal — every task-id-keyed type, and a `pr-review` task whose body has no bare PR URL — the pair collapses to one key and today's behaviour is unchanged.
2. **Acquire both, or neither.** The claim path writes the primary, then the secondary. The spawn proceeds only when both are held. A secondary write failing with `ErrLeaseConflict` releases the primary and the task is treated as claimed elsewhere.
3. **One log line per dual acquire.** A successful dual acquire logs `event=claim_dual_acquired key=<primary> second=<secondary> task=<id>` at info level. A secondary conflict logs `event=claim_dual_conflict key=<secondary> task=<id>` at warning level.
4. **Hold and release the pair as one unit.** Renew, fence, jobless release, takeover, lost-claim handling and restart recovery act on both refs together, and every release path — plain, success hold, fenced, jobless, release-after-running-check, takeover, claim-lost — removes both. A claim is not held while only one of its refs is held, and a release that removes one ref but not the other leaves the claim pending-release rather than half-released.
5. **Both refs share one effective expiry.** The two refs are renewed in the same tick, so their `lease_until` values are equal by construction and drift between them is bounded by one `RenewInterval`. The effective expiry of the pair is that shared value.
6. **Restart recovery adopts the pair.** A listing carrying both refs for a running Job re-adopts both. A listing carrying only one adopts that one and re-derives the other, so a half-written pair from a crashed predecessor is repaired rather than inherited.
7. **The fallback population is covered for free.** Because the per-repo key is written for every repo-keyed task, a `pr-review` task whose body carries no PR URL is excluded from the other cluster by the same write that excludes it today.

## Constraints

- The constants in `docs/claim-lease-protocol.md` stay invariants. No knob, flag or opt-out is added for lease length, tick, fence, takeover or jobless grace.
- The claim ref API is unchanged: `PUT /api/v1/refs/claims/<key>` with `expected_sha`, `409` on lease mismatch, never retried.
- Keys and task ids remain forbidden as metric label values.
- The four repo-keyed types and the task-id-keyed types keep their current key shapes.
- Behaviour is byte-identical for every task whose two derivations are equal.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection |
|---|---|---|---|
| Secondary key write conflicts | Primary released, no Job created, `event=claim_dual_conflict` warning | Next dispatch pass retries the whole pair | the warning line, one per conflict |
| Secondary key write fails without 409 | Same as conflict: release the primary, log the error, no Job | Retry next pass; the primary is not held | `executor_claim_total{result="error"}` and the error log |
| Primary written, process dies before the secondary | Restart recovery finds one ref, adopts it, re-derives and writes the other | Automatic on restart; the task is not spawned until both are held | the extra `PUT` in the recovery pass |
| One of the two refs cannot be released | Claim stays pending-release, retried every tick until the fence point | Existing pending-release path; no new state | the claim stays in the held set across ticks; `event=claim_released` never fires for it |
| Fleet permanently mixed (a cluster never upgraded) | Every cluster keeps writing both refs; correctness holds, write volume stays doubled | The operator completes the rollout; the dual write is removed in a later release | each cluster's running Job image per pass |
| A secondary ref outlives its owner (crash, claim-lost, a failed DELETE) | The next `ClaimPair` that conflicts on the secondary reads it back and displaces it with a compare-and-swap once its lease has expired, so the task is not permanently unclaimable | Automatic on the next dispatch pass; no operator action | the `taken_over` count rising where a plain create would have failed |
| git-rest rate-limits the doubled writes | A write failing on 429 is treated as a plain error: the primary is released and the pass retries | Next pass retries; the accepted cost is that the doubled volume may lengthen the retry cycle | `executor_claim_total{result="error"}` |

## Security / Abuse

The change adds one more ref write per repo-keyed claim against the same git-rest endpoint under the same credentials. No new secret, no new scope, no new endpoint. `validateClaimComponent` applies to both keys unchanged, so a hostile task body cannot inject a path segment through the secondary key.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Pair derivation + dual acquire in `pkg/claim` | 1, 2, 3, 7 | 1, 2, 3, 8 | — |
| 2 | Release-path matrix + renew/fence over the pair | 4, 5 | 4, 5, 6 | prompt 1 |
| 3 | Restart recovery and pending-release over the pair | 4, 6 | 6, 7 | prompt 1 |
| 4 | Protocol doc + CHANGELOG | — | 9 | prompt 1 |

Rationale: prompt 1 establishes the pair and the acquire path; prompt 2 carries the release-path matrix, which is the largest single surface (seven paths, each with its own method); prompt 3 extends restart recovery over the held set; prompt 4 is documentation and runs last.

AC10 and AC11 are verification-rung criteria, not prompt rows — they are satisfied by the operator-executable rollout after merge, never by a container prompt.

## Do-Nothing Option

Every future key-scheme change repeats the 2026-10-23 failure: a rolling upgrade silently disables the claim mechanism for the length of the rollout, and the fleet runs the same task on two clusters while reporting success. The next scheme change is already foreseeable — the per-PR key exists because per-repo keying serialised reviews — so the cost is not hypothetical and recurs at each change.
