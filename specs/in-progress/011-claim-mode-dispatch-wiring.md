---
status: verifying
approved: "2026-10-21T23:26:26Z"
generating: "2026-10-21T23:27:32Z"
prompted: "2026-10-21T23:38:12Z"
verifying: "2026-10-22T00:09:28Z"
branch: dark-factory/claim-mode-dispatch-wiring
---

## Summary

- Adds the `CLAIM_MODE` environment variable (`off` | `shadow` | `on`, default `off`) to the executor and wires the existing, already-tested `pkg/claim` lease manager into the one dispatch funnel every spawn path passes through.
- `off` keeps today's behaviour byte for byte: the claim ref client is never constructed and no claim request is made, so the merge is inert on every cluster until an operator sets the variable.
- `shadow` claims exactly as `on` would but never blocks a spawn — the existing `cluster:` filter still decides — so a cluster can prove the protocol against real traffic before it is allowed to gate anything.
- `on` makes the claim decide: a lease conflict skips the task for that pass, a successful takeover after another cluster's lease expired proceeds, and a claim error fails closed rather than risking two clusters running one task.
- Nothing here changes `pkg/claim`; this spec is the dispatch contract around it. Releasing claims, restart recovery and the mismatch audit are the separate spec `specs/claim-lease-lifecycle-wiring.md`.

## Problem

`pkg/claim` implements the lease protocol and is fully unit-tested, but no production file imports it: `specs/in-progress/009-claim-lease-client-library.md` and `specs/in-progress/010-config-rerun-on-lost-claim.md` both defer `CLAIM_MODE` and all dispatch wiring to a later spec. Until that wiring exists the executor still decides what to run from the `cluster:` frontmatter field alone, so the claim work is dead code and the goal's success criteria — one Job per task across clusters, no overlapping per-repo Jobs — cannot be observed at all.

The wiring is where the risk lives. A claim taken after a Job is created cannot prevent the duplicate it exists to prevent; a claim taken before the existing gates holds a lease for a task those gates would have skipped; and a claim error treated as success reintroduces exactly the concurrent duplicate the protocol was built to stop. Each of those is a silent runtime failure that no unit test of the library can see.

## Goal

Every executor carries one `CLAIM_MODE` value from the environment. At `off` it behaves exactly as it does today and constructs no claim client. At `shadow` it claims immediately before creating a Job, records every outcome on the existing counters, and still spawns whatever the `cluster:` filter selects. At `on` the held claim is a precondition of the Job: no claim, no Job. In both live modes a `409` is followed by one takeover attempt against the listed claim, and a lease that has not expired skips the task.

## Non-goals

- No rollout. No `shadow` observation window, no switch to `on`, no second dev-pool executor, no nuke dev, prod or sun step — those are the anchored task's subtasks and need operator approval.
- No claim release, restart recovery, lease `Run` loop or mismatch audit — `specs/claim-lease-lifecycle-wiring.md` owns those. This spec leaves a claim held after its Job is created; that is deliberately incomplete and must not ship to a cluster alone.
- No `spec.rerunOnLostClaim` consequence wiring (park vs re-dispatch after a takeover). This spec re-dispatches on takeover, which is the documented default; the park path is a separate spec.
- No stage-pool key selection: the key comes from `claim.DeriveClaimKey` and the task's own fields. Choosing *which* task a cluster should prefer is not in scope.
- No change to `pkg/claim`, to the lease constants, or to the fence/takeover arithmetic — spec 009 owns those and its tests pin them.
- No new metric. The closed 13-series `executor_claim_total` set already exists and is pre-initialised.
- No removal of the `cluster:` filter or the assigner. In `on` the claim is an additional gate; `cluster:` still runs first and still skips foreign tasks.
- No retry loop around the claim request. A failed claim is reported and the task is retried by the next dispatch pass that would have spawned it.

## Acceptance Criteria

All container-executable. Tests use the counterfeiter fake at `mocks/claim_ref_client.go` (`PutCallCount`, `DeleteCallCount`, `ListCallCount`, `PutArgsForCall`, `Invocations`) and a recording fake for the ref-client constructor. The in-process fake git-rest in `pkg/claim/fake_git_rest_test.go` is `package claim_test` and is not importable from `pkg/handler`; do not move it. Because `spawnIfNeededClaimed` and its `spawned` return are unexported and every existing `pkg/handler` test file is `package handler_test`, the ACs that call it are satisfied by a new `package handler` internal test file — **written with plain `testing` and `t.Run`, never Ginkgo**. Measured 2026-10-22: Ginkgo specs declared in an internal test package of this repo do **not** run — `go test ./pkg` reports 0 of the specs in `pkg/job_watcher_internal_test.go`, and `go test .` reports 0 of those in `main_internal_test.go`, despite the comment in the latter claiming otherwise. A Ginkgo file there compiles, passes, and asserts nothing. The alternative — anchoring on `ConsumeMessage` and the `jobSpawner` fake's recorded Jobs — is acceptable where an AC says so. Each AC names how a broken implementation is caught ("Discriminates").

- [ ] AC1 — **mode parsing.** An unset `CLAIM_MODE` yields `off`; `off`, `shadow` and `on` each parse to themselves; an explicitly empty `CLAIM_MODE` (`CLAIM_MODE=`), `ON`, `shadoww` and `true` each fail startup with an error naming the offending value and the accepted set `off|shadow|on`. Evidence: table-driven Go test over the parser; the startup-error test asserts `errors.Is` against a sentinel and that the message contains the rejected value. The parser distinguishes *absent* from *empty* — it reads the variable's presence, not only its value. Discriminates: a parser that lowercases its input fails the `ON` row; one that treats absent and empty alike fails either the unset row or the `CLAIM_MODE=` row; one with a `default:` that swallows an invalid value fails the `shadoww` row.
- [ ] AC2 — **`off` is inert (negative AC).** With `CLAIM_MODE=off`, a spawn through `spawnIfNeededClaimed` creates exactly one Job, the recording ref-client constructor was **never called**, the `ClaimRefClient` fake records `PutCallCount() == 0`, `DeleteCallCount() == 0` and `ListCallCount() == 0`, and `executor_claim_total` is unchanged on all 13 series. Evidence: `go test -race -count=1 ./...` exits 0; the test asserts the constructor recorder is empty, the three call counts are 0, and a per-series delta of 0. Discriminates: any wiring that constructs the ref client unconditionally fails the recorder assertion; one that guards only the `Claim` call fails it too, which is the point — `off` must not even build the client, so a misconfigured git-rest URL cannot fail an `off` executor.
- [ ] AC3 — **`shadow` never blocks.** With `CLAIM_MODE=shadow` and a ref client whose `Put` always answers `409` and whose `List` returns one claim for the key with a `lease_until` still in the future: a spawn still creates exactly one Job; `conflict` +1 and `takeover_too_early` +1 (the single takeover attempt against the live lease), every other one of the 13 series +0. With a ref client that answers `200` on `Put`: the Job is created, `claimed` +1, every other series +0, and the recorded claim's `expected_sha` is empty. Evidence: two Go tests, each asserting the Job count from the fake spawner and the exact counter delta on all 13 series, read from `PutArgsForCall(0)`. Discriminates: a wiring that gates on the claim result in both modes fails the `409` row (Job count 0); one that skips the claim in shadow fails the `200` row (`claimed` +0); one that retries a create after a `409` instead of attempting a takeover fails the `409` row's `conflict` delta (+2, not +1).
- [ ] AC4 — **`on` gates the Job.** With `CLAIM_MODE=on` and the same fixture as AC3's `409` row (a `List` returning one claim with a future `lease_until`): no Job is created, the returned `spawned` is false with no error, `conflict` +1 and `takeover_too_early` +1. With a ref client answering `200` on `Put`, exactly one Job is created and `claimed` +1. Evidence: same harness as AC3 with `CLAIM_MODE=on`; asserts `spawned` and the Job count from the fake spawner. Discriminates: a wiring that spawns before claiming fails the `409` row; one that claims after `SpawnJob` fails it too, because the fake spawner records a Job.
- [ ] AC5 — **claim error fails closed in `on`, open in `shadow`.** A ref client whose `Put` returns a non-`409` error: in `on` the fake spawner records **zero** Jobs, `spawned` is false, and `error` +1; in `shadow` the fake spawner records exactly one Job and `error` +1. Evidence: two Go tests asserting the fake spawner's Job count and the per-series counter delta — the Job count is the wiring-level assertion, since `error` is incremented inside `pkg/claim`. Discriminates: a wiring that treats any error as "proceed" fails the `on` row's Job count; one that treats it as a conflict in shadow fails the shadow row's Job count.
- [ ] AC6 — **takeover after an expired lease.** With `CLAIM_MODE=on`, a key whose listed `lease_until` passed more than the 30 s takeover grace ago and whose `Put` with the expired sha answers `200`: exactly one Job is created, `taken_over` +1, and the `PUT` body carries the expired claim's sha as `expected_sha` and this executor's cluster name. A `lease_until` still in the future, and separately a `lease_until` that passed 5 s ago (inside the grace), each yield no Job and `takeover_too_early` +1. Evidence: three Go tests reading the recorded `PUT` bodies via `PutArgsForCall`. Discriminates: a wiring that always creates (`expected_sha == ""`) fails the expired row; one that spawns without checking the lease fails both too-early rows; one that treats any past `lease_until` as takeable fails the 5 s row, because `Takeover` itself refuses inside the grace.
- [ ] AC7 — **the claim is taken after the existing gates.** With `CLAIM_MODE=shadow` and (a) a task whose `cluster:` names another cluster, (b) a task deferred because its assignee is at `maxConcurrentJobs`, and (c) a task suppressed by the redrive streak cap: `PutCallCount() == 0` in every case, and the existing skip counter for that path moves exactly as it does today. Evidence: three Go tests, each asserting zero claim calls and the pre-existing counter delta. A fourth case — a task whose Config cannot be resolved — is deliberately not asserted: that task is rejected in `resolveConfig` before `spawnIfNeededClaimed` is ever entered, so a zero-claim assertion there proves nothing. Discriminates: a wiring that takes the claim at the top of `spawnIfNeededClaimed`, or inside `ConsumeMessage` before `parseAndFilter`, fails all three rows — this is the spec's headline risk and the reason the claim sits between the last gate and `SpawnJob`.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — lint, vet and the full test suite exit 0
- `go test -race -count=1 ./...` — exits 0
- `grep -rn 'CLAIM_MODE' main.go pkg/` — returns the flag's declaration and every read site
- `grep -rn 'pkg/claim' --include=*.go pkg/ main.go | grep -v _test.go | grep -v mocks/` — the wiring is the only production importer

## Desired Behavior

1. The executor reads one `CLAIM_MODE` value at startup; `off` is the default and an unrecognised value stops the process before any loop starts, naming the value and the accepted set.
2. In `off` the executor constructs no claim client and issues no claim request; every counter series and every dispatch decision is what it is today.
3. In `shadow` and `on`, a dispatch attempt takes a claim immediately before the Job is created — after every existing gate (foreign cluster, active Job, recent success, redrive streak, concurrency cap, trigger budget) — so a claim is never held for a task a gate would have skipped.
4. In `shadow` the claim outcome is recorded and the Job is created regardless of it; in `on` a `claimed` outcome is the precondition of the Job and any other outcome skips this attempt.
5. In `shadow` and `on`, a `409` conflict is followed by one takeover attempt against the listed claim: a still-valid lease skips the attempt, an expired lease is taken over with a single compare-and-swap write. In `on` a successful takeover is what lets the task proceed; in `shadow` the spawn happens either way and only the counters differ. Shadow takes the same path deliberately — it is the mode that proves the protocol before `on` is allowed to gate on it.
6. Every outcome above is recorded on the existing `executor_claim_total` series and nothing else — no new metric, no new label value.

## Constraints

- `pkg/claim` is called, never modified. Its constants, its fence arithmetic and its tests are frozen by spec 009.
- The claim is taken inside `spawnIfNeededClaimed` (`pkg/handler/task_event_handler.go:693`) between the last gate and `h.jobSpawner.SpawnJob` (`:775`). That function is the single funnel all three spawn paths use (`ConsumeMessage` `:368`, `evalDeferredRespawns` `task_event_handler_deferred.go:95`, `driveCandidate` `:1348`); wiring anywhere else misses a path.
- The claim call runs under the existing per-assignee spawn lock (`defer h.lockAssigneeSpawn(...)` at `:727`). The claim package caps each request at 30 s, so the worst case a sibling spawn waits is 30 s; taking the claim outside the lock is rejected because it lets two spawns of the same task interleave between claim and Job creation.
- The ref client is built by an injected constructor function, not inline, so `off` can be proven to have skipped construction. The wiring calls that constructor at most once per process.
- The `cluster:` filter runs before the claim and is unchanged. In `on` a task this cluster is not allowed to run is skipped before any claim request is made.
- Keys come from `claim.DeriveClaimKey` with the task's type, id, owner and repo. The wiring invents no key. Owner and repo are read from the task's single `repo` frontmatter key, which carries `<owner>/<name>` (verified against real tasks, e.g. `repo: bborbe/github-update-go-agent`); there is no separate `owner` key in the vault format. Split on the first `/`. A repo-keyed task whose `repo` key is missing, has no `/`, or has an empty side cannot have its key derived: in `on` it is skipped and counted `error`, in `shadow` it spawns with no claim. A task type that is not repo-keyed never reads `repo`.
- The ref client's base URL and gateway headers reuse the executor's existing git-rest configuration; no new endpoint configuration and no new secret.
- One `LeaseManager` per process, constructed once, with the executor's `CLUSTER` value. Two processes sharing a cluster name are outside this protocol and stay outside it.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection |
|---|---|---|---|
| git-rest unreachable in `on` | no Job is created; `error` +1; the task is retried by the next dispatch pass that would have spawned it | the pass after git-rest returns claims normally; no operator action | `executor_claim_total{result="error"}` rising with no `claimed` |
| git-rest unreachable in `shadow` | the Job is created and `error` +1; the claim is absent for that Job | next pass re-claims if the task is re-driven | `executor_claim_mismatch_total` rising during the outage (lifecycle spec) |
| Two tasks of one repo-bound type in `on` | the second gets `conflict`, creates no Job, and is retried on a later pass | the first Job's release frees the key | `conflict` rising on a repo whose first Job is still running |
| Two executors race one expired key | each sends one compare-and-swap `PUT` with the expired sha; exactly one gets `200` and spawns, the other gets `409`, counts `conflict` and skips | the loser retries on a later pass | `taken_over` and `conflict` rising together |
| Two spawns of one assignee interleave | the per-assignee spawn lock serialises them, so claim and Job creation cannot interleave | none needed | a duplicate Job for one task id |
| git-rest throttles claim writes | the request fails as a non-`409` error, which in `on` means no Job and `error` +1 | the next pass retries; a sustained throttle stalls dispatch for that cluster and is visible as `error` without `claimed` | `error` rising at a steady rate with `claimed` flat |
| Takeover race lost | the losing executor's `PUT` answers `409` and it skips | the winner owns the task | `conflict` rising alongside `taken_over` |
| Clock skew beyond the fence margin | the fence fires early and the Job stops before a peer may take over | the task is re-driven by whichever cluster holds it | `fenced` rising without `claim_lost` |

## Security / Abuse

The claim endpoint is the existing git-rest API, authenticated with the executor's existing gateway secret; this spec adds no credential and no new network destination. Claim keys are built from task fields, and `DeriveClaimKey` already rejects `..`, `/`, a leading `.`, a trailing `.lock` and any character outside `[A-Za-z0-9._-]`, so a task cannot steer a claim outside the `claims/` namespace. In `on` the claim is a gate, not a grant: it can only prevent this executor from spawning, never make it spawn something the `cluster:` filter and Config resolution would not already allow.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | `CLAIM_MODE` parsing, validation and the injected ref-client constructor (nothing constructed in `off`) | 1, 2, 6 | 1, 2 | — |
| 2 | Claim gate in `spawnIfNeededClaimed`: claim after the last gate and before `SpawnJob`, shadow never blocks, `on` gates, error handling | 3, 4, 6 | 3, 4, 5, 7 | prompt 1 |
| 3 | Takeover on `409`: list, compare `lease_until`, single compare-and-swap `PUT`, `takeover_too_early` when the lease is live | 5, 6 | 6 | prompt 2 |

Rationale: prompt 1 is pure configuration and lands an inert change on its own. Prompt 2 is the whole behavioural risk and is testable against the counterfeiter fake without any takeover logic. Prompt 3 needs prompt 2's conflict branch to exist. Keep each prompt's PR under the 1200-added-line review limit; prompt 2 is the only one likely to approach it, and its tests are the part that can be split off if it does.

## Do-Nothing Option

`pkg/claim` stays dead code. The executor keeps deciding from `cluster:` alone, so two clusters that both match a task still start two Jobs, per-repo tasks still overlap, and the goal's success criteria stay unobservable. The library's tests keep passing while proving nothing about the running system — and the longer it sits unwired, the more its protocol drifts from what dispatch actually needs.
