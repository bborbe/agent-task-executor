---
status: draft
created: "2026-10-26"
---

## Summary

- The executor's only task-content source is the compacted Kafka view, and the reconcile floor reads that same projection — so a task whose surviving record is stale is re-driven at that stale phase forever, and nothing inside the process can notice.
- Measured on dev 2026-10-26: 16 of 7,232 owned tasks are capped and re-driven every 60 s, and 3 of those 16 are `completed` or `aborted` on disk — the executor is actively trying to spawn finished work once a minute.
- Costing the three obvious shapes against the 45 s pass budget ruled out all of them: a full file sweep is 2 h 49 min per pass, a per-candidate file re-read adds 22.4 s to a budget that already aborts at 16 candidates, and a producer-side assertion cannot run inside the executor.
- The adopted shape is one index read per reconcile pass, compared against the view snapshot: a single call plus a scan over a 9,002-file index that vault-api already refreshes incrementally every 30 s at 20–65 ms.
- The check reports divergence and the index age, never repairs: the view is the suspect, and the vault is not written from a detector.

## Problem

`ReconcileOnce` snapshots the in-memory view and drives every eligible task from it. The view is filled by a Kafka consumer and by nothing else, so when a producer publishes a snapshot it read *before* an agent's phase write landed, the view keeps that stale record indefinitely — the topic is compacted, and the reconcile floor re-reads the same projection rather than the vault.

The consequence is not cosmetic. The redrive cap is keyed on the projection's phase, and its reset is a phase *transition* the projection never delivers, so a task capped at a phase it left hours ago can never un-cap. On dev that is 16 tasks re-driven every minute, 3 of them already terminal on disk. Every dashboard reading `evaluated`, `deferred`, `re_driven` or `capped` is reading the stale view, so a shipped fix reads as working when it is not.

Nothing in the process can currently tell the difference between "the vault agrees with the view" and "the view is all we have". This spec adds that distinction.

## Goal

A running executor can state, for the tasks it owns, which of them the vault disagrees with about `status` or `phase` — from inside the process, without a human comparing two sources by hand — and it states how many ids it actually compared and how stale the comparison's source was, so a failed read cannot pass as agreement.

## Non-goals

- **Replacing the view as the task-content source.** The view stays the sole input to eligibility and driving. This spec adds a detector, not a second source of truth.
- **Repairing divergence.** The check reports; it never writes to the vault, never re-publishes, and never alters the view. The view re-reads disk on its own refresh, and the defect is on the cached side.
- **Fixing the producer.** The stale-snapshot publish is a separate defect with its own owner. This spec detects the symptom it leaves behind.
- **Classifying why a file is missing from the index.** The list envelope carries a global `warnings` count, not a per-file reason, so this spec reports the count and leaves per-file diagnosis to an operator probe (see Failure Modes).
- **Clearing redrive refs.** Operator-only, and clearing them would re-fail the same rows.
- **Prod rollout.** Dev first; a prod deploy is its own decision.

## Assumptions

- vault-api refreshes its task index incrementally on a 30 s default, so a per-pass read returns warm data rather than paying a full re-index.
- The personal vault's `25 Tasks/` holds 9,002 files; the response therefore carries every task in the vault, and the executor must not narrow it (see Constraints).
- The view snapshot is the only source of the executor's owned-id set. This spec does not introduce a second one.

## Acceptance Criteria

- [ ] **Divergence is reported from inside the executor, and only for ids that diverge.** One reconcile pass against a stale view emits a log line naming each divergent task id with both sides (`view` and `vault` `status` + `phase`), and emits no such line for a task whose file agrees with the view in the same pass.
  - evidence: log line matching `event=view_divergence task=<id>` with `view_status=`, `vault_status=`, `view_phase=`, `vault_phase=` present; and `grep -c 'event=view_divergence task=<agreeing-id>'` returns 0 over the same window
- [ ] **The check fires on a staged divergence, for both fields independently, not only on the dev fixture.** A `DescribeTable` over an `httptest` index serves the same fixed 3-id owned set in every entry, with exactly one divergent id: (a) that id's `status` differs and its `phase` matches, (b) its `phase` differs and its `status` matches, (c) both match. Entries (a) and (b) each emit `event=view_divergence task=<synthetic-id>` carrying both sides; entry (c) emits no line for that id.
  - evidence: the integration test's captured log output contains the line for `<synthetic-id>` under entries (a) and (b) and not under entry (c); and the summary line reads `compared=3 divergent=1` under (a) and (b), `compared=3 divergent=0` under (c)
- [ ] **The pass states its own coverage and the age of its source.** Each pass that reaches the check emits one summary line carrying the number of ids compared, the number divergent, the index age, and the index `warnings` count — so a pass that compared nothing is distinguishable from a pass that found nothing.
  - evidence: in the AC 2 fixture, whose owned set is fixed at 3 ids with exactly 1 divergent, the captured output contains `event=view_divergence_summary compared=3 divergent=1` and carries `index_age_seconds=` and `warnings=` fields
- [ ] **The check is bounded to one call per pass that reaches it, and does not block a spawn on a read error.** The check issues exactly one `GET /api/v1/tasks` per such pass regardless of task count; passes that return before the check (`view_not_ready`, `task_vault_unstamped`, `task_vault_mismatch`) and a pass whose context has already expired make none. When the call fails, the pass still drives its candidates to completion.
  - evidence: over a healthy window containing no `event=reconcile_aborted` line, `sum(executor_view_divergence_total{result=~"ok|read_error"})`, the count of `event=view_divergence_summary` lines and the count of `event=reconcile evaluated=` lines are all equal; and with the configured vault-api URL pointed at a closed port, the pass logs `event=view_divergence_read_error` while `event=reconcile evaluated=… candidates=` is unchanged from the healthy pass
- [ ] **A pass with no vault-api URL makes no call.** In a deployment that holds no claim, `VAULT_API_URL` is empty and the executor already refuses to construct a vault-api client without one. One `ReconcileOnce` then emits no `event=view_divergence` line and increments no `executor_view_divergence_total` series.
  - evidence: `grep -c 'event=view_divergence'` returns 0 and `increase(executor_view_divergence_total[5m]) == 0` for every series over the window
- [ ] **Post-Deploy (Rung-2):** the check fires on the dev deployment against the three known-divergent ids, and every id it reports resolves to a file that actually disagrees.
  - evidence: `kubectlnukedev -n agent logs deploy/agent-task-executor-personal --since=10m | grep 'event=view_divergence task='` lists at least one of `b75217de-ffd7-5167-a7fb-09bdaacf8484`, `c8432e1d-9c3a-575c-9757-3f25eca5df88`, `f9aea3d2-922a-5c88-81fe-1027c0561783`; and for every id it lists, `grep -rl 'task_identifier: <id>' "25 Tasks/"` resolves to one file whose `^status:` / `^phase:` differ from that line's `vault_status=` / `vault_phase=`
  - `deploy_check:` `kubectlnukedev -n agent get deploy/agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `$(git describe --tags --abbrev=0)`

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — format, generate, test, check, addlicense all clean
- `go test -mod=mod -count=1 ./pkg/handler/ ./pkg/metrics/ ./pkg/claim/` — the check's unit and integration tests and the index client's own tests pass
- `grep -n 'event=view_divergence' pkg/handler/*.go` — the check emits the documented event names
- `grep -n 'view_divergence' pkg/metrics/metrics.go` — the counter is declared with a closed label set

### Operator-executable (runs on the host after PR merge)

- `make test` — full suite green
- `kubectlnukedev -n agent get deploy agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}'` — dev runs the released tag

## Desired Behavior

1. **One index read per pass that reaches the check.** When a vault-api client is available, `ReconcileOnce` fetches the task index once, under the pass context, before the drive loop. The fetch is lazy in the same sense as the existing streak cache: a pass that returns early — view not ready, unstamped, or mismatched — never reaches it and pays nothing. The read is additionally preceded by the same `passDone` guard the drive loop uses, so a pass whose context has already expired makes no call either.
2. **Comparison over the owned set.** The check joins the index response's `items[].id` to the view snapshot's owned tasks by their `task_identifier`, and compares **both** `status` and `phase`. A difference in either field is a divergence; matching on one field while differing on the other is still a divergence, and the per-id line carries both sides of both fields so the reader can see which one moved.
3. **Absence has exactly two per-id outcomes.** An owned id either joins the index or it does not. A non-joining id is reported as `absent` and counted in the `absent` series. The response's `warnings` count is carried in the summary as a coverage signal — it is a global count of files vault-api could not index, not attributable to a specific id from this response, so it is never reported per id. (An operator distinguishes "file missing" from "file unindexable" with the probe in Failure Modes; the pass does not.)
4. **Both sides are named.** A divergence line carries the view's `status` and `phase` and the vault's `status` and `phase`. It carries no task title, no assignee and no frontmatter body.
5. **Agreement is silent.** An id whose `status` and `phase` both match produces no per-id line. Silence per id is the control that makes the check discriminating.
6. **The pass reports its coverage.** One summary line per pass that reaches the check carries `compared`, `divergent`, `index_age_seconds` and `warnings`. `compared` counts ids actually joined, not owned ids, so a failed or empty index read reads as `compared=0` rather than as agreement.
7. **A read failure is a finding, not a pass failure.** A failed, throttled, oversized or unparseable index read logs `event=view_divergence_read_error`, increments its own metric series, and returns to the pass without touching the candidate set. `ReconcileOnce` continues to return `nil` for it, so the loop can never tear the executor down through `service.Run`.
8. **No URL, no client.** A deployment that holds no claim carries no `VAULT_API_URL`, and the executor already refuses to build a vault-api client without one. The check therefore cannot run there, and emits nothing.

## Constraints

- **The view remains the only driving input.** Eligibility, admission order, capping and spawning are untouched. The check runs alongside them and feeds nothing back into them.
- **The pass budget is not extended.** The check runs under the existing `handler.DefaultReconcilePassTimeout` (45 s). It must not add a per-task round trip; the single index call replaces the possibility of N reads rather than adding to them.
- **Reuse the existing vault-api client conventions — do not build a second auth path.** `GET /api/v1/tasks` sits behind `gatewaySecretMiddleware`, which compares the `X-Gateway-Secret` header. The repo's client already sets that header, and its initiator header is misspelled `X-Gateway-Initator` in `pkg/claim/vault_api_client.go`; a new client that "corrects" the spelling or omits the pair gets `401` on every pass and produces a check that is silently dead. Reuse `pkg.ReadVaultAPIGatewaySecret` and the same header pair. See `docs/claim-lease-protocol.md` for the surrounding conventions.
- **Do not filter the index request.** `GET /api/v1/tasks` with no query returns every task in the vault. The tempting `cluster=` filter must be rejected: files carrying no `cluster` frontmatter are not in the cluster set, so a filtered read would manufacture `absent` divergences for exactly the tasks most likely to be stale.
- **Bound the response body.** The index response is untrusted input; read it through an `io.LimitReader` capped at 32 MiB, and treat an over-limit or unparseable body as `read_error`, not as an empty index.
- **Index age is server-computed.** `index_age_seconds` comes from vault-api, so the executor never compares two clocks. Do not "improve" it into a client-side timestamp.
- **One counter, and its increment unit is fixed.** `executor_view_divergence_total{result}` with the closed label set `ok`, `mismatch`, `absent`, `read_error`. `ok` and `read_error` increment once per pass that reaches the check; `mismatch` and `absent` increment once per id. An alert written against one reading is wrong under the other, so the unit is part of the contract.
- **No task id as a metric label value.** Keys and task ids are never label values, mirroring `ClaimTotal`'s existing rule.
- **Do not reuse the existing vault-ownership label values.** `executor_reconcile_passes_total` already carries `task_vault_mismatch` and `task_vault_unstamped`; those describe vault-ownership classification over the in-memory snapshot, not a per-id file comparison. A new metric name is required so a dashboard cannot silently merge two different failures.
- **Errors wrap.** Use the repository's error conventions; no `fmt.Errorf`, no bare `return err` on a new path.
- **Time via injection.** Any timestamp the check emits comes from the injected `CurrentDateTimeGetter`, never `time.Now()`.
- **Tests are Ginkgo `DescribeTable` / `Entry`.** Package suites call bare `RunSpecs`; the suite timeout is configured once for the root package in `main_test.go`, not per package.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection |
|---|---|---|---|
| vault-api unreachable or times out | `event=view_divergence_read_error`, `read_error` series increments, pass continues and drives every candidate | next pass retries; the check is advisory | the `read_error` series rising |
| vault-api returns `429` | `read_error`, never a silent zero-divergence pass | next pass retries at the normal cadence; no client-side backoff loop is added | `read_error` rising in bursts |
| Index returns an empty task list | summary reads `compared=0 divergent=0`, which is not agreement | `kubectlnukedev -n dev get --raw '/api/v1/namespaces/dev/services/vault-api-personal:9090/proxy/readiness'` returns `"ready":true` with `index_age_seconds` below 60 | `compared` at 0 while `owned` is non-zero |
| Index is stale (refresh stopped) | `index_age_seconds` in the summary grows past the 30 s refresh interval | restart vault-api, then re-read `/readiness` and confirm `index_age_seconds` drops below 60 | `index_age_seconds` monotonic growth |
| Response envelope changes shape (field renamed, `items` nested, `index_age_seconds` dropped) | parse failure is `read_error`; never a silent empty index | pin the envelope in `docs/vault-api-task-index.md` and fix the client | `read_error` rising while `ok` stays flat |
| Owned id absent from index | reported as `absent`, `absent` series increments per id | distinguish the two causes with `kubectlnukedev -n dev get --raw '/api/v1/namespaces/dev/services/vault-api-personal:9090/proxy/api/v1/tasks/<id>'` — `422 invalid_frontmatter` means the file exists but was excluded, `404 not_found` means no file | the `absent` series and the per-id line |
| Index `warnings` non-zero | carried in the summary; not attributed to any id | same 422-vs-404 probe on the ids reported `absent` | `warnings` non-zero |
| Response body over the 32 MiB bound or unparseable | `read_error`, no partial comparison | inspect the vault-api response size | `read_error` rising while `ok` stays flat |
| No `VAULT_API_URL` (non-claim deployment) | no client, no call, no line, no series | none needed | absence of all of the above |

## Security / Abuse

- The check sends no task content to vault-api; it reads. The request carries only the gateway auth header the deployment already uses.
- The check logs task identifiers, statuses and phases only — never titles, assignees, or frontmatter bodies.
- The index response is untrusted input, bounded at 32 MiB. A parse failure is a `read_error`, not a panic.
- No new credential is introduced. `VAULT_API_GATEWAY_SECRET` is already read through `pkg.ReadVaultAPIGatewaySecret`; the value is never logged, never placed in env, and never written to the image.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Vault-api index client **and its contract doc**: one bounded `GET /api/v1/tasks`, typed response, `io.LimitReader` bound, error wrapping, reuse of `X-Gateway-Secret` / `X-Gateway-Initator`, no client without a URL; pin the envelope field set (`items`, `total`, `warnings`, `index_age_seconds`) against the vault-api version it was read from and record the `cluster=` filter trap, at `docs/vault-api-task-index.md` | 1, 8 | 5 | — |
| 2 | The comparison: join view snapshot to index, compare **both** `status` and `phase`, report per-id divergence, classify non-joining ids as `absent`, emit the summary with `compared` / `divergent` / `index_age_seconds` / `warnings` | 2, 3, 4, 5, 6 | 1, 2, 3 | prompt 1 |
| 3 | Failure isolation and metrics: `read_error` path including `429` and parse failures, closed label set, per-pass vs per-id increment unit, pass-continues-on-error | 7 | 4 | prompts 1, 2 |
| 4 | Tests: the three-entry `DescribeTable` fixture (status-only, phase-only, agreeing), fixed owned-set counts for the summary assertion, `absent` classification, read-error isolation, no-URL no-op | — | 1, 2, 3, 4, 5 | prompts 1–3 |

## Do-Nothing Option

The 16 capped ids keep being re-driven every minute, 3 of them finished work the executor is trying to spawn again. The cap never clears because its reset is a transition the view never delivers, so the condition is permanent rather than self-correcting. Every "is it fixed?" reading taken from the executor's counters stays corrupted, which is how a shipped filter fix already read as working when it was not. The sibling row that depends on this one — comparing the capped set against task files — cannot close, because the comparison it needs is the one this spec builds.
