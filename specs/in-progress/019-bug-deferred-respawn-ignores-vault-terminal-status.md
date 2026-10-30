---
status: verifying
approved: "2026-10-29T23:20:13Z"
generating: "2026-10-29T23:20:14Z"
prompted: "2026-10-29T23:41:47Z"
verifying: "2026-10-30T00:18:44Z"
branch: dark-factory/bug-deferred-respawn-ignores-vault-terminal-status
---

## Summary

- The executor's in-memory task view can disagree with the vault and cannot self-heal: on 2026-10-29 the sun executor held 242 divergent task ids in a single pass (`compared=15237 divergent=242 index_age_seconds=37`).
- The duplicate runs come from the **deferred-respawn** path, not the reconcile gather: a task whose vault status is `completed` while its view still looks workable is seeded at startup, then re-spawned every grace window, forever.
- Measured: task `06ac512f-5f41-5bc3-adc1-6a9b7b80c929` logged `view_status=in_progress view_phase=execution vault_status=completed vault_phase=human_review` and started twice 14 minutes apart. The **view** phase is `execution` — non-terminal, and the phase the seed actually reads; `human_review` is the **vault** phase.
- The fix makes the vault index **gate** spawn eligibility on the three spawn paths that decide from the view. The reconcile candidate gather reuses the read that pass already performs for the advisory divergence check; the deferred evaluation adds one read of its own per evaluation pass; the seed needs none, because it gates on the view it already holds.
- The change is fail-open (no client, failed read, or spent pass budget suppresses nothing), bounded in log volume (20 names per pass, id-sorted) and uncapped in its counter.

## Problem

The executor replays every task's `status` and `phase` from Kafka into an in-memory view, and three spawn paths decide eligibility from that view: the startup seed that re-parks in-flight work, the deferred-respawn evaluation that re-spawns it when the grace window elapses, and the reconcile floor that re-drives orphaned tasks. The vault is the authority for a task's lifecycle, and the view is a lagging copy of it that cannot be corrected in place — so a task the vault has finished can still look workable to the executor indefinitely. Nothing on the deferred path consults the vault, and the only component that compares the two (`checkViewDivergence`) is explicitly advisory: it reports and changes nothing. The result is unbounded duplicate execution of work the vault already recorded as done, on one cluster, with no error anywhere: on 2026-10-29 the sun executor carried 242 divergent ids in one pass and spawned 164 distinct tasks on the day, with 7 cross-cluster post-success duplicates over 162 ids inside a 73-minute window.

## Goal

A task the vault reports as terminal is never spawned by any path that decides from the in-memory view. The deferred seed refuses it, the deferred evaluation refuses it, and the reconcile candidate gather refuses it — the gather reusing the reconcile pass's existing index read, the deferred evaluation performing its own single read. Every refusal is visible: one id-sorted `event=index_terminal_suppressed` line per id per pass (capped at 20 names per pass) and one increment of `executor_reconcile_refused_total{gate="index_terminal"}` per suppressed id (uncapped). When the index cannot be read, the executor behaves exactly as it does today.

## Non-goals

- **Do NOT repair the stale view.** The divergence check keeps reporting the same divergences after this change; `divergent=` is not this change's success metric, and any attempt to converge the view belongs to a separate spec.
- **Do NOT touch the cross-cluster suppression guard's `claim_probe_failed` path.** It is a different mechanism that fails **closed** — a failed claim read suppresses the spawn and retries on the next pass — while this gate fails open. Conflating the two would invert one of them.
- **Do NOT change how the vault writes or validates task frontmatter, and do NOT touch the controller's `MergeFrontmatter` ownership guard.**
- **Do NOT change the fail-open direction for an unreadable index.** A total index outage spawns exactly as today; closing that would trade a duplicate-run class for a fleet-wide stall.
- **Do NOT add a config knob, flag or opt-out for this gate.** It is an invariant, not a tunable; a per-deployment escape hatch would re-open the duplicate it closes.
- **Do NOT gate the Kafka event path.** It stays ungated: it is the recovery route for a task that genuinely re-opens (see Failure Modes).

## Assumptions

- The vault index served by `GET /api/v1/tasks` is the authority for `status` and `phase`, and it reports the same canonical vocabulary the view normalizes to. `docs/vault-api-task-index.md` records this as an assumption rather than a contract, and it stays one.
- A deployment that carries no `VAULT_API_URL` builds no index client. Such a deployment gets no gate and keeps today's behaviour — it also emits no divergence check, so it has no evidence to act on.
- The cluster that measured this defect (sun) is a vault-api-backed executor deployment. The runtime replay below names observables rather than a pod name, because the deployment name is read at replay time from the operator's cluster.

## Reproduction

Live, on the sun cluster, 2026-10-29. Task `06ac512f-5f41-5bc3-adc1-6a9b7b80c929` logged:

```
view_status=in_progress view_phase=execution vault_status=completed vault_phase=human_review
```

and started twice, 14 minutes apart:

```
github-update-go-agent-06ac512f-20261029223657   Complete
github-update-go-agent-06ac512f-20261029225058   Running
```

The seed admits this task because the seed reads the **view** phase, which is `execution` — non-terminal. `human_review` is the **vault** phase and is never consulted by the seed. The view status `in_progress` is likewise workable.

Task `3c7b9b53` did the same at 22:22:45 and 22:45:45. Task `153b4dcd-82a0-5501-9a06-77094c2fb21b` held `view_status=in_progress / view_phase=ai_review` against `vault_status=completed / vault_phase=done`, logged `event=view_divergence` 66 times over 75 minutes with zero convergence, and then spawned.

The pass summary at 22:00:04 local on sun:

```
compared=15237 divergent=242 index_age_seconds=37
```

The day's totals: 164 distinct tasks spawned; a 73-minute window scored 7 cross-cluster post-success duplicates over 162 ids.

The spawning path is identified, not inferred: `grep -icE 'reconcile|redrive'` over sun's executor log for the divergent id returns **0**, so the spawn cannot have come from the reconcile gather. It came from `respawn_after_grace_window`, emitted by the deferred-respawn evaluation, which is driven by its own ticker and never passes through the reconcile candidate gather.

Smallest reproduction at the code seam (this is what the regression tests below encode):

1. A task store holding one task with `current_job` set, `job_started_at` older than the respawn grace window, view `status: in_progress`, view `phase: execution`.
2. A vault index whose entry for that id reads `status: completed` (vault phase `human_review`).
3. Start the deferred-respawn loop.

Observed before the fix: one Job spawned for that id. Expected: no Job, one `index_terminal_suppressed` line, one counter increment.

Reproduce with:

```
go test ./pkg/handler/ -count=1
```

against the new suites named in Acceptance Criteria 2, 3 and 5; the same suites are run against `v0.49.1` to prove they fail on the pre-change commit.

## Expected vs Actual

- **Expected:** a task whose status is terminal is never a spawn candidate. This invariant is stated on the view side (`terminalStatuses` in `pkg/handler/task_event_handler.go`, and `IsTerminalStatus`'s own comment: *"A terminal task must never be a candidate: its file says the work is over"*), and `docs/vault-api-task-index.md` exists precisely because the vault is the authority for `status` and `phase`.
- **Actual:** the invariant holds only on the path that implements it. The terminal-status check sits in the Kafka event filter, and the deferred seed skips a terminal **phase** but not a terminal **status**; the funnel the deferred path spawns through applies neither check. A task whose view is `in_progress / execution` and whose vault is `completed / human_review` is therefore seeded and re-spawned on every grace window, and the reconcile gather is never reached from that path.

## Why this is a bug

The terminal-status rule is unconditional where it exists — its comment says a terminal task admitted as a candidate is capped "on every pass, forever" — yet the deferred path bypasses it entirely: the Kafka event filter is the only place the view-side status gate runs, and `evalDeferredRespawns` calls the spawn funnel directly, which applies no status or phase gate at all. The vault is documented as the authority for a task's lifecycle, and the executor already reads the vault's answer once per pass; it simply discards it after reporting. A task file that says the work is over is therefore re-executed indefinitely, which is the one outcome the terminal set exists to prevent. Nothing else catches it: the claim lease does not help (the duplicate is post-success on the same cluster, and the success hold is bounded by its grace window), the re-drive cap does not help (it charges a streak, it does not read the vault), and the divergence check does not help (advisory by its own contract).

## Acceptance Criteria

- [ ] `make precommit` exits 0 in the repository root — evidence: exit code.
- [ ] **Reconcile path, three eligible ids:** a fixture of three ids — each stamped `Vault: testTaskVault` (the constant `task_event_handler_test.go` already declares, so `v0.49.1` has it too; without the stamp `ownedTasks` and `skipForeignVaultTask` drop all three and every fixture spawns nothing), each with view `in_progress / execution` and no `current_job`, and a Config listing `in_progress`, `completed` and `aborted` in `trigger.statuses` and `execution` in `trigger.phases` — driven through `ReconcileOnce`, where the index reports `completed` for the first, `aborted` for the second and a matching `in_progress / execution` for the third, spawns exactly one Job and it is the third — evidence: new Ginkgo suite in `pkg/handler/`, test exits 0, `SpawnJobCallCount() == 1`, and the assertion carries the spawned ids (`Expect(...).To(Equal(1), "spawned ids: %v", ids)`).
- [ ] **Deferred path, three eligible ids:** the same three ids and Config, each additionally carrying `current_job` set and a `job_started_at` older than the respawn grace window — without both the seed parks nothing and the deferred path is never exercised — driven through the startup seed and one deferred evaluation pass, spawns exactly one Job and it is the third — evidence: new Ginkgo suite in `pkg/handler/`, test exits 0, `SpawnJobCallCount() == 1`, assertion carrying the spawned ids.
- [ ] **Both new suites and the internal seed-gate spec fail on the pre-change commit, for the behavioural reason** — evidence: `git worktree add /tmp/atx-prefix v0.49.1`, copy the three new test files in (the two suites plus the internal seed-gate file), `go test ./pkg/handler/ -count=1` exits non-zero, the failure messages list the spawned ids (that list contains at least one terminal id) or the seeded deferred entry, and `grep -cE 'build failed|undefined:|cannot use'` over the output returns 0 (a compile failure does not satisfy this criterion).
- [ ] **Seed gate, view-side status — never parked, not merely never spawned:** a fixture with `Vault` stamped to the executor's own vault, view `status: completed`, view `phase: execution` (non-terminal and in `defaultTriggerPhases`), `current_job` set, `job_started_at` older than the grace window, a Config listing `in_progress`, `completed` and `aborted`, and no index client. After the startup seed the task is **absent from the deferred set**, `SpawnJobCallCount() == 0` after one evaluation pass and again after a second, and on `v0.49.1` the same fixture spawns exactly one Job — evidence: internal Ginkgo spec in package `handler` (where `deferredRespawns` is visible), test exits 0, `len(h.deferredRespawns) == 0` and no entry for that id after the seed, `SpawnJobCallCount() == 0` after both passes, and 1 under the `v0.49.1` run in the criterion above. The map assertion is load-bearing: a status check added inside `spawnIfNeededClaimed` also yields zero spawns, but leaves the task parked and would gate the Kafka path — which the Constraints forbid.
- [ ] **Fail-open, no client — both paths:** with no index client configured, an otherwise eligible task spawns on the reconcile path, and an otherwise eligible due entry spawns on the deferred path — evidence: two Ginkgo specs, test exits 0, `SpawnJobCallCount() == 1` in each.
- [ ] **Fail-open, failed read — reconcile path:** with an index `Read` returning an error, an otherwise eligible task still spawns and `executor_view_divergence_total{result="read_error"}` increases by 1 — evidence: Ginkgo spec, test exits 0, `SpawnJobCallCount() == 1`, `testutil.ToFloat64` delta of 1.
- [ ] **Fail-open, failed read — deferred path:** with an index `Read` returning an error, an otherwise eligible due entry still spawns; the deferred loop runs no divergence check, so no `read_error` series moves and no log line is expected — evidence: Ginkgo spec, test exits 0, `SpawnJobCallCount() == 1`, and `executor_view_divergence_total{result="read_error"}` delta of 0 across that pass.
- [ ] **No over-suppression:** a task whose index entry is non-terminal but **differs** from the view (`index in_progress / execution` against `view in_progress / ai_review`) is still driven and spawns — evidence: Ginkgo spec, test exits 0, `SpawnJobCallCount() == 1`. This case passes with and without the fix by design: it fails only if the gate suppresses divergence instead of terminality, and it is the guard against that shortcut.
- [ ] **The divergence check still reports:** one non-terminal divergent id in a reconcile pass emits exactly one `event=view_divergence task=<id>` line and increases `executor_view_divergence_total{result="mismatch"}` by 1 — evidence: Ginkgo spec with a captured stderr, `strings.Count(out, "event=view_divergence task=") == 1`, `testutil.ToFloat64` delta of 1. This is the guard against deleting the divergence read to satisfy the read budget.
- [ ] **Log line, per pass, sorted, capped:** one pass suppressing 25 ids emits exactly 20 `event=index_terminal_suppressed task=<id>` lines, naming the 20 lexicographically lowest ids and none of the 5 highest, and a second identical pass emits 20 lines again — evidence: internal test capturing stderr through the pipe pattern the refusal test already uses (`captureRefusalLog` in `pkg/handler/task_event_handler_reconcile_refusal_internal_test.go`), `strings.Count(out, "event=index_terminal_suppressed task=") == 20` per pass, and the 5 highest ids absent from the captured output.
- [ ] **Counter, uncapped:** the same 25-id pass increases `executor_reconcile_refused_total{gate="index_terminal"}` by exactly 25 — evidence: `testutil.ToFloat64` delta of 25 in the same test.
- [ ] **The series is registered at zero, before any refusal:** `gatherLabels(mfs, "executor_reconcile_refused_total", "gate")` in `pkg/metrics/metrics_test.go` reads the default gatherer without creating any series, so its `ConsistOf` is the non-vacuous pre-initialisation proof. That spec currently asserts exactly the eight existing gates and its per-gate zero loop covers eight, so it fails until it is updated — and it takes AC1 with it. The change must extend both to nine, listing `index_terminal` — evidence: the `pkg/metrics` spec exits 0, and `grep -c 'index_terminal' pkg/metrics/metrics_test.go` returns ≥1. A `testutil.ToFloat64(....WithLabelValues(...)) == 0` assertion does not satisfy this, because `WithLabelValues` creates the series at 0 by itself.
- [ ] **The gate's documentation is correct:** the comment no longer closes the set at eight, the help string covers both paths, and the stale phrase is gone — evidence: `grep -c 'eight gates' pkg/metrics/metrics.go` returns 0, `grep -c 'all eight series' pkg/metrics/metrics.go` returns 0, `grep -c 'and the deferred-respawn path' pkg/metrics/metrics.go` returns 1.
- [ ] **One index read per pass, per path:** (a) a reconcile pass in which the candidate gate suppresses an id and the divergence check also consumes the index performs exactly one `Read`; (b) a reconcile pass that returns at `view_not_ready` performs zero; (c) one deferred evaluation pass with three due entries performs exactly one `Read`; (d) one deferred evaluation pass with no due entries performs zero — evidence: a counting index stub, four specs, each exits 0 asserting the exact call count.
- [ ] **The index document states the gate:** the advisory claim is gone, the read scope is stated per path, and the metric contract carries a subsection for the new gate — evidence: `grep -c 'changes no eligibility' docs/vault-api-task-index.md` returns 0, `grep -c 'index_terminal' docs/vault-api-task-index.md` returns ≥2, `grep -c 'index_terminal_suppressed' docs/vault-api-task-index.md` returns ≥1, `grep -c 'one per suppressed id' docs/vault-api-task-index.md` returns ≥1; § *Divergence check* states that the read now gates eligibility on the reconcile candidate gather and the deferred path, that the reconcile pass shares one read with the gate, that the deferred evaluation performs its own single read and none when nothing is due, and that a failed read fails open; § *Metric contract* carries a subsection naming `executor_reconcile_refused_total{gate="index_terminal"}` explicitly (unit: one per suppressed id, uncapped; fail-open rule) rather than an edit to the divergence counter's table.
- [ ] **CHANGELOG records the fix:** `head -20 CHANGELOG.md | grep -c '^## Unreleased'` returns 1, and the `## Unreleased` bullet names the deferred-respawn duplicate run — evidence: file content.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — exits 0 (format, generate, test, lint, addlicense)
- `go test ./pkg/handler/ -count=1` — exits 0; the reconcile, deferred, seed-gate, fail-open, no-over-suppression, divergence-report, cap/sort, counter and read-budget specs are green
- `go test ./pkg/metrics/ -count=1` — exits 0; the nine-gate registration spec is green
- `grep -c 'eight gates' pkg/metrics/metrics.go` — 0
- `grep -c 'changes no eligibility' docs/vault-api-task-index.md` — 0
- `head -20 CHANGELOG.md | grep -c '^## Unreleased'` — 1

### Operator-executable (runs on the host, spec verification ladder)

- Pre-change failure proof. The three new test files must be self-contained (no shared helper in a fourth file), or the copy below is incomplete:
  ```
  git worktree add /tmp/atx-prefix v0.49.1
  cp pkg/handler/<new-reconcile-suite>.go pkg/handler/<new-deferred-suite>.go pkg/handler/<new-seed-gate-internal>.go /tmp/atx-prefix/pkg/handler/
  (cd /tmp/atx-prefix && go test ./pkg/handler/ -count=1 2>&1 | tee /tmp/atx-prefix.log)
  grep -cE 'build failed|undefined:|cannot use' /tmp/atx-prefix.log   # must be 0
  git worktree remove /tmp/atx-prefix
  ```
- **Runtime replay — this is the operator's completion gate for this spec.** The spec is not complete until the replay shows, on the deployed binary, that a vault-terminal id is suppressed and no longer respawns. On the vault-api-backed executor that measured the defect, pick a task id the vault reports terminal (`status: completed` or `aborted`) while its view still looks workable — the divergence check names exactly those ids in `event=view_divergence`. Then, over a 15-minute window on that executor's logs:
  - `event=index_terminal_suppressed task=<id>` — ≥1 line naming that id
  - `event=respawn_after_grace_window task=<id>` — 0 lines for that id
  - `sum(increase(executor_reconcile_refused_total{gate="index_terminal"}[15m]))` — > 0
- **Why there is no Post-Deploy AC here:** the deploy check cannot be pinned at spec time — the executor's image tag is a release tag that does not exist yet, and the wrapper for the cluster that measured this defect (sun) is not recorded in this repository. The replay above is the completion gate; the container specs carry the behavioural proof.
- **No new scenario:** the gate's decision is in-process and the suites drive the real handler through its exported entry points, so an end-to-end scenario would add a cluster and a vault-api stub to a path already exercised, not signal.

## Desired Behavior

1. The deferred seed refuses a task whose **view** status is terminal (`completed`, `aborted`), in addition to the terminal-phase and empty-`current_job` refusals it already applies. A terminal-status task is never parked as pending work.
2. The deferred evaluation refuses a due entry whose **vault-index** state is terminal, and discards the entry without spawning. A task seeded before the vault moved on is caught at evaluation time.
3. The reconcile candidate gather refuses a task whose **vault-index** state is terminal, beside the existing view-side status and phase gates, so the id never enters the candidate set and never reaches the drive loop.
4. "Terminal on the index side" means the index's `status` is `completed` or `aborted`, **or** its `phase` is a terminal phase — the index-side twins of the two existing view-side gates. The set is the existing fixed one; no Config, task field or index value can widen it. An index entry that is non-terminal but differs from the view suppresses nothing. The asymmetry with DB1 is deliberate: the seed gates on the view because it already holds one and pays no read, while the evaluation and the gather gate on the index because the view is what is wrong there — and the event path stays ungated so a task that genuinely re-opens still starts.
5. The index read is budgeted per pass, not globally. Within one reconcile pass, one read is shared by the candidate gate and the divergence check. The deferred evaluation runs on its own ticker outside any reconcile pass, so it performs its own single read for its evaluation and none when no entry is due. No consumer adds a second read within one pass. The measured cost: the deferred loop ticks every 30 s and the reconcile loop every 60 s, so a pass that gates on every tick adds up to three reads a minute per executor where there was one — which is why the no-due-entries pass must read nothing.
6. Every suppression increments `executor_reconcile_refused_total{gate="index_terminal"}` once — on both the deferred and the reconcile path, so a deferred-path duplicate is not invisible in the metric. The counter is never capped, and its help text and doc comment name both paths.
7. Each pass that suppressed at least one id emits the suppressed ids as `event=index_terminal_suppressed task=<id>` at **V(2)** — the level the deployed executors run at, matching `event=reconcile_refused` — in ascending id order, capped at `maxDivergenceLogLines` (20) names per pass. The cap bounds the line, never the counter. The level is load-bearing: the runtime replay reads this line out of a deployed executor's logs, and a V(3) line is invisible there.
8. A missing index client or a failed index read suppresses nothing: the pass drives its candidates exactly as it does today. A spent pass budget also suppresses nothing, and drives nothing further — the pass stops where it is and the next pass re-evaluates. On the reconcile path a failed read is reported once by the divergence check as `event=view_divergence_read_error`; on the deferred path it is silent, because that loop runs no divergence check.

## Constraints

- `terminalStatuses` and the terminal-phase set stay fixed sets in `pkg/handler/task_event_handler.go`. A Config narrows eligibility and never widens it past "this task is over" — the same rule the existing view-side gates state.
- The eight existing reconcile gates keep their names, order and behaviour. A task an existing gate refuses is counted and reported under that gate, never as `index_terminal`; the new gate runs at the gather stage, after the existing filter returns the task eligible.
- The suppression line is **not** routed through `refuseReconcile`, which reports a gate only when it changes for a task. This line is emitted per pass for every suppressed id in the capped set, at V(2).
- The counter's label is `gate`; the label value is exactly `index_terminal`; no task id, cluster, vault or assignee is ever a label value.
- `maxDivergenceLogLines` (20) is reused as the cap. No new constant for the cap.
- The divergence check's own reporting is unchanged: same event names, same counts, same `compared` / `divergent` / `absent` semantics, same read-error handling. Only the advisory claim about eligibility changes, because it is no longer true.
- The Kafka event spawn path is unchanged and stays ungated.
- The index request stays unfiltered and read-only: no `cluster=` filter, no write to the vault, the existing client, the existing auth header pair, the existing 32 MiB body bound.
- The new specs must compile against `v0.49.1`: they may not reference any identifier this change introduces, and each file must be self-contained — a shared helper in a fourth file breaks the pre-change copy in Verification. The two external suites use the existing `testTaskVault`, which `v0.49.1` already declares; the internal seed-gate file declares its own vault constant, because `testTaskVault` lives in package `handler_test` and is not visible in package `handler`.
- Repo conventions are frozen: Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, glog, counterfeiter mocks, `//counterfeiter:generate` for new fakes.
- CHANGELOG gains an `## Unreleased` section above `## v0.49.1`.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection | Reversibility | Concurrency |
|---|---|---|---|---|---|
| Index unreachable, or no client configured | Fail open: no suppression, the pass drives as today | None needed; behaviour is the pre-change behaviour | Reconcile path: `executor_view_divergence_total{result="read_error"}` rises. Deferred path: silent by design | Reversible | Each pass reads independently; no shared state |
| Index is stale: it still says `in_progress` while the vault says `completed` | No suppression this pass; the task can still spawn | The next pass with a fresh index suppresses it; the duplicate window shrinks to the index's staleness, not to zero | `event=view_divergence` for the id plus `index_age_seconds` in the pass summary | Reversible | Two passes can each see a different index generation |
| Schema drift: an index `status` or `phase` outside the canonical set | Treated as non-terminal; nothing is suppressed | None needed; the value is reported so an operator can see it | `event=view_divergence` for the id, with the unfamiliar value in `vault_status=` / `vault_phase=`; `event=unknown_phase` on the view side if the view carries it | Reversible | — |
| The vault genuinely re-opens a task while the index still says terminal | The floor and deferred paths suppress it | The re-open publishes a task update, and the ungated event path spawns it; no operator action | `index_terminal_suppressed` for an id the operator knows is open | Reversible | The event path and the gate are independent |
| A task is refused by a view-side gate and by the index gate in the same pass | Only the view-side gate counts and logs | None needed | `executor_reconcile_refused_total` shows the existing gate, not `index_terminal` | Reversible | Single pass, sequential |
| The pass budget is spent mid-gather | Remaining tasks are neither suppressed nor driven; nothing is spawned for them | The next pass re-evaluates them | `event=reconcile_aborted` with the evaluated count | Reversible | Next pass is a fresh pass scope |
| The reconcile loop and the deferred loop both suppress the same id | Each counts its own refusal; each pass emits its own capped line | None needed; the two counts are two refusals, not a double count | Counter rate exceeds the divergent-id count within one minute | Reversible | Two goroutines, two pass scopes, one read each |
| The index returns a duplicate id | The last item wins, as the divergence check's map build already decides | None needed | None; no behaviour change | Reversible | — |

## Security / Abuse Cases

- **The gate can only suppress, never spawn.** A corrupt or hostile index therefore cannot start work; its worst outcome is a delayed spawn, bounded by the ungated event path and by the fail-open rule.
- **A stale-index denial is bounded, not permanent.** A suppressed id is re-evaluated on the next pass, and a genuine re-open arrives on the event path, which this change does not gate.
- **No new trust boundary.** The request, the auth header pair, the secret handling and the 32 MiB body bound are unchanged; the new log line carries the task id and nothing else, and the gateway secret still never reaches a log line or an error string.
- **No label injection.** Task ids appear only in the capped log line; the metric's label domain is the fixed gate vocabulary.
- **No new write path.** The index client is read-only by its interface, and nothing in this change writes to the vault.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Pass-scoped index read for the reconcile pass plus the candidate-gather gate, with the three-id reconcile suite, the divergence-still-reports spec, the no-over-suppression spec, the two reconcile fail-open specs and the two reconcile read-budget specs | 3, 5 | 2, 6, 7, 9, 10, 15 | — |
| 2 | Deferred seed gate and deferred evaluation gate, with the three-id deferred suite, the seed-gate spec, the two deferred fail-open specs and the two deferred read-budget specs | 1, 2, 4, 5, 8 | 3, 5, 6, 8, 15 | — |
| 3 | Observability: the per-pass sorted capped V(2) `index_terminal_suppressed` line, the uncapped `gate="index_terminal"` counter, its pre-initialisation, the nine-gate update to the existing `pkg/metrics` registration spec, and the help/comment correction | 6, 7 | 11, 12, 13, 14 | prompts 1, 2 (the suppression seam) |
| 4 | Documentation and release record: § *Divergence check* and a new § *Metric contract* subsection in the index document, plus the `## Unreleased` CHANGELOG entry | — | 16, 17 | prompts 1, 2, 3 |

Rationale: prompts 1 and 2 are independent (different files, different loop) and can run in parallel; AC 4 spans both and is satisfied only once both suites have landed. AC 6 and AC 15 each have one half per path, so they also complete only when prompts 1 and 2 are both in. Prompt 3 needs a suppression seam to assert against, so it follows 1 and 2. Prompt 4 records what shipped and states the read budget and the counter's unit, so it follows all three. `make precommit` (AC 1) is the gate on every prompt.

## Do-Nothing Option

The duplicate runs continue at their measured rate: 242 divergent ids per pass on one executor, 164 distinct tasks spawned in a day, and 7 cross-cluster post-success duplicates per 162 ids per 73 minutes. Each duplicate re-executes work the vault has already recorded as complete, consumes a spawn slot another task could use, and can overwrite a task file whose first run already finished. Nothing self-heals: the view never converges, the divergence check is advisory by contract, and the claim lease does not cover a post-success duplicate on the same cluster. Doing nothing leaves the fleet spawning finished work indefinitely, with the evidence already in the log.
