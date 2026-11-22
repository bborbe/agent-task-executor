---
status: prompted
approved: "2026-11-22T01:17:43Z"
generating: "2026-11-22T09:18:19Z"
prompted: "2026-11-22T09:39:02Z"
branch: dark-factory/bug-rearm-clobbered-by-stale-park-reemission
---

## Summary

- The executor re-publishes yesterday's terminal result for a task whose vault status was just changed back to `in_progress`, so a deliberate re-arm is rewritten to `completed` within about 90 seconds and never reaches the scheduler.
- The park's "already published for this success" dedup lives only in one pod's memory, while the success record it dedups against lives in the shared claim store and survives restarts. A pod that did not publish the park sees a day-old success with no publish record and publishes the park again.
- The same record is already migrated: the success record moved to the shared store for exactly this reason ("a fresh instance returns never succeeded for every task"), and the park marker was left behind.
- A second, independent defect sits at the skip gate: the reconcile refuses a task whose *view* says terminal without ever consulting the vault index it already reads in the same pass, so a view that lags a re-arm skips the task instead of evaluating it.
- Fix both: the park marker becomes durable in the shared store and keyed per success completion, and the terminal gate lets a non-terminal vault state override a terminal view while naming which source decided.

## Problem

A task completed yesterday is re-armed today by writing `status: in_progress` to the vault. Within roughly 90 seconds the executor rewrites the file back to `status: completed` / `phase: done`, so the re-arm never reaches the scheduler and the work does not run. The trigger is not the re-arm itself: it is a park for a success that already produced a park yesterday, re-emitted because the record that would have suppressed it was never durable. Every re-arm of a previously-completed task is exposed to this, which makes stop-at-window-end and resume-in-the-next-window unreachable by re-arming a vehicle.

## Goal

A park is published at most once per successful Job completion, no matter how many executor restarts or cluster changes happen between the success and the next pass, and a later completion of the same task still gets its own park. A task whose vault status is non-terminal is never rewritten to a terminal status by a park that already went out. When the executor's view and the vault disagree about whether a task is terminal, the vault decides, and the log names which source decided.

## Non-goals

- Do NOT repair, converge or back-fill the view. The divergence keeps being reported by `event=view_divergence` after this fix; this spec only stops the divergence from causing a park or a skip. **Rejected alternative: converge the view instead.** It repairs a symptom the executor cannot write authoritatively — the vault is the source of truth and the view is a projection of it — and the divergence is already reported; converging it is a separate, larger change that would not stop the park re-emission, because the park path reads the view's *phase* and acts on a success record that survives restarts.
- Do NOT change the vault write-back path, the controller's apply cycle, or the `TaskUpdated` event contract.
- Do NOT add a config knob, env var or opt-out for either half. Both engage unconditionally wherever their store is configured.
- Do NOT change the Kafka event path. It already decides on the status carried by the event — the vault's own write — so it is authoritative without this fix.
- Do NOT change the park's existing decision inputs: `taskShowsPriorRun`, `last_result_status`, the grace window and the re-drive cap keep their current semantics.
- Do NOT touch the metric-side ghost counting; a separate, closed row covers it.
- Do NOT add a new metric. The existing event counters and the new log line carry the signal.

## Assumptions

- Every cluster that dispatches from one vault shares one git-rest claim namespace. True today for nuke dev, nuke prod and sun, and the same assumption the success store and the redrive streak already rely on.
- The vault index reflects an `in_progress` write inside the park's write-back grace. The reproduction shows a 33-second lag, so this assumption is doing real work and the grace window is what covers it.
- A park marker lives under its own frozen ref prefix and cannot collide with a dispatch claim, a redrive-streak ref or a success record.
- When the shared store is readable, the per-pod map and the durable marker agree; when it is not, the map is the floor.
- The reconcile's index read is pass-scoped and shared, so the new gate consumes the read the pass already made rather than adding a request.

## Acceptance Criteria

- [ ] A recorded park suppresses a second publish for the same success completion. Evidence: a unit test that records a park for a success, advances the clock past the write-back grace, and asserts the publisher is called **zero** times and the log carries no `event=parked_succeeded` for that task — `make test` row names the case.
- [ ] A later completion of the same task still parks. Evidence: a unit test where a task succeeds and parks, is re-armed, and then succeeds **again** asserts the second completion's park publishes exactly once and that the stored marker's completion value advances to the second completion. A marker recording only that the task has been parked — with no completion value — fails this.
- [ ] A success with **no** recorded park still parks. Evidence: a unit test with a success older than the grace, no park record, and a `done` result status asserts the publisher is called exactly **once** and `event=parked_succeeded` appears exactly once. This is the negative control: a fix that removes the park path must fail it.
- [ ] The park marker survives a restart. Evidence: a test that writes the marker through one store instance and reads it back through a **freshly constructed** instance returns the recorded publish time, with the in-memory map empty.
- [ ] With the shared store unavailable, behaviour degrades to today's. Evidence: a test with a failing store asserts the first publish happens and the pod's own memory still suppresses the second.
- [ ] A terminal view over a non-terminal vault index is evaluated, not refused. Evidence: a test where the view reports `completed` and the index reports `in_progress` asserts **no** `event=reconcile_refused` line for the task appears, the spawn funnel is reached (its own `skipped_*` line or a Job reference), and exactly one `event=terminal_gate_divergence` line is emitted naming `view_status=completed`, `vault_status=in_progress` and `decision_source=vault`.
- [ ] A terminal view over a terminal vault index is still refused. Evidence: a test where both report `completed` asserts `event=reconcile_refused … gate=terminal_status` appears exactly once, no `event=terminal_gate_divergence` line is emitted, and no spawn occurs.
- [ ] A suppressed park leaves the task file untouched. Evidence: after the suppressed-park case, the task's frontmatter still reads `status: in_progress` and `git diff -- <task-file>` is empty.
- [ ] **Post-Deploy (Rung-2):** a re-armed task survives on dev. Evidence: after the release is deployed, a task at `status: completed` is set to `in_progress`; ten or more minutes later `vault-cli task get <task> status` still returns `in_progress`, and `kubectlnukedev -n agent logs deploy/agent-task-executor --since=15m | grep -E 'parked_succeeded|reconcile_refused.*gate=terminal_status'` returns **zero** lines for that task id.
  - `deploy_check:` `kubectlnukedev -n agent get deploy agent-task-executor -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `$(git describe --tags --abbrev=0 origin/master)`
- [ ] **Post-Deploy (Rung-2):** the motivating re-arm reaches the scheduler. Evidence: after the same re-arm, the executor log for that task carries either a Job reference or an explicit `run_window_closed` deferral line — not a pass whose `candidates` count excludes the task.
  - `deploy_check:` `kubectlnukedev -n agent get deploy agent-task-executor -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `$(git describe --tags --abbrev=0 origin/master)`

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — lint, vet and the repo's own gates pass
- `make test` — the unit and integration suite passes, including the eight new cases above
- `grep -n 'decision_source=vault' $(git ls-files 'pkg/handler/*.go' | grep -v _test.go)` — the divergence log line exists in **production** source, not only in a test

### Operator-executable (runs on the host after PR merge and deploy)

- `kubectlnukedev -n agent get deploy agent-task-executor -o jsonpath='{.spec.template.spec.containers[0].image}'` — the deployed image carries the release that includes this fix
- `vault-cli task set "<task>" status in_progress`, then two `vault-cli task get "<task>" status` reads ten or more minutes apart
- `kubectlnukedev -n agent logs deploy/agent-task-executor --since=15m` — the clean log window for the re-armed task id

## Desired Behavior

1. A park is published at most once for a given successful Job completion, and the suppression holds across a pod restart and across clusters that share the claim store.
2. The marker is keyed by the success completion, not by the task, so a second genuine completion of the same task gets its own park.
3. A task re-armed from a terminal status back to a non-terminal status is not rewritten to a terminal status by a park for a success that already produced one.
4. When the executor's view reports a task terminal and the vault index reports the same task non-terminal, the task is evaluated rather than refused.
5. On that disagreement the executor logs one `event=terminal_gate_divergence` line naming the view value, the vault value, and the source that decided.
6. When the vault index agrees that the task is terminal, the executor refuses it exactly as it does today, and emits no divergence line.
7. The park still fires for a task that genuinely needs it — a succeeded Job at an unchanged phase with no park recorded for that completion and no re-arm.

## Constraints

- **No new task-frontmatter field.** Agent pods rewrite frontmatter from a pre-spawn snapshot and would clobber it. This is the documented reason the redrive streak lives in the claim store rather than in the record — see the spec titled *A redrive streak lost across clusters and restarts*, Non-goals.
- **One record per task, bounded.** The success store bounds itself by deleting superseded refs on write; the park marker must not accumulate one ref per completion unbounded. **The two rules reconcile through the success store's own deletion rule:** a superseded completion is deleted on write, so a stale completion can no longer re-emit a park and its deleted marker is never needed. An implementer who bounds the marker by keeping only the latest without that coupling would believe stale-completion parks are still suppressed when they are not. Marker storage, bounded-ref and cross-cluster semantics are documented in `docs/claim-lease-protocol.md`, which this fix must extend.
- **The per-pod map stays** as a safety floor: an unreadable store must never let a second park through for a success this pod already published, and must never suppress the first.
- **The `terminal_status` and `index_terminal` gates stay distinct.** `index_terminal` (already shipped) suppresses on index evidence and fails **open**; `terminal_status` refuses on view evidence. They are driven by the same index read and the same terminal vocabulary, and conflating them would invert one of them. A failed index read must not flip either gate's direction, and the new gate must reuse the pass-scoped read rather than adding a second request. The gate reads the cached index's **status value**, not only its derived terminal set — the terminal set cannot supply `vault_status=` for the divergence line.
- **The vault index's authority, freshness and fail-open direction** are documented in `docs/vault-api-task-index.md`, which this fix must extend with the new gate's rule.
- **Claim mode off is unchanged.** A deployment with no shared claim store keeps today's behaviour exactly.
- **No new metric.** The existing event counters and the log line carry the signal.

## Failure Modes

| Trigger | Expected behavior | Recovery |
|---|---|---|
| Shared store read fails on the park path | Fall back to the per-pod map; a first park still publishes, a repeat is still suppressed | Log `event=park_marker_read_failed task=<id>`; the next pass retries. No re-publish. |
| Shared store write of the park marker fails after the park was published | The park stands; the marker is missing, so a later pass may re-publish once | Log `event=park_marker_write_failed task=<id>`. The marker failure must never re-enable the write-back path — the write-back is the irreversible step, the publish is not. |
| Vault index read fails at the terminal gate | Fall back to today's view-only decision | Log `event=terminal_gate_index_unavailable gate=terminal_status`; behaviour is unchanged from the pre-fix path. |
| Vault index is stale and reports a genuinely completed task as non-terminal | The task is evaluated and may be spawned once | The spawn path's own `skipped_status` / `skipped_phase` / `skipped_assignee` filters still run; the task is refused there. |
| Two clusters both publish a park for the same success | Both writes carry identical content; the second is a no-op | The marker is keyed by completion, so neither cluster can suppress the other's legitimate first park. |
| A pod on the pre-fix image runs during the rolling deploy | That pod reads no marker and may re-publish once; pods on the new image suppress normally | Bounded by rollout completion. `event=parked_succeeded` for the affected task id, ceasing once the old replicas are gone, confirms the window closed. |
| Clocks disagree between clusters | The grace-window comparison is per-cluster and monotonic, never a cross-cluster subtraction | A negative elapsed value is clamped to zero, as the current code already does; covered by a unit test, no operator action exists. |

## Security / Abuse Cases

- The terminal gate **fails open**: an unreadable index falls back to the view. That direction is deliberate — the alternative refuses every task during a vault-api outage — and it is bounded to one spawn per stale index, which the spawn path's own status and phase filters then refuse.
- The new index-driven override lets a vault-reported non-terminal state outrank a terminal view. A stale-but-wrong `in_progress` therefore costs at most one spawn attempt of an already-completed task; it cannot make a task skip the eligibility filter, and it cannot write to the vault.
- No new trust boundary. The gate reads the same index endpoint the divergence check already reads, with the same credentials, and adds no new caller.
- The new log line carries a task id, two status strings and a fixed source token. It carries no frontmatter, no assignee and no secret.
- The park marker ref carries a task id, a phase and a completion timestamp in its key — the same shape the success record already exposes — and no free-text payload.

## Reproduction

Measured 2026-11-21 on the personal vault, executor in namespace `agent` on nuke dev. Times are CET; executor log stamps are UTC and were converted.

```
05:55:10  vault-cli task set <task> status in_progress
          vault-cli task set <task> assignee trading-strategy-prototype-agent
          file verified: status: in_progress, phase: execution, assignee set
05:56:49  executor: event=parked_succeeded task=<task> phase=execution elapsed=86349s
05:57:22  task file mtime records its rewrite to status: completed / phase: done
05:57:33  executor: event=reconcile_refused task=<task> gate=terminal_status
                       task_type=strategy-prototype phase=done status=completed
05:57:34  executor: event=view_divergence task=<task> view_status=completed
                       vault_status=in_progress view_phase=done vault_phase=execution
```

Throughout the window every reconcile tick logged `event=reconcile evaluated=0 … skipped=11860 candidates=0`, and no `run_window_closed`, `concurrency_admit` or `concurrency_cap` line appeared for the task. No Job was created.

`elapsed=86349 s` is approximately 24 hours, which is what identifies the success as the previous day's run rather than a new one.

## Expected vs Actual

**Expected** (per `park_succeeded.go`'s own contract: "A park already published for this success is never published again"): the re-arm survives, the task is evaluated, and it reaches the scheduler.

**Actual:** the park is published a second time for a success that already produced one, the file is rewritten to `completed`, the reconcile then refuses the task as terminal, and the task never becomes a candidate.

## Why this is a bug

The code states the invariant and then fails to hold it across a restart. `pkg/task_store.go` keeps the publish record in a plain in-memory map, while the success record it is keyed against was deliberately moved to the shared claim store for precisely this failure — `pkg/claim/success_store.go` says the per-pod record "is per-pod memory: a fresh instance returns 'never succeeded' for every task and re-opens the duplicate on every deploy". The park marker was left behind in that migration, so the guard is correct in design and unreachable in practice. The divergence log line at 05:57:34 confirms the second defect independently: the executor observed the disagreement between its view and the vault and took no action on it.

## Do-Nothing Option

Every re-arm of a previously-completed task stays exposed. Nothing self-heals: the per-pod map is empty on every deploy, so the guard is unreachable by construction rather than intermittently. The practical cost is that stop-at-window-end and resume-in-the-next-window cannot be exercised by re-arming a vehicle — which is the only mechanism the night-window work has for proving its own criterion — so that criterion stays unprovable and the goal it serves cannot close. A second cost is diagnosability: the executor already observes the view/vault disagreement and reports it only as an advisory metric, so an operator seeing a task flip back has no log line that says which source decided.

## Suggested Decomposition

Prompts should be generated in this order — each row is a single prompt with a clear scope.

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Durable per-completion park marker in the shared claim store, per-pod map retained as floor | 1, 2, 3, 7 | 1-5, 8 | — |
| 2 | Vault-authoritative terminal gate plus the divergence log line | 4, 5, 6 | 6-7 | — |
| 3 | Documentation and release record: marker ref shape and lifecycle in `docs/claim-lease-protocol.md`, gate rule in `docs/vault-api-task-index.md`, `## Unreleased` CHANGELOG entry | — | — | prompts 1, 2 |

Rationale: prompt 1 closes the clobber and is independently shippable; prompt 2 closes the skip gate and the diagnosability gap, reusing the pass-scoped index read that already exists rather than depending on prompt 1; prompt 3 keeps the protocol knowledge in the repo rather than only in this spec. Post-deploy ACs 9-10 are verified after all three land and the release is deployed.
