---
status: verifying
approved: "2026-10-28T10:59:02Z"
generating: "2026-10-28T11:05:52Z"
prompted: "2026-10-28T11:18:10Z"
verifying: "2026-10-28T12:30:41Z"
branch: dark-factory/bug-trigger-budget-refusal-is-written-to-the-record
---

## Summary

- A task refused by the scoped trigger budget is refused **silently**: the executor logs at `V(2)`, increments a metric, and writes nothing to the record.
- A record refused on every reconcile tick for weeks therefore still presents an unrelated, older failure as its current state.
- The refusal is only visible to whoever reads executor logs, so an operator looking at the record cannot tell a budget refusal from a stuck task.
- The fix writes the refusal's own numbers onto the record, beside — never over — the older failure.

## Problem

The trigger budget is a terminal gate: once `trigger_count` reaches `max_triggers` inside a scope, every later event is refused and nothing about the record changes. Because the refusal writes nothing, the record is indistinguishable from a record stuck for any other reason, and the reason it cannot run exists only in a log stream that is rate-sampled. Nine pr-review records are currently stuck this way, eight of them through this mechanism; two have been silent for 34 days.

## Goal

A task refused by the scoped trigger budget states, on the record itself, that it was refused, with the count, the cap and the scope that produced the refusal — and still keeps the earlier failure visible rather than replacing it.

## Acceptance Criteria

- [ ] **Post-Deploy (Rung-2): AC1 — the refusal reaches the record.** A refused task processed by the dev executor carries `trigger_refusal_count`, `trigger_refusal_max`, `trigger_refusal_scope` and `trigger_refusal_at` in its frontmatter, and a `## Trigger Budget` section whose body states the timestamp, `trigger_count N >= max_triggers M` and the scope. The three numbers equal those in the executor's `skip task …` line for the same task id.
  - `deploy_check:` `kubectlnukedev -n agent get deploy/agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}' | sed 's/.*://'`
  - `deploy_target:` `$(git describe --tags --abbrev=0 origin/master)`
- [ ] **Post-Deploy (Rung-2): AC2 — nothing else about the record changes.** The `## Failure` section is byte-identical before and after the refusal write, and `status`, `phase`, `assignee`, `current_job`, `trigger_count` and `trigger_scope` are unchanged. Evidence: negative — capture those six keys and the `## Failure` block before the write, compare after; every value is identical and no `## Failure` line was removed.
  - `deploy_check:` `kubectlnukedev -n agent get deploy/agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}' | sed 's/.*://'`
  - `deploy_target:` `$(git describe --tags --abbrev=0 origin/master)`
- [ ] **Post-Deploy (Rung-2): AC3 — the write does not admit the task.** The `skip task <id>: trigger_count N >= max_triggers M in scope "…"` line still appears for that record after the change. Evidence: log line — `kubectlnukedev -n agent logs deploy/agent-task-executor-personal --since=30m | grep 'skip task <id>'` returns ≥1 match in the same window as the write.
  - `deploy_check:` `kubectlnukedev -n agent get deploy/agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}' | sed 's/.*://'`
  - `deploy_target:` `$(git describe --tags --abbrev=0 origin/master)`
- [ ] **Post-Deploy (Rung-2): AC4 — the write happens once per refusal state, not once per tick.** A record refused repeatedly carries one write: `grep 'trigger_refusal_at' <task-file>` read twice, with at least three further `skip task <id>` lines observed in the log between the two reads, returns the same value both times. Evidence: state comparison — the two reads are identical while the refusals continue.
  - `deploy_check:` `kubectlnukedev -n agent get deploy/agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}' | sed 's/.*://'`
  - `deploy_target:` `$(git describe --tags --abbrev=0 origin/master)`
- [ ] **AC5 — the published command is pinned by a unit test.** A test asserts that a refusal publishes one `UpdateFrontmatterCommand` whose `Updates` carry the four `trigger_refusal_*` fields and whose body section is headed `## Trigger Budget`. Evidence: exit code — `go test ./pkg/handler/... -v -ginkgo.v -ginkgo.focus='publishes one trigger budget refusal'` exits 0 and the run prints `Ran 1 of` (`-v` is required: without it `go test` discards the Ginkgo summary on success, so the count would be invisible and the criterion unobservable).
- [ ] **AC6 — the repo's gate passes.** Evidence: exit code — `make precommit` exits 0.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — exits 0
- `go test ./pkg/...` — exits 0
- `grep -n 'trigger_refusal_count' pkg/result_publisher.go` — returns ≥1 line
- `grep -n 'Trigger Budget' pkg/result_publisher.go` — returns ≥1 line
- `git grep -n 'Heading: "## Failure"' -- pkg/` — returns exactly 2 lines, at `pkg/result_publisher.go:495` and `pkg/result_publisher.go:639`, both pre-existing; this change adds no third

### Operator-executable (runs on the host after PR merge, spec verification ladder)

- `kubectlnukedev -n agent get deploy/agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}'` — carries the released tag
- `kubectlnukedev -n agent logs deploy/agent-task-executor-personal --since=30m | grep 'skip task'` — a refusal line for the task under observation
- read that task's file — the four `trigger_refusal_*` keys, the `## Trigger Budget` section, `## Failure` intact

## Reproduction

1. Take a repo-backed task whose `trigger_count` has reached its `max_triggers`: `go-skeleton#133` (`0dd953c6-4155-5301-b6c2-8ff697f1c638`), `trigger_count: 4`, `max_triggers: 3`, scope `planning:4963ffa6`, on the dev executor `agent-task-executor-personal` running `v0.46.3`. To construct one: set `max_triggers: 3` on a repo-backed task and fire four events.
2. Observe the refusal, verbatim: `skip task 0dd953c6-4155-5301-b6c2-8ff697f1c638: trigger_count 4 >= max_triggers 3 in scope "planning:4963ffa6"` — 60 times an hour, beside `event=concurrency_admit … running=0 cap=3 max=5 quota=3`, so the record is refused in front of a free slot rather than queued behind capacity.
3. Read the record's task file: it carries `trigger_count: 4` and a `## Failure` block naming `deadline_exceeded` — a different, older cause — and nothing that mentions the budget.
4. Repeat over 34 days on `marketplace#13` (`302b1419`) and `moss#29` (`93a60611`): same silence, with an HTTP 503 `overloaded` named instead.

Note for the verifier: the executor's log stream is rate-sampled (glog lines carry a `(sample)` suffix), so an **empty** `grep 'skip task'` over a window proves nothing. Always quote a positive line from the same window as the change; never record an empty grep as the observation.

## Expected vs Actual

**Expected** — a record the executor refuses states why it cannot run. Every other terminal decision in this executor (`PublishFailure`, `PublishTypeMismatchFailure`, `PublishParkLostClaim`, `PublishAbort`) writes its reason to the record.

**Actual** — the refusal is the one gate that writes nothing. Its doc comment says so explicitly: *"reports whether the task's scoped trigger budget is exhausted. It writes nothing."*

## Why this is a bug

The record is the operator's interface to the queue. A gate that refuses permanently, indefinitely, while leaving the record asserting an unrelated cause makes the record actively misleading: an operator reading it concludes the task failed for the reason shown and remedies that, while the real refusal continues. The sibling gate shipped in v0.47.0 made every reconcile-floor refusal attributable for the same reason; the trigger budget is the remaining silent one.

## Desired Behavior

1. A refusal publishes one `UpdateFrontmatterCommand` carrying `trigger_refusal_count`, `trigger_refusal_max`, `trigger_refusal_scope` and `trigger_refusal_at` in its `Updates`.
2. The same command carries a `BodySection` headed `## Trigger Budget`, whose section text is the heading, a timestamp, `trigger_count N >= max_triggers M`, and the scope.
3. The `## Failure` section is not written, not replaced, and not truncated by this path.
4. Eligibility is unchanged: the same conditions that refused before still refuse, the spawn is still suppressed, and `trigger_count` is neither incremented nor reset by the write.
5. The write is published only when the record does not already carry the same refusal triple, **and** not more than once per triple inside the publisher's dedupe window — two guards with distinct jobs, see Constraints.

## Constraints

- **The section heading must not be `## Failure`.** `taskcmd.BodySection` is applied by `Markdown.ReplaceSection`, which replaces a section carrying the same heading. Writing `## Failure` from this path would delete the older cause — the exact outcome AC2 forbids.
- **Two dedupe layers, both required.** The record-state guard (skip when the record already carries the same count/cap/scope) is what makes the write once-per-state in the steady state. It cannot cover the window between publish and the controller applying the write, because the handler reads an informer snapshot that does not yet show it — so the publisher additionally dedupes through the same TTL'd LRU as `PublishAbort`, keyed on task id **and** the refusal triple so a changed scope is never suppressed. `PublishAbort` documents this exact trap: *"a write the controller has not yet applied must not be re-published each time."*
- The frontmatter key names above are frozen; they are persistent state and the vault records will carry them.
- `trigger_refusal_at` is written as an RFC3339 string. The vault normalises an unquoted date scalar to RFC3339, so the written form must already be that — a bare `YYYY-MM-DD` would be rewritten and read back unequal.
- `checkTriggerBudget` becomes `checkTriggerBudget(ctx context.Context, task lib.Task) (bool, error)`, following `recordTriggerSpawn` in the same file: `true` means refused and the caller suppresses the spawn; a non-nil error is a failed report and the spawn is **still** suppressed. A publish failure must never admit a refused task.
- No `context.Background()`, no `fmt.Errorf`, no bare `return err`.
- `metrics.TaskEventsTotal.WithLabelValues("skipped_trigger_cap").Inc()` and the existing `V(2)` line stay exactly as they are; this change adds a write, it does not move the signal.
- Tests are Ginkgo/Gomega, in `pkg/handler/*_internal_test.go`; the function is unexported.
- Both the frontmatter keys and the `## Trigger Budget` section are kept, though the count and cap already exist on the record as `trigger_count` and `max_triggers`. The section is what a human reads first, and the snapshot survives a later `trigger_count` reset that would otherwise erase the evidence the record was ever refused.

## Assumptions

- The four `trigger_refusal_*` keys collide with no key vault-cli owns or rewrites; the vault's date normalisation is the one interaction that matters and is handled by writing RFC3339 (see Constraints).
- The write does not pass through the result writer's ownership guard, so it perturbs nothing it does not name — AC2 asserts that outcome, and this assumption is why it holds.

## Failure Modes

| Trigger | Expected behavior | Recovery |
|---|---|---|
| Kafka publish of the refusal write fails | `checkTriggerBudget` returns the wrapped error; the spawn is still suppressed; the call site increments `TaskEventsTotal{error}` | the next reconcile tick retries, because neither dedupe layer recorded a successful publish |
| The record already carries the same refusal triple | no publish, no error, spawn still suppressed | none needed — this is the steady state |
| The record carries an *older* refusal triple (count or scope changed) | the new triple is published, replacing the section body and the four keys | none — a changed scope is a changed refusal and must be visible |
| Write published but not yet applied by the controller | the publisher's dedupe suppresses the repeat publishes inside the window; the first write lands | none — last write wins, and the record converges on one refusal |
| `## Failure` is written concurrently by the zombie path | both writes target different headings, so neither replaces the other; the record ends with both | none |
| The record is later reset (`trigger_count` lowered) while the refusal keys remain | the stale `## Trigger Budget` section still names the refusal that happened; the next genuine refusal overwrites it with the new triple | operator clears the keys when deliberately re-arming a record |

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Publisher: `PublishTriggerBudgetRefusal` on `ResultPublisher`, its interface entry, the triple-keyed dedupe, and the regenerated counterfeiter fake | 1, 2, 5 | AC1, AC5 | — |
| 2 | Handler: thread `ctx` and the error through `checkTriggerBudget`, add the record-state guard, wire the publish and the call site, plus the unit test | 3, 4, 5 | AC2, AC3, AC4, AC5, AC6 | prompt 1 |

Rationale: prompt 1 establishes the write, its shape and its in-flight dedupe, which prompt 2 then calls. AC1–AC4 are verifier-executed post-deploy observations, not prompt-executed; only AC5 and AC6 are produced by the prompts.
