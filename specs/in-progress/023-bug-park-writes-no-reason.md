---
status: verifying
approved: "2026-11-09T17:31:08Z"
generating: "2026-11-09T17:51:21Z"
verifying: "2026-11-09T18:06:53Z"
branch: dark-factory/bug-park-writes-no-reason
---

## Summary

- Both park paths write `phase: human_review` and **nothing else**, so a parked record carries no reason.
- `parkSucceededWithoutPhaseMove`'s own contract calls the park "a deliberate human park" — but the record cannot be told apart from a stall.
- The queue scorer that measures the goal's "no stuck records" criterion excuses a record only when it carries a stated reason; a bodyless park counts as stuck.
- Fix: the shared park write also records why it parked, in the record's body.
- The phase and the reason travel in one publish — the existing failure path already does exactly this.

## Problem

When the executor parks a task it flips the phase to `human_review` and leaves the body untouched. Nothing in the record says which of the two park paths fired, why, or what the previous holder was. A human opening the record sees a phase and no explanation; the scorer that decides whether the queue is healthy sees no stated reason and counts the record as stuck. The park is therefore silent in both directions — unreadable to an operator and invisible to the gate that measures the defect.

## Reproduction

Against a deployed executor (`v0.57.5` in prod, `v0.57.6` in dev), on a busy queue:

```bash
# 1. A park is happening, repeatedly.
kubectlnukeprod -n agent logs deploy/agent-task-executor-openclaw --since=72h \
  | grep -c 'event=parked_succeeded'
# observed: 19 — note --since is a ceiling, not a guarantee: this deployment's
# container log retained only ~37 minutes (16,616 lines, 15:43:48Z–16:20:37Z).

# 2. Not one of those parks left a reason in the record.
grep -rl '^## Parked' /Users/bborbe/Documents/Obsidian/private-agent/tasks
# observed 2026-11-09: exactly one hit, and it is a human-authored record
# (...agent-task-executor - 40 - 95029497 - feat-label-skipped-unknown-assignee...),
# not an executor park. No executor park has ever written the section.

# 3. The population the parks land in, measured 2026-11-09T16:51:16Z over
#    PR Review*.md with status not completed/aborted and phase human_review:
#    open=51  previous_assignee=pr-reviewer-agent 25  with '## Parked' 0
#    neither '## Parked' nor '## Verdict' 36
```

The lost-claim path shows the same shape: `executor_parked_lost_claim_total{result="parked"}` increments — 1 in the ~6.8 h since the `v0.57.5` pod started `2026-11-09T10:11:40Z` — and the record it parked carries no section either.

## Expected vs Actual

**Expected**, per `pkg/handler/park_succeeded.go:36-42` — *"a succeeded Job whose phase never moved is a deliberate human park — the agent exited 0 without advancing the task, so nothing will ever move it"* — and per the goal's criterion that a designed hand-off is a record whose reason is stated: a parked record names, in its own body, that it was parked, on which path, and why.

**Actual**: `PublishParkLostClaim` (`pkg/result_publisher.go:1027-1040`) publishes a frontmatter-only update — its own doc comment reads *"writes phase `human_review` and nothing else"* — and `PublishParkSucceeded` (`:1042-1052`) delegates to it at `:1051`. No body write exists on either path.

**The fix is a single publish.** `taskcmd.UpdateFrontmatterCommand` carries both `Updates` and `Body *BodySection`, and `publishFailure` already writes both in one command (`pkg/result_publisher.go:751-761`). The phase and the reason therefore land together by construction — no second publish, no window in which a record reads `human_review` with no reason.

## Why this is a bug

The park exists to hand a task to a human. A hand-off that states no reason is indistinguishable from the failure it is meant to be distinguished from: an operator cannot tell a deliberate park from a lease mishap, and the scorer that measures the goal cannot excuse it. The consequence is measurable — 36 open records carry neither a section nor a verdict, and the criterion they count against cannot drain while parks keep arriving without a reason.

## Goal

Every park writes a `## Parked` body section naming the reason and the evidence available on that path, so no parked record is ever bodyless and every future park is classifiable after the log has rotated.

## Assumptions

1. The controller applies `UpdateFrontmatterCommand.Body` heading-keyed, replacing a section in place rather than appending. It is implemented as `ReplaceOrAppendSection` in the controller (`agent-task-controller/pkg/command/task_update_frontmatter_executor.go:263`, tests at `:534`, `:771`, `:891`), and proven in production by `## Failure` travelling the identical command through `publishFailure` (`pkg/result_publisher.go:751-761`), whose sections do not accumulate. The controller repo is read-only for this spec.
2. The lost-claim park is rare enough that command-level evidence is sufficient for its half of this spec — 1 park in the ~6.8 h since the pod started. A field record for that path is therefore not required; the succeeded path supplies the field evidence.
3. Images deploy by release tag, not by commit SHA: `Makefile:8` derives `VERSION` from `git describe --tags $(git rev-list --tags --max-count=1)` and `Makefile.docker:29` tags with it.

## Acceptance Criteria

- [ ] **Post-Deploy (Rung-2):** a dev park leaves a record carrying exactly one `## Parked` section — evidence: `grep -c '^## Parked' <record>` returns `1` for the record the `event=parked_succeeded` line names.
  - `deploy_check:` `kubectlnukedev -n agent get deploy/agent-task-executor-openclaw -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `$(git describe --tags $(git rev-list --tags --max-count=1))`
- [ ] A park on the **lost-claim** path publishes the same section. Evidence, on the command decoded from the single message the new `Describe("PublishParkLostClaim")` producer records: `Body.Heading == "## Parked"`, `Body.Section` has prefix `## Parked` and suffix `"\n"` (the convention `pkg/result_publisher_test.go:1597` and `:1600` already assert for another section), `Body.Section` contains `parked_lost_claim`, and `Body.Section` is at most 8 lines.
- [ ] The **reason distinguishes the path**. Evidence: the succeeded path's field record — the same record AC1's evidence line names — shows a reason line containing `parked_succeeded` (`grep -A8 '^## Parked' <record>`), and the lost-claim path's decoded command contains `parked_lost_claim` — the same tokens the paths already log at `pkg/handler/park_succeeded.go:145-146` and `pkg/handler/claim_wiring.go:325`. A constant reason shared by both paths fails.
- [ ] The lost-claim section names the **claim key** and the **previous holder**. Evidence, on the same decoded command: the section contains the exact key `DeriveClaimKeysWithBody` derives for the fixture body — `claims/pr/<owner>/<repo>/<n>` for a body carrying that PR URL, or `claims/task/<taskid>` on the fallback shape returned when no PR URL is in the body (`pkg/claim/key.go:113-114`, a live warned path — `pkg/handler/claim_wiring.go:168-173` logs `event=claim_key_fallback`) — asserted by value, not merely by pattern, so a hardcoded literal fails; and a holder equal to the `PreviousTaskID` the takeover reported, or the literal `unavailable` when that value is empty (`pkg/claim/lease_manager.go:77-85`).
- [ ] The write is **idempotent by construction**, not by dedupe code. Evidence: both paths publish a byte-identical `Body.Heading` (`## Parked`), which is the heading-keyed write's idempotency condition — `BodySection` is documented at `github.com/bborbe/agent/command/task/update-frontmatter-command.go:34-40` as *"describes an idempotent body-section write"*. The record's single-section outcome is asserted once, on the field record, in AC1.
- [ ] **Negative — no unrelated body content changes.** Evidence: `grep -c '^## Failure'` and `grep -c '^## Diagnostics'` on the parked record in `private-agent/tasks` are unchanged from before the park.
- [ ] `make precommit` exits 0.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

- `make precommit` — lint, vet and the full test suite
- `go test ./pkg/ -run 'PublishPark' -v` — both park publishers' decoded-command assertions
- `go test ./pkg/handler/... -run 'Park' -v` — the park paths' handler tests
- `grep -n '## Parked' pkg/result_publisher.go` — the section literal exists in the publisher

### Operator-executable (runs on the host after PR merge, verification ladder)

- `kubectlnukedev -n agent get deploy -o custom-columns='NAME:.metadata.name,IMAGE:.spec.template.spec.containers[0].image'` — confirms `agent-task-executor-{openclaw,personal,trading}` run the new tag (all three verified present; dev was on `v0.57.6`, prod on `v0.57.5`, as of 2026-11-09)
- `kubectlnukedev -n agent logs deploy/agent-task-executor-openclaw --since=15m | grep -c 'event=parked_succeeded'` — at least one park in dev
- `grep -A8 '^## Parked' <the record that park named>` — the section, with its reason populated

## Desired Behavior

1. A park on the succeeded path appends a `## Parked` section to the task body in the same publish that sets the phase, so a reader never sees the phase without the reason.
2. A park on the lost-claim path appends the same section, carrying the claim key and the previous holder in addition to the reason.
3. The section is written heading-keyed, so a publish retried by `parkAfterLostClaim`'s loop — or a park re-evaluated on a later pass — overwrites it in place rather than appending a second copy.
4. The section is a **fixed template of at most 8 lines** — no log tail, no echo of the task body, no unbounded field.
5. Neither park path writes to any other body section; `## Failure`, `## Diagnostics` and `## Verdict` are untouched.
6. The phase write and the body write land in one command, so a record never shows `human_review` with no reason because a second write failed.

## Constraints

- `taskShowsPriorRun` (`pkg/handler/claim_wiring.go:296-304`) is **not** changed by this fix. Whether its `job_started_at` evidence is too weak is a separate question with its own classification step; changing it here would narrow a shipped guard on unclassified evidence.
- The two parks must keep sharing **one** write. `pkg/result_publisher.go:1042-1046` records why: *"Duplicating it would let a future change to the park payload drift between them."* Splitting the implementation to give the succeeded path a smaller section violates that invariant.
- **Exactly one existing assertion is permitted to change**: `pkg/result_publisher_test.go:1116`, inside `Describe("PublishParkSucceeded")` → `It("writes exactly the phase key with human_review and nothing else")`, currently `Expect(cmd.Body).To(BeNil())`. That assertion encodes this bug and must flip to assert the `## Parked` heading. The other four `Expect(cmd.Body).To(BeNil())` assertions in the package — `pkg/result_publisher_test.go:936` (`PublishClearCurrentJob`), `:1170` (`PublishRestoreAssignee`), `:1448` (`PublishAbort`), `pkg/result_publisher_log_tail_test.go:182` (`PublishClearCurrentJob`) — belong to non-park publishers and stay green unchanged.
- Satisfying AC4 requires the claim key and the previous holder to reach the park write. That means: capturing the `TakeoverResult` at `pkg/handler/claim_wiring.go:185` (today discarded as `_, takeoverErr := manager.TakeoverPair(...)`), widening `parkAfterLostClaim` (`:317`, today `(ctx, key, task)`) to carry them, and changing the `ResultPublisher` interface (`pkg/result_publisher.go:176-188`). The interface change regenerates `mocks/result_publisher.go` (counterfeiter) and updates three hand-written doubles — `pkg/handler/claim_spawn_order_internal_test.go:125`, `pkg/handler/claim_wiring_internal_test.go:319`, `pkg/job_watcher_internal_test.go:557` — without changing what any of them asserts. Call-site, private-signature and interface changes are all in scope.
- The interface and implementation doc comments that currently say the park write is frontmatter-only (`pkg/result_publisher.go:176-188`, `:1027`, `:1042`) are updated in the same change. A stale contract comment here is what re-creates this bug class.
- Both park paths and the `## Parked` section shape are documented in `docs/claim-lease-protocol.md`, which today names the park only for untrusted-author records; the section shape is added there in the same change, so the contract outlives this spec.
- **Out of scope:** the vault-side sampler entry that would make a `## Parked` section excuse a record from the stuck count. `pr-review-queue-sampler.py`'s `PARK_NOTICES` holds only `untrusted_author`, and its own comment forbids adding a shape before a real record carries it — so that registration lands only after this fix has produced such a record. It is tracked in the vault task, not here.
- No new Config field, no new CRD knob.
- `pkg/handler/claim_wiring_internal_test.go` is plain `testing`, not Ginkgo — internal-package Ginkgo specs are never discovered by the external suite in this repo.

## Failure Modes

| Trigger | Expected behavior | Recovery |
|---|---|---|
| The publish fails entirely | Neither phase nor body is written; the record is untouched | `grep 'event=parked_lost_claim_failed'` shows `attempts=3` and `executor_parked_lost_claim_total{result="failed"}` increments by 1; the record's body is unchanged |
| The publish is retried N times | Exactly one `## Parked` section | Re-publishing the same heading overwrites it in place — `BodySection` is heading-keyed and idempotent by contract. No dedupe code is added. |
| `PreviousTaskID` is empty (key absent, create used) | The section still writes, with the holder recorded as `unavailable` | Field states why it is empty; the record stays classifiable |
| Two executors park the same record concurrently | The second write overwrites in place; no duplicate section | Re-read the record: `grep -c '^## Parked'` is still `1` |
| A future park path is added | It inherits the section by using the shared write | The shared implementation is the single entry point |

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Shared park write carries the `## Parked` section, both paths, per-path evidence, tests | 1-6 | 1-7 | — |

One prompt: one code layer (`pkg/result_publisher.go` plus its call sites, the interface and tests). No scenario — the behavior is reachable by unit test through the same publisher both paths already call, and the only link a test cannot cover is the controller's application of `Body`, which is already proven in production by `## Failure` travelling the identical `UpdateFrontmatterCommand` (`pkg/result_publisher.go:751-761`). What remains unproven is the section's text, which a unit test asserts exactly.
