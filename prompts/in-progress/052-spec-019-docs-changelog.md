---
status: approved
spec: [019-bug-deferred-respawn-ignores-vault-terminal-status]
created: "2026-10-30T00:25:18Z"
queued: "2026-10-29T23:48:40Z"
branch: dark-factory/bug-deferred-respawn-ignores-vault-terminal-status
---

# Index-terminal gate documentation and release record

<summary>
- The index document no longer calls the read purely advisory
- It states the read scope per path: one shared read on the reconcile pass, one on the deferred evaluation, none when nothing is due
- It states that a failed read fails open and suppresses nothing
- The metric contract gains a subsection for the new refusal gate with its unit and fail-open rule
- The log line, its per-pass cap and its ordering are recorded next to the metric
- The changelog records the deferred-respawn duplicate run as an unreleased fix
- The released version sections are untouched

</summary>

<objective>
Record what shipped: correct the vault-api index document so the read is documented as gating eligibility, document the new refusal gate's metric contract, and add the `## Unreleased` changelog entry for the deferred-respawn duplicate fix.
</objective>

<context>
Read `docs/vault-api-task-index.md` — § *Divergence check* (the advisory claim and the "One read per pass" bullet) and § *Metric contract* (the divergence counter's unit table).

Read `CHANGELOG.md` — the file opens with the intro lines and `## v0.49.1`; there is no `## Unreleased` section today. Follow the existing voice: `- fix: **bold summary.** prose`, one bullet per logical change, naming the measured evidence.

Read the shipped code to describe it accurately:
- `pkg/handler/vault_index_cache.go` — the pass-scoped single read.
- `pkg/handler/index_terminal_suppression.go` — the per-pass sorted, capped V(2) line and the uncapped counter increment.
- `pkg/handler/task_event_handler.go` — `ReconcileOnce` installs the pass cache; `gatherCandidates` creates the collector and reports it once.
- `pkg/handler/task_event_handler_deferred.go` — the seed's terminal-status refusal and the evaluation's gate, its own single read, and no divergence check.
- `pkg/metrics/metrics.go` — `ReconcileRefusedGateIndexTerminal` and the pre-initialised nine-gate set.

Read these coding guides from the coding plugin at `/home/node/.claude/plugins/marketplaces/coding/docs/`:
- `changelog-guide.md` — entry format, verb style, anti-patterns, `## Unreleased` rules
- `documentation-guide.md` — doc voice and structure
</context>

<requirements>
1. In `docs/vault-api-task-index.md` § *Divergence check*:
   - remove the claim that the check is advisory and "changes no eligibility, admission, capping or spawning" (the exact substring `changes no eligibility` must not appear anywhere in the file). State instead that the same read now gates spawn eligibility: the reconcile candidate gather refuses an id whose index entry is terminal, and the deferred evaluation refuses a due entry whose index entry is terminal;
   - replace the "One read per pass" bullet so it states: the reconcile pass performs exactly one read and shares it between the candidate gate and the divergence check; no read happens before the vault-classification early returns (`view_not_ready`, `task_vault_unstamped`, `task_vault_mismatch`); the deferred evaluation runs on its own ticker, performs its own single read when at least one entry is due and none when nothing is due;
   - state that a failed read fails open: nothing is suppressed, the reconcile pass reports the failure once as `event=view_divergence_read_error`, and the deferred path is silent because it runs no divergence check;
   - state that terminal on the index side means `status` `completed` or `aborted`, or a terminal `phase` (`human_review`, `done`), and that a non-terminal entry that merely differs from the view suppresses nothing.
2. In `docs/vault-api-task-index.md` § *Metric contract*, add a subsection for the new gate (do not edit the divergence counter's unit table):
   - name `executor_reconcile_refused_total{gate="index_terminal"}` explicitly;
   - unit: one increment per suppressed id, **uncapped**, on both the reconcile candidate gather and the deferred-respawn evaluation;
   - the log contract: one `event=index_terminal_suppressed task=<id>` line per suppressed id per pass at V(2), sorted ascending by id and capped at 20 names per pass, the cap bounding the line and never the counter;
   - the fail-open rule: a missing client, a failed read or a spent pass budget suppresses nothing and increments nothing;
   - the label hygiene rule: `gate` is the only label, its value is the fixed gate vocabulary, and no task id, cluster, vault or assignee is ever a label value.
   The file must contain `index_terminal` at least twice, `index_terminal_suppressed` at least once, and `one per suppressed id` at least once.
3. In `CHANGELOG.md`, add a `## Unreleased` section immediately under the intro lines and above `## v0.49.1`. One bullet in the file's existing voice (`- fix: **bold summary.** prose`) that:
   - names the deferred-respawn duplicate run as the defect and cites the measured evidence (242 divergent ids in one pass on one executor, 164 distinct tasks spawned in a day, 7 cross-cluster post-success duplicates over 162 ids in a 73-minute window);
   - states the three gates: the startup seed refuses a terminal view status, the deferred evaluation and the reconcile candidate gather refuse a terminal index entry;
   - states the read budget (one shared read on the reconcile pass, one on the deferred evaluation and none when nothing is due), the fail-open direction, the ungated Kafka event path, and the observability (V(2) `event=index_terminal_suppressed` capped at 20 sorted ids per pass, uncapped `executor_reconcile_refused_total{gate="index_terminal"}`);
   - states that the stale view is not repaired.
   Do not rename an existing version heading and do not touch a released section.
4. Before finishing, re-run `<verification>` and confirm it passes; walk each requirement above against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do not change any Go code, any test, or any metric name in this prompt.
- The changelog entry goes in `## Unreleased`, above `## v0.49.1`; released sections stay byte-identical.
- The doc must not describe a second read, a config knob, a repaired view, a gated Kafka path or a written vault — none of those shipped.
- The document keeps its existing assumption framing: the index is the authority for `status` and `phase` as an assumption, not a contract.
- No config knob, flag or opt-out is documented for this gate.
</constraints>

<verification>
- `ROOTDIR=/workspace make precommit` — must exit 0.
- `! grep -q 'changes no eligibility' docs/vault-api-task-index.md`
- `test "$(grep -c 'index_terminal' docs/vault-api-task-index.md)" -ge 2`
- `test "$(grep -c 'index_terminal_suppressed' docs/vault-api-task-index.md)" -ge 1`
- `test "$(grep -c 'one per suppressed id' docs/vault-api-task-index.md)" -ge 1`
- `head -20 CHANGELOG.md | grep -q '^## Unreleased'`
</verification>
