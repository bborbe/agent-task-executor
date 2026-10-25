---
status: completed
summary: Labelled executor_redrive_capped_total by task_type and phase, propagated task_type into the cap log line and both call sites, and updated the invalidated assertions, docs, changelog and spec pins.
execution_id: agent-task-executor-redrive-labels-exec-040-redrive-capped-metric-labels
dark-factory-version: v0.198.7
created: "2026-10-25T17:06:07Z"
queued: "2026-10-25T17:06:07Z"
started: "2026-10-25T17:11:07Z"
completed: "2026-10-25T17:16:53Z"
---

# Label the re-drive cap metric by task type and phase

<summary>
- Operators can see which kinds of work the consecutive-re-drive cap is stranding
- The cap metric carries the task type and the phase of each refused spawn
- A record stuck at one phase is now distinguishable from one that advances and re-caps
- The aggregate "something was capped" counter remains, but with the dimensions that make it actionable
- The per-task log line carries the same two values, so a series and its line can be matched
- No new label is added that could grow without bound
- The cap's behaviour is unchanged: the same spawns are refused, at the same threshold
- The assertions this change invalidates are updated in the same commit, so the suite stays green
</summary>

<objective>
Make the consecutive-re-drive cap observable by dimension rather than only in aggregate, so an operator reading the counter can tell which task types and which phases are being abandoned instead of seeing a single opaque total. The cap itself does not change.
</objective>

<context>
Read `README.md` for project conventions — this repo has no `CLAUDE.md`. Then read `docs/claim-lease-protocol.md` § `## Streak refs`; its closing bullet documents `executor_redrive_capped_total` and the cap's rule.

Read `pkg/metrics/metrics.go`:
- `RedriveCappedTotal` — the counter to change. Currently `promauto.NewCounter`, unlabelled.
- `TaskRerunsTotal` — the closest single-label exemplar: `[]string{"task_type"}`. This is the label you are adding.
- `JobLookupDeniedTotal` — the only existing **two**-label vec (`[]string{"assignee", "namespace"}`), and therefore the shape you are actually writing.
- `TaskEventsTotal`, `ReconcilePassesTotal`, `SkippedUnknownAssigneeTotal` — three more `CounterVec` exemplars. Follow their doc-comment style, which explains *why* a bare counter was insufficient.

Read `pkg/handler/task_event_handler_redrive.go`:
- `signalRedriveCapped` — the only place that increments the counter and logs `event=redrive_capped`.
- `redriveStreakCapped` and `redriveStreakCappedInMemory` — its only two callers, both of which already hold the `lib.Task`.
- `taskPhaseString` — the existing phase helper. Reuse it; do not add a second way to read the phase.

Read `pkg/handler/task_event_handler.go` around the `maxConsecutiveRedrives` constant for the cap's own doc comment, which explains why the streak is per (task, phase).

The pre-initialisation rule this prompt relies on is `RULE go-prometheus/counter-pre-initialization (MUST)` in `/home/node/.claude/plugins/marketplaces/coding/docs/go-prometheus-metrics-guide.md`.
</context>

<requirements>
1. In `pkg/metrics/metrics.go`, change `RedriveCappedTotal` from `promauto.NewCounter` to `promauto.NewCounterVec` with label names `task_type` and `phase`, in that order. Update the `Help` string to say the counter is broken down by task type and phase.

2. Extend the existing doc comment on `RedriveCappedTotal` to state two things:
   - Why the two labels: the unlabelled counter said only that something was capped, so a reader of `capped=N` could not tell which kinds of work were stranded; and the phase matters because the cap is keyed per (task, phase), so a record stuck at one phase is a different problem from one that advances and re-caps.
   - Why it is deliberately NOT pre-initialised in `init()` with `.Add(0)`: the distinct `task_type` values are whatever this executor actually spawns, resolved from the agent's Config CRs at runtime rather than at compile time, and `phase` is free text from the task frontmatter. That is the unbounded side of the `counter-pre-initialization` rule's own boundary — pre-register bounded enums, use `absent()` in alerting for unbounded domains — so a later reader does not "fix" the omission.

3. In `pkg/handler/task_event_handler_redrive.go`, give `signalRedriveCapped` a `taskType string` parameter. It must call `metrics.RedriveCappedTotal.WithLabelValues(taskType, phase).Inc()` instead of `.Inc()`, and extend the existing log line with `task_type=%s` immediately after `task=`, keeping `phase=%q` where it is. Use `%s` for the new field: both existing task-type log fields in this repo use `%s` (`event=reconcile_skip … task_type=%s` in `task_event_handler.go`, `event=task_rerun task=%s task_type=%s phase=%s` in `recent_success.go`).

4. Update both calling functions — `redriveStreakCapped` and `redriveStreakCappedInMemory`, three call sites between them — to pass the task's type. Both already hold the `lib.Task`; derive the type with `task.Frontmatter.TaskType().String()`. There is no handler-level task-type helper to reuse (`taskTypeString` in `pkg/spawner/job_spawner.go` is in another package — copy its one-line shape rather than importing it).

5. Add tests in `pkg/handler/task_event_handler_redrive_test.go`, in the existing suite's style:
   - A capped spawn increments `metrics.RedriveCappedTotal` for exactly the `(task_type, phase)` pair of the task that was capped — assert with `prometheus/testutil.ToFloat64` on `WithLabelValues(<type>, <phase>)` before and after.
   - A capped spawn at a *different* phase produces a separate series, and the first series does not move. This is the property the label set exists for; a test that only checks "the counter went up" does not establish it.
   - A task with no `task_type` in its frontmatter increments the series with an empty `task_type` value rather than panicking. `WithLabelValues` panics on an arity mismatch, so this case also proves the two-value call is wired correctly.
   - A task that DOES carry a type increments the non-empty series: set `task.Frontmatter["task_type"] = "update-go"` on a copy of the fixture (the pattern already used at `pkg/handler/task_event_handler_claim_test.go:344`) and assert the capped spawn moves `WithLabelValues("update-go", "execution")` while the empty-`task_type` series does not move. Every other case here uses the empty value, so without this one a derivation that never reads the frontmatter passes them all.
   - Add one assertion in `pkg/metrics/metrics_test.go` pinning which label carries which value. `gatherLabels(mfs, name, labelName)` returns the VALUES of that label, so first touch the vec with two distinct sentinels — `metrics.RedriveCappedTotal.WithLabelValues("probe-type", "probe-phase").Add(0)` — then assert `gatherLabels(mfs, "executor_redrive_capped_total", "task_type")` contains `"probe-type"` and `gatherLabels(mfs, "executor_redrive_capped_total", "phase")` contains `"probe-phase"`. The touch is required: a CounterVec with no observed values emits no metric family, and this one is deliberately not pre-initialised (the same trap the `DeferredConcurrencyCapTotal` spec documents at `metrics_test.go:108-120`). `WithLabelValues(type, phase)` cannot detect a declaration-order swap — if the vec were declared `[]string{"phase", "task_type"}`, every other test would still pass while Prometheus reported `task_type="execution"`. This assertion is what makes the label placement a contract.

6. Update the existing assertions this change invalidates — they are part of the change, not a surprise for `<verification>`:
   - `pkg/handler/task_event_handler_redrive_test.go` asserts the AC7 log line as `event=redrive_capped task=%s streak=3 cap=3 phase=%q`. Insert `task_type=%s` between `task=%s` and `streak=` so it matches the new format; that fixture carries no `task_type`, so the expected value is empty.
   - `pkg/handler/task_reconcile_test.go` and `pkg/handler/task_event_handler_claim_test.go` read `testutil.ToFloat64(metrics.RedriveCappedTotal)` on the bare counter. `testutil.ToFloat64` panics unless the collector yields exactly one metric, so each must read the child series instead — `metrics.RedriveCappedTotal.WithLabelValues("", <the phase that fixture uses>)`. Read each fixture to get its phase; both fixtures carry no `task_type`, so the empty value is correct.
   Read the surrounding suite first and match its framework and its fixture construction. The handler suite's `BeforeEach` resets fixtures, not metrics — these counters are process-global and never reset, so assert deltas, never absolutes. Do not introduce a second test style into the file.

7. In `docs/claim-lease-protocol.md` § `## Streak refs`, extend the existing bullet that describes `executor_redrive_capped_total` to name both labels and give one example query that groups by them. Keep the section's existing prose; do not restructure it. No alert rule, dashboard or manifest in this repo references `executor_redrive_capped_total` — only `CHANGELOG.md`, `docs/`, `specs/`, `prompts/` and the Go files do — so no `k8s/` edit is needed.

8. In `CHANGELOG.md`, add a `## Unreleased` section at the top — immediately under the intro lines, above `## v0.38.3`; no such section exists today — and put one bullet under it in the file's existing voice: `- feat: **bold summary.** prose`. `feat:` is the correct prefix (the change adds observability without altering behaviour) and it is the machine-readable signal the releaser uses to pick the minor bump — dark-factory itself releases nothing here (`autoRelease: false`). Do not rename any existing version heading, and do not touch any released section.

9. Before finishing, re-run `<verification>` and confirm it passes. Then walk each `<summary>` bullet against the change and confirm it holds — in particular, confirm that no label was added whose value could grow without bound (a task identifier, a job name, a timestamp).
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT bump any version string, and do NOT create a tag. `.dark-factory.yaml` sets `autoRelease: false`, so dark-factory releases nothing; the maintainer releaser owns the tag.
- Existing tests must still pass — including the three assertions named in requirement 6, which this change invalidates and you must update.
- Do NOT change the cap's threshold, its per-(task, phase) keying, or the rule that the refusal does not clear the streak.
- Do NOT add a `task_identifier`, job-name, or timestamp label. Cardinality is bounded by the task types this executor spawns times the phase values; anything per-task would be unbounded on a counter.
- Do NOT pre-initialise the new label combinations in `init()` — see requirement 2 for why.
- Keep the existing log line's fields and their order; `task_type=%s` is an addition, not a replacement.
- This change supersedes the pin in `specs/in-progress/011-bug-redrive-streak-lost-across-clusters-and-restarts.md` that the log format and the counter's labels are unchanged. Update the three places that pin the old form so its checks stay runnable: the constraints pin ("The `event=redrive_capped` log line format and `executor_redrive_capped_total` name/labels are unchanged"), the AC13 evidence ("`event=redrive_capped task=<id> streak=3 cap=3`"), and Desired Behavior item 3 (which spells the same format string). Leave the Non-goals line "Do NOT add new metrics" as it is — this change adds no metric. Leave the `^executor_redrive_capped_total` metrics grep and the `event=redrive_capped task=<id>` log grep alone; both still match.
</constraints>

<verification>
ROOTDIR=/workspace make precommit
</verification>
