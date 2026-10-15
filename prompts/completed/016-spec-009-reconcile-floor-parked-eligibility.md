---
status: completed
spec: [009-bug-parked-tasks-never-redrive]
summary: Made the reconcile floor treat a parked task with a resolvable declared task_type as eligible and restore the resolved Config's assignee via a new floor-only resolveConfigForReconcile plus ResultPublisher.PublishRestoreAssignee, leaving the Kafka consumer path untouched.
execution_id: agent-task-executor-redrive-parked-exec-016-spec-009-reconcile-floor-parked-eligibility
dark-factory-version: v0.198.7
created: "2026-10-15T08:44:50Z"
queued: "2026-10-15T09:04:32Z"
started: "2026-10-15T09:06:47Z"
completed: "2026-10-15T09:27:04Z"
---

<summary>
- The executor's reconcile floor currently refuses a task whose routing value is empty — on the one path whose stated purpose is to recover stranded tasks.
- This prompt makes such a task eligible when the type it declares resolves to an agent configuration, and writes that configuration's routing value back onto the task.
- The value written is the one the configuration declares, never the task's recorded previous value. That key is a history, not a routing decision.
- The write goes out through the same command path the executor already uses for its other frontmatter writes, so the controller applies it and the resulting change re-publishes the task event the normal spawn path already consumes.
- The floor does not spawn for such a task. It restores the routing value and lets the next event spawn normally, so every existing guard still applies.
- The Kafka consumer entry point is not touched: the new fallback lives behind a separate function that only the floor calls, so the consumer keeps its current behaviour and its current skip labels.
- A task whose declared type resolves to no configuration, or to more than one, is left untouched and logged. No new metric is introduced.
- A task that already carries a routing value, a terminal task, and a task outside its configuration's trigger set all behave exactly as they do today — this adds a case, it does not widen one.
- No configuration field, environment variable, CLI flag, or schema field is added.
</summary>

<objective>
Make the executor's reconcile floor treat a parked task — an empty routing value with a declared type that resolves to exactly one agent configuration — as eligible, and have it publish the resolved configuration's routing value back onto the task through the executor's existing command path. This is the second and final half of spec 009: the sibling prompt (already executed) added a resolve-by-type lookup on the resolver, which has no caller today. After this prompt the parked-task class re-enters the spawn filter on its own, with no vault write by an operator and no controller restart. The new resolution fallback must sit behind a floor-only function so the Kafka consumer path is genuinely unchanged.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for project conventions (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping with `errors.Wrapf(ctx, err, ...)`, sentinel errors aliased as `stderrors`, counterfeiter mocks, coverage ≥80% for new code).

Read these files fully before changing anything:

- `/workspace/pkg/handler/task_event_handler.go` — the whole file (~1,300 lines). The anchors this prompt touches:
  - `parseAndFilter` (line 321) — the Kafka consumer path. **Do not change it.** It calls `h.resolveConfig` at line 345, and its empty-routing-value guard sits at lines 397-401 (the `skipped_assignee` branch). Both stay exactly as they are.
  - `resolveConfig` (line 410) — its body opens with `if task.Frontmatter.Assignee() == "" { return nil, false, nil }` at lines 414-416. **It has two callers** — `parseAndFilter` at line 345 and `reconcileTask` at line 1061 — which is why this prompt does NOT extend it (see requirement 1).
  - `effectiveTriggerPhases` (line 471) and `effectiveTriggerStatuses` (line 480) — the configuration-authoritative trigger sets, already used by `reconcileTask`. Reuse them; do not re-implement the fallback.
  - `ReconcileOnce` (line 887) — the pass loop. Its doc comment states the floor re-drives every eligible task "through the existing `spawnIfNeeded` path" and must be updated to describe the two outcomes.
  - `reconcileOutcome` and its constants (lines 965-972): `reconcileOutcomeSkipped`, `reconcileOutcomeRedriven`, `reconcileOutcomeDeferred`, `reconcileOutcomeCapped`.
  - `reconcileOne` (line 983) — owns the per-file decision. The relevant order is: `h.redriveStreakCapped(task, relPath)` at line 1011, then `h.spawnIfNeeded(ctx, task, config)` at line 1014, then `reconcileOutcomeRedriven` at line 1028.
  - `reconcileTask` (line 1053) — the eligibility filter. Its final gate is `if task.Frontmatter.Assignee() == "" { return task, nil, false, nil }` at lines 1078-1080, and its resolve-error return is `return lib.Task{}, nil, false, err` at line 1063.
- `/workspace/pkg/config_resolver.go` — the whole file (~200 lines). `ErrConfigNotFound` (line 20), `ErrConfigAmbiguous` (line 26), the `ConfigResolver` interface (lines 32-42) with both `Resolve` and the sibling prompt's `ResolveByTaskType(ctx context.Context, taskType string) (AgentConfiguration, error)`, and the `configResolver` implementation.
- `/workspace/pkg/result_publisher.go` — the whole file. The counterfeiter annotation (line 30), the `ResultPublisher` interface (lines 34-86), `targetVaultFromTask` (line 172), `PublishSpawnNotification` (line 177) as the shape to mirror, `PublishTypeMismatchFailure` (**defined at line 318**) — **the existing method that writes the `assignee` key** (its `assignee` write is line 333, its `updates["previous_assignee"] = priorAssignee` line 337) — and the `publishRaw` **definition at line 360** (line 191 is a call site inside `PublishSpawnNotification`).
- `/workspace/pkg/agent_configuration.go` — `AgentConfiguration` (lines 28-84). `Assignee string` is line 30. Note that `convert` in the resolver copies the spec's assignee into it verbatim, so it is the composed `{assignee}-{vaultName}` value when the configuration is vault-scoped — read the sibling prompt's spec coverage rather than assuming.
- `/workspace/pkg/handler/task_reconcile_test.go` — the whole file. It is the reconcile suite's harness: `fakeResolver *mocks.FakeConfigResolver`, `fakeResultPublisher *mocks.FakeResultPublisher`, `fakeGitRestClient *mocks.FakeGitRestClient`, `taskStore *pkg.TaskStore` (lines 36-41), and the helpers that build a task file body and drive `ReconcileOnce`. Extend this file; do not create a second reconcile suite.
- `/workspace/pkg/result_publisher_test.go` — the publisher suite (`package pkg_test`). The new publisher spec belongs here, alongside the existing `previous_assignee` assertion at line 540.
- `/workspace/mocks/result_publisher.go` and `/workspace/mocks/config_resolver.go` — counterfeiter fakes. Adding a method to `ResultPublisher` means `mocks/result_publisher.go` MUST be regenerated with `make generate`.

Verified shapes (read from the working tree, do not re-derive from memory):
- `taskcmd.UpdateFrontmatterCommand` carries `TaskIdentifier`, `TargetVault`, and `Updates lib.TaskFrontmatter` (a `map[string]interface{}`). `PublishSpawnNotification` builds one with `Updates: lib.TaskFrontmatter{"spawn_notification": true, "current_job": jobName, "job_started_at": ...}` and sends it via `p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd)`.
- `targetVaultFromTask(task lib.Task) string` reads the task's `target_vault` frontmatter. Every executor-originated frontmatter write passes it, so the routing stamp comes from the task rather than from the executor's own vault name.
- `lib.TaskFrontmatter` has `Assignee()`, `TaskType()` (a `lib.TaskType` with `.String()`), `Phase()` (a `*domain.TaskPhase`, nil when absent), `Status()`, `Stage()`, and `CurrentJob()`.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `bborbe/errors` API, sentinel errors with the `stderrors` alias, never `fmt.Errorf`.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md` — public interface + private struct + `New*` constructor, counterfeiter annotations.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — the `## Unreleased` entry format requirement 6 asks for.
- `/workspace/docs/agent-crd-specification.md` — the `spec.taskTypes` field contract.
</context>

<requirements>
1. **Add the fallback on a floor-only seam — do NOT extend `resolveConfig`.** `resolveConfig` has two callers (`parseAndFilter` at line 345 and `reconcileTask` at line 1061), so changing it would route the Kafka consumer path through the new fallback, which this prompt forbids. Leave `resolveConfig` byte-for-byte as it is today (assignee-only) and add:
   ```go
   func (h *taskEventHandler) resolveConfigForReconcile(ctx context.Context, task lib.Task) (*pkg.AgentConfiguration, bool, error)
   ```
   It calls `resolveConfig` first, and only when that returns `(nil, false, nil)` **and** the task's `task_type` is non-empty does it call `h.resolver.ResolveByTaskType(ctx, taskType)`. The fallback behaves as follows:
   - Success → return the resolved `*pkg.AgentConfiguration`.
   - `stderrors.Is(err, pkg.ErrConfigNotFound)` → the task carries no routing information this executor can use. Log at `glog.V(2)` naming the task and the `task_type`, increment `metrics.TaskEventsTotal.WithLabelValues("skipped_unknown_assignee")`, and return `(nil, true, nil)`. **Do not** increment `metrics.SkippedUnknownAssigneeTotal` — that counter is keyed on a routing value and there is none.
   - `stderrors.Is(err, pkg.ErrConfigAmbiguous)` → return the error unchanged so the caller can log it with the task in hand (see requirement 4).
   - Any other error → return it unchanged.
   - Empty `task_type` → return `(nil, false, nil)`, the task carries no routing information at all and stays parked.
   - Give it a doc comment stating why it exists separately from `resolveConfig` — the shared helper serves the Kafka consumer path, which must not gain this fallback.
2. Change `reconcileTask` (line 1053) so a parked task whose declared type resolved is eligible, while keeping every other gate:
   - Replace the final `if task.Frontmatter.Assignee() == "" { return task, nil, false, nil }` (lines 1078-1080) with a gate that admits a task when **either** its routing value is non-empty **or** the configuration was resolved by declared type. A task with neither is still not eligible.
   - Call `h.resolveConfigForReconcile` at line 1061 instead of `h.resolveConfig`. **`parseAndFilter` at line 345 keeps calling `h.resolveConfig`** — do not change that line.
   - The trigger-status, trigger-phase, and stage gates keep running against the resolved configuration exactly as they do now — a parked task outside its configuration's trigger set is still rejected, and `effectiveTriggerPhases` / `effectiveTriggerStatuses` remain the source of truth.
   - **Fix the resolve-error return** at line 1063: it is currently `return lib.Task{}, nil, false, err`, which discards the parsed task so the caller cannot name it. Change it to `return task, nil, false, err`. The caller at line 998 logs `event=reconcile_skip path=%s reason=resolve_error err=%v` — extend that log line (or add a sibling) so the task identifier and the `task_type` appear, which is what makes an ambiguous type diagnosable.
3. Add a `ResultPublisher` method that restores a task's routing value, and implement it on `resultPublisher` (`/workspace/pkg/result_publisher.go`):
   - Name it `PublishRestoreAssignee(ctx context.Context, task lib.Task, assignee string) error`.
   - It builds `taskcmd.UpdateFrontmatterCommand{TaskIdentifier: task.TaskIdentifier, TargetVault: targetVaultFromTask(task), Updates: lib.TaskFrontmatter{"assignee": assignee}}` and sends it via `p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd)`.
   - `Updates` carries **exactly one** key, `assignee`. It must contain no `trigger_count`, no `trigger_scope`, no `current_job`, no `status`, no `phase`, and no `previous_assignee` — the write restores a routing value and nothing else.
   - `PublishTypeMismatchFailure` (defined at line 318, writing the `assignee` key at line 333) is the existing precedent that the controller's update path accepts an `assignee` key; cite it in the doc comment rather than `PublishSpawnNotification`, which writes no `assignee`.
   - Give it a doc comment in the file's existing style, stating what it writes, why it is an `UpdateFrontmatterCommand` rather than a result publish, and that it is idempotent because it writes the same resolved value on every pass until the controller applies it.
4. Make `reconcileOne` (line 983) branch on the parked case **before** the `h.redriveStreakCapped(task, relPath)` check at line 1011 — not between that check and `spawnIfNeeded`:
   - The restore consumes no spawn budget, so the streak cap must not gate it. Placing the branch after the cap check would mean a task the floor already re-drove three times at the current phase returns `reconcileOutcomeCapped` and stays parked forever — which is exactly the re-drive-and-re-park class spec 009 exists to recover.
   - When the task's routing value is empty and the configuration was resolved by declared type: publish the restore via `h.resultPublisher.PublishRestoreAssignee(ctx, task, config.Assignee)`, increment `metrics.ReconcileRedrivenTotal`, log `event=reconcile_redrive task=%s path=%s reason=restore_assignee assignee=%s`, and return `reconcileOutcomeRedriven`. **Do not** call `spawnIfNeeded` and **do not** touch the redrive streak for this case.
   - When the routing value is non-empty, the path is unchanged: the streak cap check, then `spawnIfNeeded`, then the existing outcomes.
   - If `PublishRestoreAssignee` returns an error, log `event=reconcile_skip task=%s reason=restore_error err=%v` and return `reconcileOutcomeSkipped`. Do not fail the pass — the next tick retries.
5. Update the `ReconcileOnce` doc comment (line 887) and the `reconcileTask` doc comment (line 1053) to describe both outcomes: a parked task with a resolvable declared type gets its routing value restored, and every other eligible task is re-driven through the spawn path. The `reconcileTask` doc comment currently lists "empty assignee" among the `ok=false` reasons (line 1035) — correct that sentence.
6. Add a `## Unreleased` entry to `/workspace/CHANGELOG.md` describing the change in the repo's existing style, following `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md`. Read the last two released sections first to match the wording and bullet shape. This is the changelog entry the sibling prompt deliberately deferred.
7. Add specs covering, at minimum, the seven in-scope acceptance criteria of spec 009 — one spec per criterion, each named for the criterion it covers:
   - In `/workspace/pkg/handler/task_reconcile_test.go`:
     - a parked task (empty routing value, `status: in_progress`, a trigger `phase`, no `current_job`, a declared type the fake resolver resolves) publishes exactly one frontmatter update naming that task;
     - the published value is the resolved configuration's assignee, and a fixture carrying `previous_assignee: stale-agent` publishes no command whose value is `stale-agent`;
     - a declared type matching no configuration publishes zero commands for that identifier;
     - a task already carrying a routing value publishes no `assignee` update;
     - fixtures at `status: completed` and `status: aborted` publish nothing;
     - a fixture at `phase: human_review` with a configuration whose trigger set omits it publishes nothing;
     - the published `Updates` map contains no `trigger_count` and no `trigger_scope` key;
     - **a parked task whose declared type the fake resolver reports ambiguous publishes nothing and logs the ambiguity** (spec Failure Modes row 2);
     - **a parked task with an empty routing value and no declared type publishes nothing and spawns nothing** (the no-routing-information case).
   - In `/workspace/pkg/result_publisher_test.go` (`package pkg_test`): a spec for `PublishRestoreAssignee` asserting the emitted command's operation, `TaskIdentifier`, `TargetVault`, and the single-key `Updates` map.
8. Before finishing, prove the change: re-run every bullet in `<verification>` and confirm each passes, then walk spec 009's acceptance criteria 1, 4, 5, 6 and 7 against the new specs — naming each AC and the spec covering it. Report any AC with no covering spec under `## Improvements` rather than leaving it implied.
</requirements>

<constraints>
- Repo conventions are frozen: Ginkgo/Gomega v2 specs in external test packages (`package handler_test` / `package pkg_test`), `github.com/bborbe/errors` wrapping (never `fmt.Errorf`, never `context.Background()` inside `pkg/`), sentinel errors aliased as `stderrors`, counterfeiter mocks (never hand-written mocks).
- **`parseAndFilter` is byte-for-byte unchanged, including its `h.resolveConfig` call at line 345 and its skip labels.** The Kafka consumer path must not reach `ResolveByTaskType`: an ambiguous declared type would otherwise return an error out of `ConsumeMessage` and tear the consumer loop down with the offset unmarked, wedging on redelivery. Only `reconcileTask` calls `resolveConfigForReconcile`.
- **The assignee is never resolved from `previous_assignee`.** It comes from the resolved configuration's assignee field and nowhere else. The operator runbook records a full night lost to exactly this substitution. Note that `PublishTypeMismatchFailure` legitimately *writes* that key on the failure path (line 337) — reading it as a routing source is the error, not its existence.
- Do not write any frontmatter key other than `assignee` on this path. In particular, do not bump `trigger_count`, do not write `trigger_scope`, and do not clear or set `current_job`.
- Do not spawn a Job for a parked task. The restore is the whole action; the resulting event drives the normal spawn path, where every existing guard applies.
- No new metric, no new configuration field, no environment variable, no CLI flag, no schema field. The parked-miss skip reuses the existing `skipped_unknown_assignee` label on `TaskEventsTotal`.
- Do not change `Resolve` (the assignee path) or `ResolveByTaskType` in `/workspace/pkg/config_resolver.go` — the sibling prompt's contract is frozen.
- **`pkg/redrive/redrive_sweep.go` is out of scope.** It carries the same empty-routing-value guard, but spec 009's own Constraints leave it as it is. Do not "helpfully" fix it here.
- Adding a method to the exported `ResultPublisher` interface means `mocks/result_publisher.go` MUST be regenerated (`make generate`) before `make test` can compile. Do not commit a hand-edited mock.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `make test` iteratively after each meaningful change (fast feedback loop), then `make precommit` ONCE at the very end. If `make precommit` fails with a `git rev-parse`/ROOTDIR error, re-run as `make ROOTDIR=/workspace precommit`.

Do NOT run `git`, `docker`, `make build`, `kubectl`, `dark-factory`, or `gh` commands — this container has no such tooling.

- `make generate` — exits 0 and regenerates `mocks/result_publisher.go`.
- `grep -n 'PublishRestoreAssignee' mocks/result_publisher.go` — returns at least one line (the fake carries the new method).
- `grep -c 'PublishRestoreAssignee' pkg/result_publisher.go` — returns at least 2 (the interface declaration and the implementation).
- `grep -n 'ResolveByTaskType' pkg/handler/task_event_handler.go` — returns at least one line (the floor now consumes the sibling prompt's lookup; it had **no** caller before this prompt).
- `grep -n 'resolveConfigForReconcile' pkg/handler/task_event_handler.go` — returns at least 2 lines (definition + the `reconcileTask` call site), and `grep -c 'h.resolveConfig(ctx, task)' pkg/handler/task_event_handler.go` still returns **2** (the unchanged `parseAndFilter` call at line 345 and the call inside `resolveConfigForReconcile`).
- `grep -n 'skipped_assignee' pkg/handler/task_event_handler.go` — returns at least one line (the consumer path's guard is still present).
- `grep -c 'previous_assignee' pkg/handler/task_event_handler.go` — returns `0` (the handler never treats the key as a routing source).
- `grep -n 'previous_assignee' pkg/result_publisher.go` — returns exactly one line, inside `PublishTypeMismatchFailure` (the pre-existing failure-path write). Confirm it is unchanged and that `PublishRestoreAssignee` does not write the key.
- `grep -n 'trigger_count' pkg/result_publisher.go` — every match is inside a method other than `PublishRestoreAssignee`; read the lines and confirm the new method's `Updates` map does not carry it.
- `grep -n '^## Unreleased' CHANGELOG.md` — returns exactly one line, and it is the topmost section heading.
- `make test` — exits 0; the new parked-eligibility specs and all pre-existing reconcile specs are green.
- `make precommit` — exits 0.
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/... && go tool cover -func=/tmp/cover.out | grep -E 'task_event_handler.go|result_publisher.go'` — confirm the new `resolveConfigForReconcile` branches and `PublishRestoreAssignee` are at ≥80% statement coverage.
</verification>
