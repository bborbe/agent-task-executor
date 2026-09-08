---
status: completed
spec: [006-bug-trigger-cap-never-engages-for-repo-backed-tasks]
summary: 'Default-engage the scoped spawn-trigger cap for repo-backed tasks: applyTriggerBudget now engages when ref is present even with max_triggers absent (via Frontmatter.String("ref")), the doc/call-site comments state the default-engage rule, three new Ginkgo specs lock the behavior, the recurring-task regression guard comment was updated, a CHANGELOG Unreleased entry was added, and the oversized handler test file was split into task_event_handler_deferred_test.go.'
execution_id: agent-task-executor-default-cap-exec-010-spec-006-default-engage-trigger-cap
dark-factory-version: dev
created: "2026-09-08T21:15:00Z"
queued: "2026-09-08T19:22:19Z"
started: "2026-09-08T19:22:20Z"
completed: "2026-09-08T19:33:03Z"
branch: dark-factory/bug-trigger-cap-never-engages-for-repo-backed-tasks
---

<summary>
- A repo-backed task (one whose frontmatter carries a `ref`) is now capped by default: it may not spawn more than the lib default of 3 jobs in the same `phase`+`ref` scope, even when the producing watcher never writes `max_triggers`.
- The `github-update-go` watcher — the producer that triggered the live uncapped loop — engages the cap without any configuration change.
- An explicit `max_triggers` still overrides the default, with or without a `ref`; the cap value always comes from `MaxTriggers()` (field value, lib default 3 when absent).
- A recurring task (no `ref`, no `max_triggers`) stays uncapped exactly as before — the v0.7.1 Daily Sentry Triage regression guard holds.
- Scope-change and scope-adoption semantics are unchanged: a changed `phase` or `ref` resets the budget; an absent `trigger_scope` adopts the current scope carrying the count forward.
- Three new Ginkgo specs lock the behavior: repo-backed at default cap is skipped, repo-backed below the cap spawns and increments, and an explicit `max_triggers` above the default wins over the default-3 for a repo-backed task.
- The engagement-gate doc comment is rewritten to state the default-engage rule (no stale "absent max_triggers still means no cap" claim).
- A `## Unreleased` fix bullet is added to the CHANGELOG.
</summary>

<objective>
Make the scoped spawn-trigger cap default-engage for repo-backed tasks: a task whose frontmatter carries a `ref` is capped at the lib default of 3 spawns per `phase`+`ref` scope even when `max_triggers` is absent, while an explicit `max_triggers` still overrides the default and a task with neither `ref` nor `max_triggers` (the recurring-task shape) stays uncapped. This closes the live uncapped-respawn loop for the `github-update-go` watcher (a task with a deterministically failing planning gate respawned 4x in ~27 min on 2026-09-08) without touching the v0.7.1 regression guard.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for project conventions (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, counterfeiter mocks, glog `V(n)` gating, coverage ≥80% for new code).

Read these files fully before changing anything:
- `/workspace/pkg/handler/task_event_handler.go` — `applyTriggerBudget` (lines ~987-1056, the gate at lines ~1019-1030 and the "The cap stays OPT-IN" comment at line ~991), `triggerScope` (lines ~975-985), `triggerBudgetWrite` (lines ~1058-1078), and the `spawnIfNeeded` call-site comment at lines ~557-559 ("evaluates the opt-in cap"). The only call site of `applyTriggerBudget` is `spawnIfNeeded` (line ~560).
- `/workspace/pkg/handler/task_event_handler_test.go` — the cap/scope Ginkgo spec block (lines ~503-827), especially the recurring-task regression guard spec at lines ~571-606 (which carries the stale "the cap stays opt-in rather than being flipped default-on" comment at lines ~579-583), and the `BeforeEach` harness at lines ~41-82 showing how `h`, `fakeSpawner`, `fakeResultPublisher`, `buildMsg`, `ctx`, and `domain.TaskPhase*` are wired.
- `/home/node/go/pkg/mod/github.com/bborbe/agent@v0.86.0/agent_task-frontmatter.go` — the lib accessors this code depends on: `func (f TaskFrontmatter) String(key string) (string, bool)` (line ~144; `ok` is false when the key is absent or non-string), `func (f TaskFrontmatter) MaxTriggers() int` (line ~101; returns the `max_triggers` field value, or 3 when absent), `func (f TaskFrontmatter) TriggerCount() int` (line ~88), `func (f TaskFrontmatter) Phase() *domain.TaskPhase` (line ~26).

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega, external test packages, coverage ≥80%.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-doc-best-practices.md` — GoDoc comment style for the rewritten comment.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — the existing `errors.Wrapf(ctx, err, ...)` idiom stays untouched.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` format and prefix rules.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new code.
</context>

<requirements>
1. Rewrite the `applyTriggerBudget` doc comment in `/workspace/pkg/handler/task_event_handler.go`. Replace the entire comment block at lines ~987-1004 (from `// applyTriggerBudget evaluates the scoped trigger cap...` through the paragraph ending `...without it leaking into the next phase or the next commit.`) with the following — it must state the default-engage rule and must contain the tokens `default-engage` and `repo-backed` (acceptance-criterion grep evidence). It MUST NOT contain the phrase `absent max_triggers still means no cap` (acceptance-criterion `grep -c` must return 0):
   ```go
   // applyTriggerBudget evaluates the scoped trigger cap for one spawn decision and,
   // when the spawn may proceed, publishes exactly one counter write recording it.
   // Returns capped=true when the cap is reached and the spawn must be skipped.
   //
   // The cap default-engages for repo-backed tasks: a task whose frontmatter
   // carries a ref (written by the agents that clone a repo, alongside clone_url
   // and base_ref) scopes its budget to phase+ref via triggerScope, and that scope
   // moves with the repo, so accrual against a constant scope cannot occur — the
   // safe default-on form. An explicit max_triggers engages the cap for any task,
   // with or without a ref. A task with neither ref nor max_triggers is a
   // recurring task: it is not repo-backed, sits at a stable phase, and its scope
   // is a constant, so a default cap would accrue across re-dispatches and fire at
   // 3, re-creating the 2026-08-27 Daily Sentry Triage incident (the v0.7.1
   // regression). That task stays uncapped by default.
   //
   // The cap value is always MaxTriggers(): the explicit max_triggers field, or
   // the lib default of 3 when it is absent — a repo-backed task with no
   // max_triggers is therefore capped at 3.
   //
   // Scoping also fixes the opt-in cap's correctness: a task that opts in no
   // longer burns its budget across unrelated attempts, so an operator can set a
   // tight max_triggers without it leaking into the next phase or the next commit.
   ```

2. Change the engagement condition in `applyTriggerBudget` (lines ~1019-1030). The gate engages when `max_triggers` is present OR `ref` is present; the cap value is `task.Frontmatter.MaxTriggers()` exactly as today — do NOT write any default constant, the lib returns 3 when `max_triggers` is absent (verified in `agent_task-frontmatter.go`). Current:
   ```go
   	_, optedIn := task.Frontmatter["max_triggers"]
   	if optedIn && !scopeChanged &&
   		task.Frontmatter.TriggerCount() >= task.Frontmatter.MaxTriggers() {
   ```
   New:
   ```go
   	_, optedIn := task.Frontmatter["max_triggers"]
   	_, hasRef := task.Frontmatter.String("ref")
   	if (optedIn || hasRef) && !scopeChanged &&
   		task.Frontmatter.TriggerCount() >= task.Frontmatter.MaxTriggers() {
   ```
   Keep the `glog.V(2).Infof` skip log line, `metrics.TaskEventsTotal.WithLabelValues("skipped_trigger_cap").Inc()`, and `return true, nil` unchanged. Do NOT change `triggerScope`, `triggerBudgetWrite`, or any `PublishIncrementTriggerCount`/`PublishSetTriggerScope` call. This is a pure gate-condition change — no new imports, no new fields, no signature change (the only call site, `spawnIfNeeded` at line ~560, keeps compiling untouched).

3. Update the `spawnIfNeeded` call-site comment in the same file (lines ~557-559) so it no longer says "opt-in cap". Replace `// Scoped trigger budget: evaluates the opt-in cap against the task's current` with `// Scoped trigger budget: evaluates the spawn-trigger cap (default-engages on` and keep the rest of the comment on the next line as `// phase+ref scope and publishes the counter write for the spawn about to`. The exact wording may be adjusted for line length, but the word "opt-in" must no longer appear there.

4. Add three new Ginkgo specs to `/workspace/pkg/handler/task_event_handler_test.go` inside the same `Describe("ConsumeMessage", ...)` block, adjacent to the existing cap specs (e.g. immediately after the `keeps capping when the ref is unchanged (deterministic gate failure)` spec, which ends around line ~658). Mirror the exact existing pattern: `fakeSpawner.IsJobActiveReturns(false, nil)`, a `lib.Task` literal with `TaskIdentifier`/`Frontmatter`, `h.ConsumeMessage(ctx, buildMsg(task))`, and mock call-count assertions. Use the `domain.TaskPhasePlanning` constant already imported. Note the ref `314c4b32aaaaaaaaaaaaaaaa` truncates to `314c4b32` by `triggerScope` (`triggerScopeRefLen = 8`), matching the stored `trigger_scope` `planning:314c4b32`.

   a. Acceptance Criterion 1 (repo-backed at default cap, skipped — the bug shape):
   ```go
   It("skips spawn for a repo-backed task at the default cap (ref present, max_triggers absent)", func() {
   	fakeSpawner.IsJobActiveReturns(false, nil)
   	// The bug shape: the github-update-go watcher emits a ref but never
   	// max_triggers, so the cap never engaged and a deterministically failing
   	// planning gate respawned unbounded. ref presence now default-engages the
   	// cap at the lib default of 3.
   	task := lib.Task{
   		TaskIdentifier: lib.TaskIdentifier("test-task-cap-repo-atcap"),
   		Frontmatter: lib.TaskFrontmatter{
   			"status":        "in_progress",
   			"phase":         string(domain.TaskPhasePlanning),
   			"trigger_scope": "planning:314c4b32",
   			"ref":           "314c4b32aaaaaaaaaaaaaaaa",
   			"assignee":      "github-update-go-agent",
   			"stage":         "prod",
   			"trigger_count": 3,
   		},
   	}
   	err := h.ConsumeMessage(ctx, buildMsg(task))
   	Expect(err).To(BeNil())
   	Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(0))
   	Expect(fakeResultPublisher.PublishSetTriggerScopeCallCount()).To(Equal(0))
   	Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
   })
   ```

   b. Acceptance Criterion 2 (repo-backed below the default cap, spawns and increments):
   ```go
   It("spawns and increments for a repo-backed task below the default cap (ref present, max_triggers absent)", func() {
   	fakeSpawner.IsJobActiveReturns(false, nil)
   	fakeSpawner.SpawnJobReturns("github-update-go-job-1", nil)
   	task := lib.Task{
   		TaskIdentifier: lib.TaskIdentifier("test-task-cap-repo-below"),
   		Frontmatter: lib.TaskFrontmatter{
   			"status":        "in_progress",
   			"phase":         string(domain.TaskPhasePlanning),
   			"trigger_scope": "planning:314c4b32",
   			"ref":           "314c4b32aaaaaaaaaaaaaaaa",
   			"assignee":      "github-update-go-agent",
   			"stage":         "prod",
   			"trigger_count": 2,
   		},
   	}
   	err := h.ConsumeMessage(ctx, buildMsg(task))
   	Expect(err).To(BeNil())
   	Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(1))
   	Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
   })
   ```

   c. Goal requirement (an explicit `max_triggers` overrides the lib default of 3 for a repo-backed task — count 4 with `max_triggers: 5` must spawn, proving the explicit value beats default-3):
   ```go
   It("uses an explicit max_triggers above the default for a repo-backed task (ref present)", func() {
   	fakeSpawner.IsJobActiveReturns(false, nil)
   	fakeSpawner.SpawnJobReturns("github-update-go-job-2", nil)
   	task := lib.Task{
   		TaskIdentifier: lib.TaskIdentifier("test-task-cap-repo-override"),
   		Frontmatter: lib.TaskFrontmatter{
   			"status":        "in_progress",
   			"phase":         string(domain.TaskPhasePlanning),
   			"trigger_scope": "planning:314c4b32",
   			"ref":           "314c4b32aaaaaaaaaaaaaaaa",
   			"assignee":      "github-update-go-agent",
   			"stage":         "prod",
   			"trigger_count": 4,
   			"max_triggers":  5,
   		},
   	}
   	err := h.ConsumeMessage(ctx, buildMsg(task))
   	Expect(err).To(BeNil())
   	Expect(fakeResultPublisher.PublishIncrementTriggerCountCallCount()).To(Equal(1))
   	Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
   })
   ```

5. Update the stale comment inside the existing recurring-task regression guard spec (`does not skip spawn when max_triggers is absent (recurring task)`, lines ~571-606). Replace the comment at lines ~579-583:
   ```
   // Scoping must NOT change this. A recurring task is not repo-backed: it
   // carries no ref and sits at a stable phase, so its scope is constant
   // and would never earn a reset. That is precisely why the cap stays
   // opt-in rather than being flipped default-on.
   ```
   with:
   ```
   // Scoping must NOT change this. A recurring task is not repo-backed: it
   // carries no ref and sits at a stable phase, so its scope is constant and
   // would never earn a reset. The default-engage gate keys on ref presence,
   // so this task (no ref, no max_triggers) stays uncapped — the v0.7.1
   // regression guard.
   ```
   Do NOT change any assertion in this spec — `Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))`, `PublishIncrementTriggerCountCallCount() == 0`, `PublishSetTriggerScopeCallCount() == 1` with scope `"planning:"` and count `6` all stay. This is the acceptance-criterion-3 regression guard.

6. Do NOT modify the other existing cap/scope specs — they must all stay green under the new gate. This is expected and is exactly the acceptance evidence: `skips spawn when trigger_count >= max_triggers (cap reached)` (AC4, `max_triggers` opt-in without `ref`), `resets the budget when the ref changes (new commit earns a retry)` (AC5, scope change), `keeps capping when the ref is unchanged`, both scope-adoption specs, and the happy-path/error-path specs (none carries a `ref`, so the new `hasRef` disjunct does not engage them).

7. Add the CHANGELOG entry in `/workspace/CHANGELOG.md`. Create `## Unreleased` between the `All notable changes...` preamble line and the `## v0.13.1` heading (directly above the highest `## vX.Y.Z`, never above the `# Changelog` title or inside the preamble — per the changelog-guide preamble-frozen rule) with one bullet following the changelog-guide format:
   ```
   ## Unreleased

   - fix: default-engage the scoped spawn-trigger cap for repo-backed tasks — a task whose frontmatter carries a `ref` is now capped at the lib default of 3 spawns per `phase`+`ref` scope even when `max_triggers` is absent, closing the uncapped respawn loop for the `github-update-go` watcher; an explicit `max_triggers` still overrides the default, and recurring tasks (no `ref`, no `max_triggers`) stay uncapped
   ```
</requirements>

<constraints>
- Repo conventions are frozen: Ginkgo/Gomega v2 tests (no stdlib table tests), `github.com/bborbe/errors` wrapping (never `fmt.Errorf`), counterfeiter mocks for new dependencies, glog `V(n)` gating.
- The recurring-task regression guard MUST hold: a task with **no `ref` and no `max_triggers`** (the v0.7.1 / 2026-08-27 Daily Sentry Triage shape) stays uncapped. The existing `does not skip spawn when max_triggers is absent (recurring task)` spec (`test-task-cap-absent`, `trigger_count` 5) keeps asserting `SpawnJobCallCount() == 1` — do not change any of its assertions.
- A task that carries `ref` but also an explicit `max_triggers` keeps the explicit value (the lib `MaxTriggers()` returns the field value; default 3 only when absent — `github.com/bborbe/agent v0.86.0` `agent_task-frontmatter.go`).
- Scope-change semantics unchanged: a changed `phase` or `ref` resets the budget (fresh count 1).
- Scope adoption semantics unchanged: an absent `trigger_scope` adopts the current scope carrying the count forward — never a reset.
- The default-engage gate is `ref` presence via `Frontmatter.String("ref")` — the same key `triggerScope` uses to build `<phase>:<ref[:8]>`. The explicit `max_triggers` opt-in path is retained: the cap engages when `ref` is present OR `max_triggers` is present. A task with no `ref` scopes on phase alone (constant) and stays uncapped only when `max_triggers` is also absent.
- CHANGELOG: add an `## Unreleased` bullet for this fix (section currently absent — HEAD is `## v0.13.1`).
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `make test` iteratively after each meaningful change (fast feedback loop), then `make precommit` ONCE at the very end. If `make precommit` fails with a `git rev-parse`/ROOTDIR error (the container's `.git` may be masked), re-run as `make ROOTDIR=/workspace precommit`.

- `make test` — exits 0; the new and existing cap specs above are green.
- `make precommit` — exits 0.
- `grep -nE 'String\\("ref"\\)|default-engage|repo-backed' pkg/handler/task_event_handler.go` — returns at least one line stating the default-engage rule (acceptance-criterion-6 evidence).
- `grep -c 'absent max_triggers still means no cap' pkg/handler/task_event_handler.go` — returns 0 (acceptance-criterion-6 evidence).
- `grep -n 'test-task-cap-repo-atcap\|test-task-cap-repo-below\|test-task-cap-repo-override' pkg/handler/task_event_handler_test.go` — shows the three new specs are present.
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/handler/... && go tool cover -func=/tmp/cover.out` — confirm the changed `applyTriggerBudget` gate branch is exercised by the new specs (the capped and uncapped sides are each covered).

Do NOT run `docker`, `make build`, `kubectl`, `dark-factory`, or `gh` commands in this prompt — those are operator-executable and belong on the spec's verification ladder (`git show --stat HEAD | grep task_event_handler.go` and `gh pr diff | grep 'String("ref")'` are the operator-side acceptance checks).
</verification>
