---
status: completed
spec: [018-bug-trigger-budget-refusal-is-written-to-the-record]
summary: Moved the scoped trigger budget gate into a new file, made it publish a deduped refusal onto the record, and added unit/Ginkgo coverage plus a changelog entry
execution_id: agent-task-executor-trigger-refusal-exec-048-spec-018-handler-trigger-refusal
dark-factory-version: v0.198.7
created: "2026-10-28T11:10:14Z"
queued: "2026-10-28T13:22:18Z"
started: "2026-10-28T13:23:40Z"
completed: "2026-10-28T13:32:46Z"
branch: dark-factory/bug-trigger-budget-refusal-is-written-to-the-record
---

# Handler: record a trigger budget refusal on the task

<summary>
- A record refused by the scoped trigger budget now writes that refusal onto its own task file
- The refusal names the count, the cap and the scope that produced it
- A record that already carries the same refusal is not rewritten, so a refused record converges on one write
- A refusal with different numbers or a different scope is written again and replaces the earlier one
- The refusal never admits the task: the spawn stays suppressed in every case, including a failed write
- A failed write is reported and counted as an error, and the next reconcile tick retries it
- The existing refusal log line and counter are unchanged, so the deployed signal does not move
- Nothing else about the record changes: status, phase, assignee, the job field and both counters are untouched
</summary>

<objective>
Make the scoped trigger budget's refusal attributable on the record: thread `ctx` and an error through `checkTriggerBudget`, publish the refusal through the publisher method added by the previous prompt, and guard the publish on the record's own refusal triple — so a record refused every tick for weeks stops presenting an unrelated older failure as its current state.
</objective>

<context>
Read `pkg/handler/task_event_handler.go` in full (1998 lines). This prompt changes the scoped trigger budget gate and its single call site.

**File-length constraint (load-bearing).** `pkg/handler/task_event_handler.go` is at 1998 lines against revive's `file-length-limit: 2000` (`.golangci.yml`). The new logic therefore lives in a **new file** `pkg/handler/task_event_handler_trigger_refusal.go`, and `checkTriggerBudget` **moves** there. This is the same remedy the completed sibling prompt `prompts/completed/046-reconcile-gate-signal.md` used for the same limit.

Read in `pkg/handler/task_event_handler.go`:
- `triggerScope(fm lib.TaskFrontmatter) string` — produces the scope key `phase + ":" + ref[:8]`.
- `checkTriggerBudget` and the comment block immediately above it. That block currently opens "applyTriggerBudget evaluates ..." (a stale name: no `applyTriggerBudget` function exists) and contains the sentence "It writes nothing." — move the block with the function and correct both statements.
- `recordTriggerSpawn(ctx context.Context, task lib.Task) error` — the signature and error-wrapping pattern `checkTriggerBudget` now follows.
- `spawnIfNeededClaimed` — the single call site, immediately after the `deferIfAtConcurrencyCap` block.
- `recordTriggerSpawn`'s call site in `finishSpawn` (`spawnClaimedJob` calls `finishSpawn`) — the exemplar for incrementing `metrics.TaskEventsTotal.WithLabelValues("error").Inc()` on a failed publish.

Read `pkg/handler/task_event_handler_reconcile_refusal.go` — the sibling gate shipped in v0.47.0. It is the precedent for a refusal helper living in its own file with its own doc comment explaining why the signal is where it is.

Read `pkg/result_publisher.go` — the `PublishTriggerBudgetRefusal(ctx, task, scope, count, maxTriggers)` method added by the previous prompt. Its contract: one `UpdateFrontmatterCommand` carrying `trigger_refusal_count`, `trigger_refusal_max`, `trigger_refusal_scope`, `trigger_refusal_at` and a `## Trigger Budget` body section; deduped in-flight on task id + triple; returns a wrapped error on publish failure and does not remember a failed publish.

Verified library facts (do not re-derive):

```go
// github.com/bborbe/agent — TaskFrontmatter is map[string]interface{}
func (f TaskFrontmatter) TriggerCount() int          // trigger_count, 0 when absent
func (f TaskFrontmatter) MaxTriggers() int           // max_triggers, 3 when absent
func (f TaskFrontmatter) Int(key string) (int, bool) // accepts int AND float64 (YAML-decoded)
func (f TaskFrontmatter) String(key string) (string, bool)
```

Read these coding-plugin guides at `/home/node/.claude/plugins/marketplaces/coding/docs/`:
- `go-error-wrapping-guide.md` — `errors.Wrapf(ctx, err, ...)`, never `fmt.Errorf`, never `context.Background()`
- `go-testing-guide.md` — Ginkgo/Gomega conventions
- `go-glog-guide.md` — the `V(2)` level and `event=` prefix convention

Read the test exemplars:
- `pkg/handler/task_event_handler_test.go` — the `TaskEventHandler` Ginkgo suite (package `handler_test`): its `BeforeEach` builder for `NewTaskEventHandler`, `buildMsg`, and the existing budget specs ("skips spawn when trigger_count >= max_triggers (cap reached)", "keeps capping when the ref is unchanged").
- `pkg/handler/task_event_handler_capped_internal_test.go` — the plain-Go internal test style (package `handler`). Its header comment records the trap this prompt must respect: **a Ginkgo spec declared in an internal test package is never discovered** (the `RunSpecs` bootstrap lives in `package handler_test`), so `make precommit` would pass with the test never run.
- `pkg/result_publisher_test.go` — the `capturingSyncProducer` and `decodeUpdateFrontmatterCommand` helpers the AC5 spec copies (different package, so copy them, do not import).
</context>

<requirements>
1. Create `pkg/handler/task_event_handler_trigger_refusal.go` (BSD license header, `package handler`; imports `context`, `lib "github.com/bborbe/agent"`, `"github.com/bborbe/errors"`, `"github.com/golang/glog"`, `"github.com/bborbe/agent-task-executor/pkg/metrics"`).

2. **Move** `checkTriggerBudget` and its whole comment block out of `pkg/handler/task_event_handler.go` into the new file, and change the signature to:

   ```go
   func (h *taskEventHandler) checkTriggerBudget(ctx context.Context, task lib.Task) (bool, error)
   ```

   `true` means refused (the caller suppresses the spawn); a non-nil error means the refusal could not be written, and the caller still suppresses. Keep the existing eligibility expression, the existing `glog.V(2).Infof("skip task %s: trigger_count %d >= max_triggers %d in scope %q", ...)` line and the existing `metrics.TaskEventsTotal.WithLabelValues("skipped_trigger_cap").Inc()` call byte-for-byte — this change adds a write, it does not move the signal. In the moved comment, collapse the stale opening (`applyTriggerBudget evaluates ... and, when the spawn may proceed, publishes exactly one counter write recording it. Returns capped=true ...`) **and** the already-present `checkTriggerBudget reports whether ... exhausted. It writes nothing.` sentence into one accurate block that states: (a) this function evaluates the scoped cap and returns `(refused, error)`; (b) it publishes **no** counter write — that is `recordTriggerSpawn`'s job — so the old `publishes exactly one counter write recording it` and `Returns capped=true` claims must go rather than be prefixed; (c) a refusal now writes its own reason onto the record beside — never over — any older failure. Keep the remaining paragraphs (default-engage for repo-backed tasks, the `MaxTriggers` cap value, the 2026-08-27 incident) verbatim.

   Also update the two surviving references to the retired name, so no comment names a function that no longer exists: `pkg/handler/task_event_handler.go:84` (the `see applyTriggerBudget` comment) and `pkg/handler/task_reconcile_test.go:327` (the `applyTriggerBudget's cap branch is disarmed` comment).

   In the refusing branch, after the metric increment:

   ```go
   	if err := h.recordTriggerRefusal(ctx, task, currentScope); err != nil {
   		return true, err
   	}
   	return true, nil
   ```

   and the non-refusing return becomes `return false, nil`.

3. Add to the same new file:

   ```go
   // recordTriggerRefusal publishes the refusal onto the record unless the record
   // already carries the same refusal triple.
   func (h *taskEventHandler) recordTriggerRefusal(ctx context.Context, task lib.Task, scope string) error
   ```

   - Read `count := task.Frontmatter.TriggerCount()` and `maxTriggers := task.Frontmatter.MaxTriggers()` — the same two values the `skip task ...` log line prints, so the record's numbers equal the log's numbers.
   - If `triggerRefusalRecorded(task.Frontmatter, scope, count, maxTriggers)` is true: `return nil` (steady state — no publish, no error, the spawn is still suppressed by the caller).
   - Otherwise call `h.resultPublisher.PublishTriggerBudgetRefusal(ctx, task, scope, count, maxTriggers)`; on error return `errors.Wrapf(ctx, err, "publish trigger budget refusal for task %s", task.TaskIdentifier)`.
   - This is the record-state half of the two required dedupe layers. It makes the write once-per-state in the steady state; it cannot cover the window between publish and the controller applying the write, which is why the publisher also dedupes.

   ```go
   // triggerRefusalRecorded reports whether the record already carries this exact
   // refusal triple. trigger_refusal_at is deliberately not compared: it changes on
   // every write and would defeat the guard.
   func triggerRefusalRecorded(fm lib.TaskFrontmatter, scope string, count int, maxTriggers int) bool
   ```

   Compare `fm.Int("trigger_refusal_count")`, `fm.Int("trigger_refusal_max")` and `fm.String("trigger_refusal_scope")` against `count`, `maxTriggers` and `scope`, requiring all three keys present. Use `Int` (not a type assertion) so a value read back as `float64` after YAML/JSON decoding still matches — a `trigger_refusal_count` written as an `int` and read back from the vault arrives as `float64`.

4. Update the single call site in `spawnIfNeededClaimed` (`pkg/handler/task_event_handler.go`), which today reads:

   ```go
   	// Scoped trigger budget: check only. The counter write is published by
   	// recordTriggerSpawn after SpawnJob succeeds, so an infrastructure failure
   	// cannot burn one of the task's attempts.
   	if h.checkTriggerBudget(task) {
   		return spawnOutcomeSuppressed, nil
   	}
   ```

   to:

   ```go
   	// Scoped trigger budget: check only. The counter write is published by
   	// recordTriggerSpawn after SpawnJob succeeds, so an infrastructure failure
   	// cannot burn one of the task's attempts. A refusal writes its own reason
   	// onto the record; a failed write still suppresses the spawn.
   	refused, err := h.checkTriggerBudget(ctx, task)
   	if err != nil {
   		metrics.TaskEventsTotal.WithLabelValues("error").Inc()
   		return spawnOutcomeSuppressed, err
   	}
   	if refused {
   		return spawnOutcomeSuppressed, nil
   	}
   ```

   `err` is already declared in this function's scope — first bound by `active, err := h.isJobActive(...)` and reused by `capped, err := h.deferIfAtConcurrencyCap(...)` — so `:=` with the new `refused` is valid. Do not move, reorder or change any other gate in the function. After this edit `pkg/handler/task_event_handler.go` must be **under 2000 lines** (`wc -l`).

5. Add `pkg/handler/task_event_handler_trigger_refusal_internal_test.go` — **plain Go `testing`, package `handler`**, with a header comment stating why it is not Ginkgo (an internal-package Ginkgo spec is never discovered; see `task_event_handler_capped_internal_test.go`). Name every test function with the `TestCheckTriggerBudget` prefix (`TestCheckTriggerBudgetPublishesRefusal`, `TestCheckTriggerBudgetSkipsRecordedTriple`, ...) so the verification's `-run 'TestCheckTriggerBudget'` selects exactly this file. Construct the handler as `h := &taskEventHandler{resultPublisher: pub}` — `checkTriggerBudget` reads no other field — with a recording publisher that embeds the package's existing `stubResultPublisher` (`claim_spawn_order_internal_test.go`) and overrides `PublishTriggerBudgetRefusal` to append `(taskID, scope, count, maxTriggers)` and return a configured error. Cases:
   Every case below builds the frontmatter with **`phase: "planning"`**. It is load-bearing, not decoration: `triggerScope` composes the current scope as `fm.Phase() + ":" + ref[:8]`, so a fixture without `phase` yields `":4963ffa6"` — mismatching any stored `trigger_scope`, so `scopeChanged` becomes true and the record is not refused — and the case would then assert the opposite of what it claims.
   - **at cap, no refusal keys:** `phase: "planning"`, `trigger_count: 4`, `max_triggers: 3`, `trigger_scope: "planning:4963ffa6"`, `ref: "4963ffa6aaaaaaaaaaaaaaaa"` → `(true, nil)`, exactly one publish with `scope == "planning:4963ffa6"`, `count == 4`, `maxTriggers == 3`.
   - **at cap, record already carries the same triple as ints:** add `trigger_refusal_count: 4`, `trigger_refusal_max: 3`, `trigger_refusal_scope: "planning:4963ffa6"` → `(true, nil)`, zero publishes.
   - **same triple decoded as float64:** same keys but values `float64(4)`, `float64(3)` (and the scope string) → `(true, nil)`, zero publishes. This is the serialization boundary assertion.
   - **at cap, record carries an older triple:** stored `trigger_refusal_scope: "planning:deadbeef"` (and matching count/cap) → `(true, nil)`, exactly one publish carrying the *current* scope.
   - **below cap:** `trigger_count: 2`, `max_triggers: 3` → `(false, nil)`, zero publishes.
   - **reset record, stale triple:** the case-1 fixture with `trigger_count: 2` (so `ref` and `trigger_scope: "planning:4963ffa6"` are inherited, which keeps the published scope unambiguous), plus `trigger_refusal_count: 4`, `trigger_refusal_max: 3`, `trigger_refusal_scope: "planning:4963ffa6"` → `(false, nil)`, zero publishes — the stale `## Trigger Budget` section survives a record whose counter was reset rather than being rewritten on a non-refusal. Then the same record with `trigger_count: 3` → `(true, nil)` and exactly one publish carrying the new triple. This is the spec's failure-mode row 6.
   - **publish fails:** publisher returns `errors.New(ctx, "kafka unavailable")` → `(true, non-nil error)`, exactly one attempted publish. The spawn-suppression half is the returned `true`.

6. Add `pkg/handler/task_event_handler_trigger_refusal_test.go` — **Ginkgo, package `handler_test`** (this is where the suite is discovered). It holds a `capturingSyncProducer` implementing `libkafka.SyncProducer` (`SendMessage`, `SendMessages`, `Close`) and a `decodeUpdateFrontmatterCommand` helper, both copied from `pkg/result_publisher_test.go` (they are already Gomega-based; copy, do not import — different package). Then exactly one spec whose text begins **"publishes one trigger budget refusal"** — the acceptance criterion greps for that string:

   ```go
   var _ = Describe("Trigger budget refusal write", func() {
   	It("publishes one trigger budget refusal carrying the four keys and a ## Trigger Budget section", func() { ... })
   })
   ```

   Build a real publisher, not the fake, so the assertion covers the whole dispatch path:
   `pkg.NewResultPublisher(producer, base.TopicPrefix("develop"), currentDateTime, nil)` with `currentDateTime.SetNow(libtimetest.ParseDateTime("2026-10-28T11:00:00Z"))`, and pass it to `handler.NewTaskEventHandler` (same positional arguments as the suite's `BeforeEach`: spawner, `"dev"`, `base.Branch("prod")`, `""`, `true`, `true`, resolver, publisher, `pkg.NewTaskStore()`, `currentDateTime`, view, `testTaskVault`, `handler.DefaultReconcilePassTimeout`, `handler.DefaultReconcileInterval`, `pkg.ClaimModeOff`, `nil`). Configure `fakeSpawner.IsJobActiveReturns(false, nil)`, `fakeResolver.ResolveReturns(pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"}, nil)`, and a ready view (`ReadyReturns(true)`, `SnapshotReturns(nil)`, `GetReturns(lib.Task{}, false)`).

   Drive `ConsumeMessage` with the reproduction shape from the spec: `phase: planning`, `trigger_scope: "planning:4963ffa6"`, `ref: "4963ffa6aaaaaaaaaaaaaaaa"`, `trigger_count: 4`, `max_triggers: 3`, `status: in_progress`, `assignee: claude`, `stage: prod`, and a stamped **`Vault: testTaskVault`**. ⚠️ **The stamp is load-bearing:** `spawnIfNeededClaimed` calls `skipForeignVaultTask` before every other gate, and `VaultMatcher.Accepts("")` is false, so an unstamped task is dropped as foreign and nothing is ever published — the `SpawnJobCallCount() == 0` assertion would then pass for entirely the wrong reason. Do **not** try to reuse the suite's `buildMsg`: it is a closure declared inside `task_event_handler_test.go`'s `Describe("TaskEventHandler")` body and is not visible from another file. Declare a local `buildMsg` in the new file, copying that closure (`if task.Vault == "" { task.Vault = testTaskVault }`, then `json.Marshal`); `testTaskVault` is a package-level const in the `handler_test` package (`pkg/handler/task_event_handler_test.go:45`) and is reachable. Assert:
   - `SpawnJobCallCount() == 0`;
   - exactly one captured message, whose operation is `taskcmd.UpdateFrontmatterCommandOperation`;
   - `cmd.Updates` has exactly 4 entries: `trigger_refusal_count` numerically 4, `trigger_refusal_max` numerically 3, `trigger_refusal_scope` `"planning:4963ffa6"`, `trigger_refusal_at` `"2026-10-28T11:00:00Z"`;
   - `time.Parse(time.RFC3339, at)` succeeds (the vault-normalisation boundary);
   - `cmd.Body` is non-nil, `cmd.Body.Heading == "## Trigger Budget"` (and is not `"## Failure"`), and `cmd.Body.Section` contains `trigger_count 4 >= max_triggers 3` and `planning:4963ffa6`.

7. Add one bullet under the existing empty `## Unreleased` heading in `CHANGELOG.md`, in the file's voice (`- fix: **bold summary.** prose`), describing that a trigger-budget refusal now writes its count, cap, scope and timestamp onto the record plus a `## Trigger Budget` section, that the write is once per refusal triple (record guard plus in-flight dedupe), that the older `## Failure` section and every other key are untouched, and that the existing `skipped_trigger_cap` counter and log line are unchanged. Do not touch any released section.

8. Before finishing, re-run `<verification>` and confirm it passes; walk each requirement and the spec's Failure Modes table against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
- **The section heading must not be `## Failure`.** `taskcmd.BodySection` is applied by `Markdown.ReplaceSection`, which replaces a section carrying the same heading; writing `## Failure` from this path would delete the older cause.
- **Two dedupe layers, both required.** The record-state guard (skip when the record already carries the same count/cap/scope) is what makes the write once-per-state in the steady state. It cannot cover the window between publish and the controller applying the write, because the handler reads an informer snapshot that does not yet show it — so the publisher additionally dedupes through the same TTL'd LRU as `PublishAbort`, keyed on task id **and** the refusal triple so a changed scope is never suppressed.
- The frontmatter key names are frozen: `trigger_refusal_count`, `trigger_refusal_max`, `trigger_refusal_scope`, `trigger_refusal_at`. `trigger_refusal_at` is RFC3339.
- **Eligibility is unchanged:** the same conditions that refused before still refuse, the spawn is still suppressed, and `trigger_count` is neither incremented nor reset by this write.
- `metrics.TaskEventsTotal.WithLabelValues("skipped_trigger_cap").Inc()` and the existing `V(2)` refusal line stay exactly as they are; this change adds a write, it does not move the signal.
- A publish failure must never admit a refused task: `checkTriggerBudget` returns the wrapped error and the spawn is still suppressed. The call site increments `metrics.TaskEventsTotal.WithLabelValues("error").Inc()`.
- No `context.Background()`, no `fmt.Errorf`, no bare `return err`.
- Do not add a config knob, a new metric, a new counter label, or a new log line for the successful write.
- `pkg/handler/task_event_handler.go` must stay under revive's 2000-line limit; all new logic goes in the new file.
- Tests are Ginkgo/Gomega where the suite discovers them (`package handler_test`); the unexported gate's unit tests are plain Go in `package handler` because a Ginkgo spec in an internal package is never discovered.
</constraints>

<verification>
- `ROOTDIR=/workspace make precommit` — must pass.
- `go test ./pkg/handler/... -mod=mod -v -ginkgo.v -ginkgo.focus='publishes one trigger budget refusal'` — must pass and print `Ran 1 of` (without `-v` `go test` discards the Ginkgo summary on success, so the count would be invisible).
- `go test ./pkg/handler/ -mod=mod -run 'TestCheckTriggerBudget' -v` — must pass.
- `grep -n 'trigger_refusal_count' pkg/result_publisher.go` — returns at least one line.
- `grep -n 'Trigger Budget' pkg/result_publisher.go` — returns at least one line.
- `grep -rn 'Heading: "## Failure"' pkg/ | wc -l` — prints `2` (the two pre-existing sites; this change adds no third).
- `wc -l pkg/handler/task_event_handler.go` — prints a number below `2000`.
</verification>
