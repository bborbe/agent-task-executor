---
status: completed
spec: [018-bug-trigger-budget-refusal-is-written-to-the-record]
summary: 'Added ResultPublisher.PublishTriggerBudgetRefusal writing the four trigger_refusal_* keys and a ## Trigger Budget section with triple-keyed TTL dedupe, regenerated mocks, and covered it with six specs'
execution_id: agent-task-executor-trigger-refusal-exec-047-spec-018-publisher-trigger-refusal
dark-factory-version: v0.198.7
created: "2026-10-28T11:10:14Z"
queued: "2026-10-28T12:20:20Z"
started: "2026-10-28T12:20:21Z"
completed: "2026-10-28T12:30:40Z"
branch: dark-factory/bug-trigger-budget-refusal-is-written-to-the-record
---

# Publisher: write a trigger budget refusal onto the task

<summary>
- A record refused by the scoped trigger budget can now state that refusal on its own task file
- The refusal records the count, the cap, the scope and the moment of refusal as four frontmatter keys
- A human-readable section headed "Trigger Budget" restates the same three numbers in prose
- Repeating the identical refusal does not rewrite the record, so a refused record converges on one write
- A changed refusal (different count, cap or scope) is written again and replaces the earlier one
- The older failure section is never touched by this write, so the previous cause stays visible
- A failed publish is reported to the caller and is not remembered as done, so the next tick can retry
- Existing publisher methods and the existing refusal signal (log line and counter) are unchanged
</summary>

<objective>
Give the result publisher a way to write a trigger-budget refusal onto the task record — the four frozen `trigger_refusal_*` keys plus a `## Trigger Budget` section — with an in-flight dedupe keyed on the task id and the refusal triple, so a permanently refused record stops asserting an unrelated older failure and becomes attributable to the budget.
</objective>

<context>
Read `pkg/result_publisher.go` in full (830 lines). This prompt adds one method to the `ResultPublisher` interface and its implementation. Follow the existing methods exactly:

- `PublishFailure` — the exemplar for "Updates map + `taskcmd.BodySection` in one command", the `errors.Wrapf(ctx, err, "... for task %s", ...)` wrapping style, and the `## Failure` body format.
- `PublishAbort` — the exemplar for the TTL'd LRU dedupe: it builds a namespaced key (`"abort:" + task identifier`), checks `p.dedupe.checkDedupe(key)`, logs at `V(2)` on a hit, and calls `p.dedupe.recordDedupe(key)` **only after** `publishRaw` succeeds. Its doc comment states the trap this prompt repeats: "a write the controller has not yet applied must not be re-published each time".
- `PublishSetTriggerScope` / `PublishRestoreAssignee` — the exemplar for a write that must land on the on-disk frontmatter directly (`UpdateFrontmatterCommand`, not a result publish).
- `ttlDedupe` / `newDedupe` / `checkDedupe` / `recordDedupe` — the dedupe store and its capacity/TTL constants.
- `targetVaultFromTask` — every command sets `TargetVault: targetVaultFromTask(task)`.

Verified library facts (do not re-derive, do not "improve"):

```go
// github.com/bborbe/agent/command/task
type BodySection struct {
	Heading string `json:"heading"` // MUST include the markdown prefix, e.g. "## Failure"
	Section string `json:"section"` // MUST include the heading as its first line and a trailing newline
}

type UpdateFrontmatterCommand struct {
	TaskIdentifier lib.TaskIdentifier  `json:"taskIdentifier"`
	Updates        lib.TaskFrontmatter `json:"updates"`
	Body           *BodySection        `json:"body,omitempty"`
	TargetVault    string              `json:"targetVault,omitempty"`
}
```

`taskcmd.BodySection` is applied by `Markdown.ReplaceSection`, which replaces a section carrying the same heading. Writing `## Failure` from this path would delete the older cause — the exact outcome the spec forbids.

Read `pkg/result_publisher_test.go` (package `pkg_test`) for the test harness this prompt extends: `capturingSyncProducer`, `failingSyncProducer`, `partialFailingSyncProducer`, `decodeUpdateFrontmatterCommand`, and the `ResultPublisher` `Describe` block whose `BeforeEach` builds a publisher with a fixed clock (`currentDateTime.SetNow(libtimetest.ParseDateTime("2026-04-18T12:00:00Z"))`) and `base.TopicPrefix("develop")`.

Read these coding-plugin guides at `/home/node/.claude/plugins/marketplaces/coding/docs/`:
- `go-error-wrapping-guide.md` — `errors.Wrapf(ctx, err, ...)`, never `fmt.Errorf`, never `context.Background()`
- `go-patterns.md` — the interface + private struct + `New*` shape this file already follows
- `go-testing-guide.md` — Ginkgo/Gomega suite conventions in this repo
- `go-glog-guide.md` — the `V(2)` level and the `event=` prefix convention the dedupe-hit line follows
</context>

<requirements>
1. In `pkg/result_publisher.go`, add this method to the `ResultPublisher` interface, placed after `PublishSetTriggerScope` and before `PublishTypeMismatchFailure`. Give it a GoDoc comment in the file's existing voice that states, in prose: it writes the four `trigger_refusal_*` keys and a `## Trigger Budget` body section and no other key; it never writes `## Failure`; it is deduped per task **and** refusal triple through the same TTL'd LRU as `PublishAbort`, because the handler reads an informer snapshot that does not yet show a write the controller has not applied.

   ```go
   PublishTriggerBudgetRefusal(ctx context.Context, task lib.Task, scope string, count int, maxTriggers int) error
   ```

2. Implement it on `*resultPublisher` in the same file, after `PublishSetTriggerScope`. Contract:
   - Dedupe key: `fmt.Sprintf("trigger_refusal:%s:%s:%d:%d", task.TaskIdentifier, scope, count, maxTriggers)`. Keying on the triple (not the task id alone) is load-bearing: a changed scope is a changed refusal and must never be suppressed. Put the key construction in a small unexported helper `triggerRefusalDedupeKey(id lib.TaskIdentifier, scope string, count int, maxTriggers int) string` so the key shape has exactly one home. Do **not** write a direct unit test for the helper: `pkg/result_publisher_test.go` is `package pkg_test`, so an unexported `pkg` symbol is unreachable there and a `pkg.triggerRefusalDedupeKey(...)` call would not compile — the two dedupe specs in requirement 5 pin its behaviour through the exported method instead. (If a direct pin is ever wanted it belongs in a `package pkg` file, alongside the stub from requirement 3.)
   - On a dedupe hit: `glog.V(2).Infof(...)` with an `event=trigger_refusal_dedupe task=%s scope=%q count=%d max=%d` line and `return nil` — no publish, no error.
   - Timestamp: `now := p.currentDateTime.Now().UTC().Format(time.RFC3339)`. The vault normalises an unquoted date scalar to RFC3339, so the written form must already be that; a bare `YYYY-MM-DD` would be read back unequal.
   - `Updates` carries exactly these four keys and nothing else:
     ```go
     lib.TaskFrontmatter{
     	"trigger_refusal_count": count,
     	"trigger_refusal_max":   maxTriggers,
     	"trigger_refusal_scope": scope,
     	"trigger_refusal_at":    now,
     }
     ```
   - `Body` carries exactly this section — the heading, a timestamp line, the `trigger_count N >= max_triggers M` line and the scope line, with a trailing newline:
     ```go
     section := fmt.Sprintf(
     	"## Trigger Budget\n\n- **Timestamp:** %s\n- **Refusal:** trigger_count %d >= max_triggers %d\n- **Scope:** %s\n",
     	now, count, maxTriggers, scope,
     )
     ```
     with `Heading: "## Trigger Budget"`.
   - `TaskIdentifier: task.TaskIdentifier`, `TargetVault: targetVaultFromTask(task)`, published through `p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd)`.
   - On publish error: return `errors.Wrapf(ctx, err, "publish trigger budget refusal for task %s", task.TaskIdentifier)` and do **not** call `recordDedupe` — the next reconcile tick must retry.
   - On success: `p.dedupe.recordDedupe(key)` then `return nil`.
   - Do not write `status`, `phase`, `assignee`, `current_job`, `trigger_count`, `trigger_scope` or `max_triggers`. Do not add a metric or a log line beyond the dedupe-hit line — the spec explicitly keeps the existing `skipped_trigger_cap` counter and `V(2)` refusal line where they are, in the handler.

3. Adding a method to the `ResultPublisher` interface breaks three hand-written implementations. Add a stub for the new method to each, matching the style of the neighbouring stubs:
   - `pkg/job_watcher_internal_test.go` — `fakeResultPublisher` (append after `PublishSetTriggerScope`)
   - `pkg/handler/claim_spawn_order_internal_test.go` — `stubResultPublisher` (this also satisfies `abortRecordingPublisher` and `failingAbortPublisher`, which embed it)
   - `pkg/handler/claim_wiring_internal_test.go` — `parkRecordingPublisher`
   Each stub takes `(context.Context, lib.Task, string, int, int) error` and returns `nil`.

4. Regenerate the counterfeiter fake with `ROOTDIR=/workspace make generate`. The `//counterfeiter:generate -o ../mocks/result_publisher.go --fake-name FakeResultPublisher . ResultPublisher` directive at the top of `pkg/result_publisher.go` is unchanged; `make generate` wipes and rebuilds `mocks/`. Confirm `mocks/result_publisher.go` now carries `PublishTriggerBudgetRefusal`, `PublishTriggerBudgetRefusalCallCount`, `PublishTriggerBudgetRefusalArgsForCall`, `PublishTriggerBudgetRefusalReturns` and `PublishTriggerBudgetRefusalReturnsOnCall`. Never hand-edit `mocks/`.

5. Add a `Describe("PublishTriggerBudgetRefusal", ...)` block to `pkg/result_publisher_test.go` (package `pkg_test`), inside the existing `ResultPublisher` `Describe`, using the existing `capturingSyncProducer` and `decodeUpdateFrontmatterCommand`. Specs:
   - **shape:** one captured message; operation is `taskcmd.UpdateFrontmatterCommandOperation`; `cmd.Updates` has exactly 4 entries; `trigger_refusal_scope == "planning:4963ffa6"`; `trigger_refusal_count` and `trigger_refusal_max` asserted with `BeNumerically("==", 4)` / `BeNumerically("==", 3)` — **not** `Equal(4)`, because the command is read back through its JSON encoding where every number decodes as `float64` (the same trap the `max_triggers` assertion at `pkg/result_publisher_test.go:329` documents in its own comment); `trigger_refusal_at` equals the fixed clock as `2026-04-18T12:00:00Z` **and** `time.Parse(time.RFC3339, at)` succeeds (this is the vault-normalisation boundary).
   - **section:** `cmd.Body` is non-nil, `cmd.Body.Heading == "## Trigger Budget"`, and `cmd.Body.Section` starts with `## Trigger Budget`, contains `trigger_count 4 >= max_triggers 3`, contains `planning:4963ffa6`, and ends with `\n`.
   - **does not touch the failure section or ownership keys:** `cmd.Body.Heading != "## Failure"`; none of `status`, `phase`, `assignee`, `current_job`, `trigger_count`, `trigger_scope`, `max_triggers` is present in `cmd.Updates` (assert each with `_, ok := cmd.Updates["..."]; Expect(ok).To(BeFalse())`).
   - **same triple publishes once:** two consecutive calls with identical `scope`/`count`/`maxTriggers` capture exactly one message and the second call returns no error.
   - **changed triple publishes again:** a second call with a different `scope` (same count and cap) captures a second message; likewise a different `count`.
   - **failed publish is not remembered:** build the publisher with `partialFailingSyncProducer{successCount: 0, err: errors.New(ctx, "kafka unavailable")}`; call twice and assert both calls return an error and the producer saw 2 attempted messages (proves `recordDedupe` did not run).
   Add `"time"` to the test file's imports.

6. Before finishing, re-run `<verification>` and confirm it passes; walk every requirement above against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
- The section heading must not be `## Failure`. `taskcmd.BodySection` is applied by `Markdown.ReplaceSection`, which replaces a section carrying the same heading; writing `## Failure` from this path would delete the older cause.
- The frontmatter key names are frozen: `trigger_refusal_count`, `trigger_refusal_max`, `trigger_refusal_scope`, `trigger_refusal_at`. They are persistent state and the vault records will carry them.
- `trigger_refusal_at` is written as an RFC3339 string.
- No `context.Background()`, no `fmt.Errorf`, no bare `return err` — wrap with `errors.Wrapf(ctx, err, ...)` from `github.com/bborbe/errors`.
- Do not add a metric, a counter label, or a config knob. The spec's observability is unchanged: `metrics.TaskEventsTotal.WithLabelValues("skipped_trigger_cap").Inc()` and the handler's existing `V(2)` refusal line stay exactly as they are, and this prompt adds no third signal.
- Both dedupe layers are required and this prompt ships the first: the record-state guard (skip when the record already carries the same triple) belongs to the handler prompt and cannot cover the window between publish and apply, which is why this publisher dedupes too.
- Never hand-edit `mocks/` — regenerate it.
</constraints>

<verification>
- `ROOTDIR=/workspace make precommit` — must pass.
- `go test ./pkg/ -mod=mod -v -ginkgo.v -ginkgo.focus='PublishTriggerBudgetRefusal'` — must pass **and** print `Ran 6 of` (the six specs of requirement 5; a focus matching no spec still exits 0, and without `-v` `go test` discards the Ginkgo summary on success, so neither the exit code nor the bare command can distinguish a green run from one that executed nothing).
- `grep -n 'trigger_refusal_count' pkg/result_publisher.go` — returns at least one line.
- `grep -n 'Trigger Budget' pkg/result_publisher.go` — returns at least one line.
- `grep -rn 'Heading: "## Failure"' pkg/ | wc -l` — prints `2` (the two pre-existing sites in `pkg/result_publisher.go`; this change adds no third).
- `grep -n 'PublishTriggerBudgetRefusal' mocks/result_publisher.go` — returns at least one line.
</verification>
