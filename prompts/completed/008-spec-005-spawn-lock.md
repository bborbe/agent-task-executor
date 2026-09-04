---
status: completed
spec: [005-bug-executor-restart-drops-deferred-respawn-queue]
summary: Made the per-assignee spawn lock in spawnIfNeeded unconditional to serialize count-then-spawn across the reconcile loop, Kafka consumer, and deferred-respawn loop, and locked it with a concurrent regression spec in task_reconcile_test.go that races both paths for the same uncapped task and asserts exactly one SpawnJob
execution_id: agent-task-executor-exec-008-spec-005-spawn-lock
dark-factory-version: dev
created: "2026-09-03T18:04:37Z"
queued: "2026-09-03T18:18:58Z"
started: "2026-09-03T18:34:58Z"
completed: "2026-09-03T18:41:04Z"
branch: dark-factory/bug-executor-restart-drops-deferred-respawn-queue
---

<summary>
- The per-assignee spawn lock in `spawnIfNeeded` is now taken unconditionally (for any resolved config) instead of only when `MaxConcurrentJobs > 0`, so the reconcile tick and the Kafka event path serialize count-then-spawn per assignee even for uncapped agents.
- Without this change an uncapped assignee could double-spawn the same task: both the reconcile loop and the Kafka consumer would read `IsJobActive == false` before either creates the Job, then both spawn.
- A new concurrency regression test races the reconcile path against the event path for the same uncapped task against a stateful fake spawner and asserts exactly one `SpawnJob`.
- This closes spec 005 AC 5 ("no double-spawn"); the reconcile loop from the previous prompt is what makes the race reachable for uncapped assignees, so this prompt runs after it.
</summary>

<objective>
Harden `spawnIfNeeded` against the reconcile-vs-event double-spawn race by taking the per-assignee spawn lock unconditionally, and lock the behavior with a concurrent regression test that races both paths for the same uncapped task.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for project conventions (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, counterfeiter mocks, coverage ≥80% for new code).

Read these files fully before changing anything:
- `/workspace/pkg/handler/task_event_handler.go` — the `lockAssigneeSpawn` helper (lines 172-183) and the conditional lock in `spawnIfNeeded` (lines 466-473, the `if config != nil && config.MaxConcurrentJobs > 0` guard to change). The doc comment on `lockAssigneeSpawn` (lines 152-171) explains the serialization rationale — extend it to cover the uncapped case.
- `/workspace/pkg/handler/task_event_handler_test.go` — the existing concurrent test "admits exactly MaxConcurrentJobs when spawns arrive concurrently" (lines 305-361) for the capped case: note the `time.Sleep(20 * time.Millisecond)` widening pattern and the stateful `CountActiveJobs` stub. Mirror that technique.
- `/workspace/pkg/handler/task_reconcile_test.go` — created by the previous prompt; append the new regression spec here (do NOT extend the 1909-line `task_event_handler_test.go`; revive file-length-limit is 2000).
- `/workspace/pkg/spawner/job_spawner.go` — the `JobSpawner` interface (lines 39-52): `SpawnJob`, `IsJobActive`, `CountActiveJobs` — the fake used in the regression test.

Library contracts (verified in this repo):
- `lib "github.com/bborbe/agent"` — `lib.Task`/`lib.TaskFrontmatter`; the fake `IsJobActive` reads `lib.TaskIdentifier`.
- `pkg.AgentConfiguration{Assignee: ..., Image: ..., MaxConcurrentJobs: 0}` — `MaxConcurrentJobs: 0` means uncapped (see `deferIfAtConcurrencyCap`, `task_event_handler.go:540`).

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-concurrency-patterns.md` — per-assignee lock rationale, no raw `go func` in production code (the test may use them with `GinkgoRecover`).
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega concurrency assertions.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — never `fmt.Errorf`.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new code.
</context>

<requirements>
1. In `/workspace/pkg/handler/task_event_handler.go`, change the conditional lock in `spawnIfNeeded` (lines 467-473) to be unconditional. Replace:
   ```go
   	// Held across the cap check AND the SpawnJob call below, so no other goroutine
   	// can observe a stale Job count in between. Only taken when a cap is configured:
   	// an uncapped agent has nothing to serialize, and holding the lock anyway would
   	// queue its spawns behind each other for no benefit.
   	if config != nil && config.MaxConcurrentJobs > 0 {
   		defer h.lockAssigneeSpawn(config.Assignee)()
   	}
   ```
   with:
   ```go
   	// Held across the cap check AND the SpawnJob call below, so no other goroutine
   	// can observe a stale Job count in between. Taken unconditionally (for any
   	// resolved config, capped or not) because three goroutines reach spawnIfNeeded
   	// — the Kafka consumer, the deferred-respawn loop, and the reconcile loop
   	// (spec 005) — and for an UNCAPPED assignee they must still serialize
   	// count-then-spawn so two of them cannot both read IsJobActive=false before
   	// either creates the Job and double-spawn the same task. Spawns are rare
   	// against Job runtimes measured in minutes, so the per-assignee serialization
   	// costs nothing measurable. config is nil only for tasks that never reach this
   	// code (empty assignee is filtered upstream); the nil guard stays defensive.
   	if config != nil {
   		defer h.lockAssigneeSpawn(config.Assignee)()
   	}
   ```
   Update the `lockAssigneeSpawn` doc comment (lines 152-171) to state that the lock now also serializes the reconcile loop against the event path for uncapped agents (keep the existing capped-race incident text; add one sentence: "Taken unconditionally since spec 005: the reconcile loop and the Kafka consumer must also serialize for uncapped assignees, or a reconcile tick racing the event path for the same task would double-spawn.")

2. Append one regression spec to `/workspace/pkg/handler/task_reconcile_test.go` (inside the existing `Describe("TaskEventHandler reconcile loop", ...)` block). It races the event path and the reconcile path for the SAME uncapped task and asserts exactly one `SpawnJob`:
   ```go
   It("races the reconcile path against the Kafka event path for an uncapped task and spawns exactly one job (AC 5)", func() {
       // Spec 005 AC 5: with the per-assignee lock taken unconditionally, a
       // reconcile tick racing the event path for the SAME uncapped task results
       // in exactly one Job. Without the unconditional lock both goroutines read
       // IsJobActive=false before either has created its Job, then both spawn.
       // The fake is stateful like the real cluster: IsJobActive reports whether a
       // Job has been created, and the sleep widens the read->spawn window so the
       // unlocked race is deterministic rather than luck-of-the-timing.
       fakeResolver.ResolveReturns(
           pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest", MaxConcurrentJobs: 0},
           nil,
       )
       var spawned atomic.Int64
       fakeSpawner.CountActiveJobsReturns(0, nil)
       fakeSpawner.SpawnJobStub = func(
           _ context.Context,
           _ lib.Task,
           _ pkg.AgentConfiguration,
       ) (string, error) {
           spawned.Add(1)
           return "job-name", nil
       }
       fakeSpawner.IsJobActiveStub = func(
           _ context.Context,
           _ lib.TaskIdentifier,
       ) (bool, error) {
           time.Sleep(20 * time.Millisecond)
           return spawned.Load() > 0, nil
       }
       fakeGitRestClient.IsReadyReturns(true, nil)

       task := lib.Task{
           TaskIdentifier: lib.TaskIdentifier("tid-double-spawn"),
           Frontmatter: lib.TaskFrontmatter{
               "status":   "in_progress",
               "phase":    string(domain.TaskPhaseExecution),
               "assignee": "claude",
               "stage":    "prod",
           },
       }
       fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-double-spawn.md"}, nil)
       fakeGitRestClient.GetReturns([]byte(renderTaskFile(task)), nil)

       // NOTE: buildMsg in task_event_handler_test.go is a Describe-scoped
       // closure (not package-level) and is NOT visible from this file. Define a
       // local equivalent so the regression spec is self-contained.
       buildMsg := func(t lib.Task) *sarama.ConsumerMessage {
           value, err := json.Marshal(t)
           Expect(err).To(BeNil())
           return &sarama.ConsumerMessage{Value: value}
       }

       var wg sync.WaitGroup
       wg.Add(2)
       go func() {
           defer GinkgoRecover()
           defer wg.Done()
           Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(BeNil())
       }()
       go func() {
           defer GinkgoRecover()
           defer wg.Done()
           Expect(h.ReconcileOnce(ctx)).To(BeNil())
       }()
       wg.Wait()

       Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
   })
   ```
   This spec uses the local `buildMsg` closure defined above (do NOT reference the one in `task_event_handler_test.go` — it is Describe-scoped and not visible here) and `renderTaskFile` (defined in the same `Describe("TaskEventHandler reconcile loop", ...)` block of `task_reconcile_test.go` by the previous prompt). Imports to ensure present in the reconcile test file: `sync`, `sync/atomic`, `time`, `encoding/json`, `github.com/IBM/sarama` (the `sarama.ConsumerMessage` type).

3. Do NOT add a CHANGELOG entry in this prompt — the single `feat:` bullet in `## Unreleased` from the previous prompt already covers the double-spawn hardening (its text names the unconditional lock). Do NOT add any other code changes: no new config, no metrics, no interface changes.
</requirements>

<constraints>
- Repo conventions are frozen: Ginkgo/Gomega v2 tests, `github.com/bborbe/errors` wrapping, counterfeiter mocks, coverage ≥80% for new code.
- The lock becomes unconditional for any resolved config (`config != nil`); the `config == nil` path stays unlocked (no assignee to serialize on — such tasks never reach this code).
- Do NOT change the Kafka event path, `deferIfAtConcurrencyCap`, or `checkActiveCurrentJob` — only the lock guard and its comments.
- Do NOT make the interval, the lock, or anything else configurable — the spec forbids new knobs.
- Put the regression spec in `pkg/handler/task_reconcile_test.go` — NEVER in the 1909-line `task_event_handler_test.go` (revive file-length-limit is 2000).
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass (including the existing capped concurrent test at `task_event_handler_test.go:305` — with the lock now always taken, its behavior is unchanged because the lock was already taken for capped configs).
</constraints>

<verification>
Run `go test -mod=mod ./pkg/handler/...` iteratively after the change (fast feedback loop), then `make precommit` ONCE at the very end.

- `go test -mod=mod ./pkg/handler/...` — exits 0.
- `make precommit` — exits 0.
- `grep -n 'lockAssigneeSpawn' pkg/handler/task_event_handler.go` — shows the unconditional `if config != nil` guard in `spawnIfNeeded` (acceptance-criterion evidence: "the per-assignee spawn lock is taken unconditionally in spawnIfNeeded").
- `go test -mod=mod -run TestHandler -count=10 ./pkg/handler/` — the double-spawn regression spec passes across repeated runs (the race is timing-sensitive; -count=10 makes flakiness visible).
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/handler/... && go tool cover -func=/tmp/cover.out` — the modified `spawnIfNeeded` branch is exercised.

Do NOT run `docker`, `make build`, `kubectl`, `dark-factory`, `gh`, or `scripts/*.sh` commands in this prompt — those are operator-executable and belong on the spec's verification ladder.
</verification>
