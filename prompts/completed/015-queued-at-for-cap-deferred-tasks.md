---
status: completed
summary: Record queued_at on concurrency-cap deferral via a new PublishQueueEntry command, with vault-aware no-restamp logic and full test coverage
execution_id: agent-task-executor-queued-at-exec-015-queued-at-for-cap-deferred-tasks
dark-factory-version: v0.198.7
created: "2026-10-20T20:40:18Z"
queued: "2026-10-20T20:40:18Z"
started: "2026-10-20T20:41:06Z"
completed: "2026-10-20T20:47:51Z"
---

# Record queued_at for cap-deferred tasks

<summary>
- A task that becomes eligible for a spawn but is held back by the concurrency cap now records when its wait started
- The timestamp is written once, at the moment the deferral happens, and never moves on a later retry
- A task that spawns immediately records nothing — the field means "this task waited"
- The field is written before the job starts, so the agent's own write-back carries it and it stays readable after the job finishes
- Existing behaviour is unchanged: no new failure path, no change to the cap, no change to what a deferral does
- The write goes through the same mechanism the spawn notification already uses
</summary>

<objective>
Record a durable per-task queue-entry timestamp (`queued_at`, an RFC3339 UTC string) when the executor defers a task behind the concurrency cap, so a consumer reading the task file can compute a queue age for the agent-execution stage. The field name and format are the agreed contract: `queued_at` is the moment the first wait began, it is written once, and it is never emitted by an agent result write-back.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for project conventions.

Read `pkg/handler/task_event_handler.go` — `spawnIfNeeded` calls `deferIfAtConcurrencyCap`, which registers an entry in `h.deferredRespawns` and returns `true` when the assignee is already at `MaxConcurrentJobs`. The cap is counted through `h.activeJobsFor(ctx, assignee)` (which calls `h.jobSpawner.ListActiveJobs`); `isJobActive` runs before the cap check and reads the same set. `evalDeferredRespawns` later re-drives the stored entry through `spawnIfNeeded`.

Read `pkg/result_publisher.go` — `PublishSpawnNotification` is the pattern to copy: it builds a `taskcmd.UpdateFrontmatterCommand` with `TaskIdentifier`, `TargetVault` (from `targetVaultFromTask`) and a `lib.TaskFrontmatter` of updates, then calls `p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd)`.

Read `pkg/result_publisher_test.go` — `decodeUpdateFrontmatterCommand` (line ~112) unmarshals a captured Kafka message into the command; the `PublishSpawnNotification` spec uses it to assert the `Updates` payload.

Read `pkg/gitrestclient/git_rest_client.go` — `Get(ctx, relPath)` returns a task file's bytes. The handler keeps `h.pathIDs` (repo-relative path → task identifier) under `h.vaultMu`, populated by the reconcile loop via `recordPathID`.

Read `pkg/task_store.go` — `TaskStore` is an in-memory map, not the vault; it is populated on spawn, so it is not the authority for this guard.

Read `pkg/handler/task_event_handler_deferred_test.go` — the existing Ginkgo harness for this handler: `mocks.FakeResultPublisher`, `mocks.FakeGitRestClient`, `libtime.NewCurrentDateTime()`, `handler.NewTaskEventHandler(...)`.

Read `pkg/job_watcher_internal_test.go` — `fakeResultPublisher` (line ~316) hand-implements the whole `ResultPublisher` interface and is passed to `NewJobWatcher`; it cannot use the counterfeiter fake because of an import cycle.

Coding guides (in-container): `/home/node/.claude/plugins/marketplaces/coding/docs/go-cqrs.md` (command construction), `go-error-wrapping-guide.md`, `go-glog-guide.md` (`V(n)` gating), `go-testing-guide.md` (external test package, Ginkgo/Gomega), `changelog-guide.md` (`## Unreleased` placement and prefix).
</context>

<requirements>
1. Add `PublishQueueEntry(ctx context.Context, task lib.Task, queuedAt string) error` to the `ResultPublisher` interface in `pkg/result_publisher.go`, with a doc comment stating it publishes `queued_at` only and touches no other key.
2. Implement `PublishQueueEntry` on `*resultPublisher` in `pkg/result_publisher.go`, mirroring `PublishSpawnNotification`: one `taskcmd.UpdateFrontmatterCommand` whose `Updates` map holds exactly `{"queued_at": queuedAt}`, published through `publishRaw` with `taskcmd.UpdateFrontmatterCommandOperation`. Add a spec for it in `pkg/result_publisher_test.go` next to the existing `Describe("PublishSpawnNotification")`, using `decodeUpdateFrontmatterCommand`: assert one message was captured, the operation equals `taskcmd.UpdateFrontmatterCommandOperation`, `cmd.Updates` has length 1, and `cmd.Updates["queued_at"]` equals the string passed in. The JSON round-trip through `base.Command` is the contract the controller consumes.
3. Regenerate the Counterfeiter mock with `make generate` (or `go generate ./...`) so `mocks/result_publisher.go` carries `FakeResultPublisher.PublishQueueEntry`; do not hand-edit the mock. Then add a `PublishQueueEntry(ctx context.Context, task lib.Task, queuedAt string) error` stub (return nil) to the hand-written `fakeResultPublisher` in `pkg/job_watcher_internal_test.go` (line ~316) — it implements the full `ResultPublisher` interface and is passed to `NewJobWatcher`, so the new interface method otherwise breaks compilation of the `pkg` test package.
4. In `pkg/handler/task_event_handler.go`, add `stampQueueEntry(ctx context.Context, task lib.Task) lib.Task` and call it inside `deferIfAtConcurrencyCap` immediately before the `h.deferredMu.Lock()` that registers the entry, assigning the result back to `task`. It must:
   - return the task unchanged when its `Frontmatter` already holds a non-empty `queued_at` — `task.Frontmatter.String("queued_at")` returns `(string, bool)`, so read it as `if v, ok := task.Frontmatter.String("queued_at"); ok && v != ""` (the idiom `targetVaultFromTask` already uses);
   - otherwise read the task's file through a new helper `queuedAtFromVault(ctx, id lib.TaskIdentifier)` and return a copy carrying that value when it is non-empty — the vault is the authority for a duplicate event that arrives after the publish but before the controller has written the field; without it that event would re-stamp and move the timestamp;
   - otherwise stamp `h.currentDateTime.Now().UTC().Format("2006-01-02T15:04:05Z07:00")`, call `h.resultPublisher.PublishQueueEntry`, log `glog.V(1).Infof("event=queued_at_written task=%s queued_at=%s", task.TaskIdentifier, queuedAt)` on success, log a warning on failure, and return a copy whose `Frontmatter` carries the value so later re-deferrals see it. A failed publish is not retried — the in-memory copy suppresses the next attempt, so that wait reports no queue age; state that in a code comment.
5. `queuedAtFromVault` resolves the task's repo-relative path by scanning `h.pathIDs` under `h.vaultMu` (RLock), releases the lock before I/O, calls `h.gitRestClient.Get(ctx, path)` with the caller's ctx unchanged (no `context.Background()`, no extra retry loop — the client owns its own), parses with `parseTaskFile`, and returns `""` on an unknown path, a read error, or an absent/empty field. A failed read must log and continue — the deferral itself must never fail because of this write. The read runs on the deferral path while the per-assignee spawn lock is held, so it must stay a single bounded call.
6. Copy the task's frontmatter map before mutating it (the caller's map is shared with the event); never mutate `task.Frontmatter` in place.
7. Add a `Describe("queued_at on cap deferral")` block in `pkg/handler/task_event_handler_deferred_test.go` with four specs, driving the deferral through `ConsumeMessage`. In `BeforeEach`, override the resolver with `fakeResolver.ResolveReturns(pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest", MaxConcurrentJobs: 1}, nil)`, pin the clock with `currentDateTime.SetNow(libtimetest.ParseDateTime("2026-05-17T09:34:00Z"))`, and set `fakeSpawner.ListActiveJobsReturns(map[lib.TaskIdentifier]bool{"other-task": true}, nil)` — the cap is counted through `ListActiveJobs` (via `activeJobsFor`), not `CountActiveJobs`, and the map must carry a DIFFERENT identifier because `isJobActive` returns early when the task's own id is active. Each payload carries `status: in_progress`, `phase: execution`, `assignee: claude`, `stage: prod` (the harness branch is `base.Branch("prod")`), and `fakeSpawner.IsJobActiveReturns(false, nil)`:
   - payload without `queued_at` and an empty vault → `PublishQueueEntryCallCount()` is 1 and the recorded `queuedAt` argument equals the fixed clock's value formatted `2006-01-02T15:04:05Z07:00`;
   - payload without `queued_at` while the vault already holds the value → `PublishQueueEntryCallCount()` is 0. `queuedAtFromVault` finds the path only through `h.pathIDs`, which only `reconcileOne`→`recordPathID` populates, so drive `h.ReconcileOnce(ctx)` first with `fakeGitRestClient.ListReturns([]string{"24 Tasks/tid-queued.md"}, nil)` and a file body carrying `task_identifier: tid-queued` and a QUOTED `queued_at: "2026-05-17T09:34:00Z"` — `parseTaskFile` decodes YAML into `map[string]interface{}` and an unquoted RFC3339 scalar resolves to `time.Time`, which `Frontmatter.String` rejects. The payload's `TaskIdentifier` must be `tid-queued` (otherwise `skipForeignVaultTask` drops it as foreign and the 0-count passes without ever reaching `queuedAtFromVault`), and the spec must assert the deferral actually occurred — `metrics.DeferredConcurrencyCapTotal.WithLabelValues("claude")` incremented — so it cannot pass vacuously. The file content must be a full frontmatter-delimited document (`---\n…\n---`), not a bare YAML fragment: `parseTaskFile` requires the leading `---`, and without it `recordPathID` never runs;
   - a second deferral of the same task after the first write → `PublishQueueEntryCallCount()` stays 1. Drive this through the in-memory deferred entry, not a second `ConsumeMessage` (a repeat payload carries no `queued_at` and would re-stamp): after the first deferral, advance the clock past the 60 s cap retry delay with `currentDateTime.SetNow(libtimetest.ParseDateTime("2026-05-17T09:35:01Z"))` — `concurrencyCapRetryDelay` is unexported and this test is in package `handler_test`, so hardcode the timestamp — and call `h.EvalDeferredRespawns(ctx)`; the re-driven `deferIfAtConcurrencyCap` sees `queued_at` in the entry's frontmatter and must not publish again;
   - `queuedAtFromVault` returns empty on each of its three failure branches, each still publishing once and never failing the consume: an unknown id with an empty `pathIDs`; `fakeGitRestClient.GetReturns(nil, errors.Errorf(ctx, "read failed"))` after a reconcile has recorded the path; and a reconciled file whose frontmatter has no `queued_at`. Use a `DescribeTable` for these three;
   - publish failure → `fakeResultPublisher.PublishQueueEntryReturns(errors.Errorf(ctx, "publish failed"))`; `ConsumeMessage` returns nil, `PublishQueueEntryCallCount()` is 1, and a second deferral of the same task publishes nothing (the in-memory copy suppresses the retry).
8. Add a `## Unreleased` section to `CHANGELOG.md` directly above the top `## vX.Y.Z` heading, with a `feat:`-prefixed bullet describing the new `queued_at` frontmatter field. Do not bump any version string.
9. Before finishing, re-run the `<verification>` command and confirm it passes; walk each numbered requirement above against the change.
</requirements>

<constraints>
- Use the injected clock (`h.currentDateTime`), never `time.Now()`; no new `time.Time` struct fields.
- Format the timestamp with `"2006-01-02T15:04:05Z07:00"` — the same layout `job_started_at` uses.
- Tests are Ginkgo/Gomega; use `libtime.NewCurrentDateTime()` and `SetNow` for a fixed clock.
- Wrap errors with `github.com/bborbe/errors`; never `fmt.Errorf` and never a bare `return err`.
- Gate `glog.Infof` calls with `V(n)`.
- Do not change the concurrency cap, its defaults, or the deferral retry interval.
- Do not write `queued_at` anywhere except the cap-deferral branch — a spawn that is not deferred must leave the field absent.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit` — must pass.
</verification>
