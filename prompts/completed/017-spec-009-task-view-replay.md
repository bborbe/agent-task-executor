---
status: completed
spec: [009-kafka-task-view-replaces-gitrest-reconcile]
execution_id: agent-task-executor-kafka-view-exec-017-spec-009-task-view-replay
dark-factory-version: v0.198.7
created: "2026-10-21T18:40:07Z"
queued: "2026-10-21T19:39:58Z"
started: "2026-10-21T19:40:00Z"
completed: "2026-10-21T19:48:28Z"
branch: dark-factory/kafka-task-view-replaces-gitrest-reconcile
---

# Build the in-memory task view from the compacted task event topic

<summary>
- The executor can rebuild its whole task picture in memory by replaying the task event topic from the beginning on startup
- Replay covers every partition, keeps only the latest version of each task, and honours deletion messages
- The view reports itself ready only once every partition has caught up to the position captured at startup
- An empty partition never holds readiness back
- After ready, the view keeps applying new events on every partition, independent of the live consumer group's assignment
- A message that cannot be decoded is dropped, any stale entry for it is removed, and the drop is counted
- The view's reader never joins or commits to the live consumer group, so it cannot steal partitions or move offsets
- The view is wired into the running service, and a reader failure ends the process so a pod restart rebuilds the view
- The agent library dependency is upgraded to the version that stamps the vault name on every task event
- Reconcile and task ownership do not change in this prompt; the next prompt switches them onto the view
</summary>

<objective>
Add an in-memory task view that is rebuilt at startup from `{TOPIC_PREFIX}-agent-task-v1-event` and kept current from the same topic, and wire it into `service.Run`. This is the data source that replaces the git-rest vault read in the next prompt. It must be complete and ready before any consumer of it exists.
</objective>

<context>
Read these files before editing:

- `go.mod` — `github.com/bborbe/agent` is pinned at `v0.86.0`. The target version is `v0.113.0`.
- `main.go` — `application.Run` creates `saramaClient` with `libkafka.CreateSaramaClient`, then `factory.CreateConsumer`, then passes the run functions to `service.Run`.
- `pkg/factory/factory.go` — `CreateConsumer` builds the live group consumer with `libkafka.NewSaramaOffsetManager(..., libkafka.Group(consumerGroup), ...)` and `libkafka.NewOffsetConsumerHighwaterMarks(...)`. Follow its factory style (`Create*` prefix, zero business logic).
- `pkg/handler/task_event_handler.go` — `parseAndFilter` decodes the message with `json.Unmarshal(msg.Value, &task)` and treats an empty value, invalid JSON, and an empty `TaskIdentifier` as skips. This is the decode behaviour the view must share.
- `pkg/metrics/metrics.go` — the counter conventions and the `init()` pre-initialisation pattern.
- `pkg/handler/task_event_handler_test.go` — the Ginkgo/Gomega suite entry point (`TestHandler`, `package handler_test`) and the `buildMsg` helper style.
- `pkg/handler/job_cache.go` — the repo's `sync.Mutex` + snapshot idiom for shared in-memory state.

Verified library facts (read from the module source, do not re-derive):

- `github.com/bborbe/agent@v0.113.0`, `agent_task.go`:
  ```go
  type Task struct {
      base.Object[base.Identifier]
      TaskIdentifier TaskIdentifier `json:"taskIdentifier"`
      Vault       string          `json:"vault,omitempty"`
      Frontmatter TaskFrontmatter `json:"frontmatter"`
      Content     TaskContent     `json:"content"`
  }
  ```
- `lib.TaskV1SchemaID.EventTopic(prefix base.TopicPrefix) libkafka.Topic` (from `github.com/bborbe/cqrs/cdb`).
- `libkafka.NewSimpleConsumer(saramaClient SaramaClient, topic Topic, initalOffset Offset, messageHandler MessageHandler, logSamplerFactory log.SamplerFactory, options ...func(*ConsumerOptions)) Consumer` — a non-group consumer: `offsetConsumer.Consume` lists every partition and runs one goroutine per partition from the given offset. Its offset manager is `NewSimpleOffsetManager`, in-memory only, so it commits nothing.
- `libkafka.HighWaterMarks(ctx context.Context, saramaClient sarama.Client, topic Topic) (PartitionOffsets, error)` where `PartitionOffsets = map[Partition]Offset`. The high-water mark is the next offset to be written, so a partition with messages `0..N-1` reports `N` and an empty partition reports `0`.
- `libkafka.MessageHandler` is `ConsumeMessage(ctx context.Context, msg *sarama.ConsumerMessage) error`.
- `libkafka.OffsetOldest` is the oldest-offset sentinel.
- `run.Func` is `func(context.Context) error`; `service.Run(ctx, funcs ...run.Func)` cancels all funcs on the first error.

Read these coding docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-concurrency-patterns.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-prometheus-metrics-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-factory-pattern.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-mocking-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-mod-dependency-fix-guide.md`
</context>

<requirements>
1. **Bump the agent dependency (AC2).** In `go.mod`, change `github.com/bborbe/agent v0.86.0` to `github.com/bborbe/agent v0.113.0`. Run `go mod tidy` (do not hand-pin transitive versions). Expect `github.com/bborbe/kafka` to move to at least `v1.25.16`, `github.com/bborbe/vault-cli` to at least `v0.126.3`, and small patch bumps of `errors`, `http`, `metrics`, `time`, `validation`. Then run `go build ./...` and fix any compile drift. The only API changes that matter are additive: `lib.Task.Vault` and `TaskFrontmatter.Cluster()`. Do not change any application behaviour while fixing drift.

2. **Create `pkg/handler/task_view.go`** (package `handler`) with this contract:

   ```go
   // HighwaterMarksFunc returns the high-water mark of every partition of topic.
   type HighwaterMarksFunc func(ctx context.Context, topic libkafka.Topic) (libkafka.PartitionOffsets, error)

   //counterfeiter:generate -o ../../mocks/task_view.go --fake-name FakeTaskView . TaskView

   // TaskView is an in-memory, latest-wins view of the compacted task event topic.
   type TaskView interface {
       // CaptureHighWaterMarks records the topic's per-partition high-water marks at
       // start. It must be called once, before the replay consumer starts.
       CaptureHighWaterMarks(ctx context.Context) error
       // ConsumeMessage applies one task event. It is the MessageHandler the replay
       // consumer is built with.
       ConsumeMessage(ctx context.Context, msg *sarama.ConsumerMessage) error
       // Ready reports whether every partition reached the mark captured at start.
       Ready() bool
       // Snapshot returns a point-in-time copy of every task in the view.
       Snapshot() []lib.Task
       // Get returns the task for id, or false when the view has none.
       Get(id lib.TaskIdentifier) (lib.Task, bool)
   }

   // NewTaskView creates an empty view for topic.
   func NewTaskView(highwaterMarks HighwaterMarksFunc, topic libkafka.Topic, currentDateTimeGetter libtime.CurrentDateTimeGetter) TaskView
   ```

   Implement it with a private struct holding `sync.RWMutex`, `tasks map[lib.TaskIdentifier]lib.Task`, `marks libkafka.PartitionOffsets`, `consumed map[libkafka.Partition]libkafka.Offset`, `marksCaptured bool`, `ready bool`, `startedAt libtime.DateTime`, the `HighwaterMarksFunc`, the topic, and the `libtime.CurrentDateTimeGetter`. Behaviour:

   - `CaptureHighWaterMarks`: set `startedAt = v.currentDateTimeGetter.Now()` before calling the func; on error return `errors.Wrapf(ctx, err, "capture high-water marks for topic %s", v.topic)`; on success store the marks, set `marksCaptured = true`, then evaluate readiness.
   - Readiness: ready iff marks were captured AND for every partition `p` in `marks`, `consumed[p] >= marks[p]`. `consumed` starts empty (zero), so a partition whose mark is `0` is ready immediately. Partitions absent from `marks` are ignored. Evaluate readiness after capture and after every consumed message.
   - On the first transition to ready, log exactly once: `glog.Infof("event=task_view_ready tasks=%d partitions=%d elapsed=%s", len(v.tasks), len(v.marks), time.Time(v.currentDateTimeGetter.Now()).Sub(time.Time(v.startedAt)))`. Guard the transition with the `ready` bool under the lock so a second evaluation cannot log again.
   - `ConsumeMessage`: decode with the shared routine from requirement 3.
     - Valid task: store `v.tasks[task.TaskIdentifier] = task` (latest wins).
     - Empty value (tombstone): delete `v.tasks[lib.TaskIdentifier(msg.Key)]`. Do not log and do not count it.
     - Decode failure: delete `v.tasks[lib.TaskIdentifier(msg.Key)]`, increment `metrics.TaskViewSkippedTotal`, and log `glog.Warningf("event=task_view_skip partition=%d offset=%d err=%v", msg.Partition, msg.Offset, err)`. A decode failure must never block readiness.
     - Every message, valid or not, advances `consumed[libkafka.Partition(msg.Partition)]` to `max(current, libkafka.Offset(msg.Offset+1))`, then evaluates readiness.
     - Always return `nil`; a bad message is skipped, never an error (the reader must not die on one bad record).
   - Do not log while holding the mutex.
   - `Snapshot` copies the values into a new slice under `RLock`; `Get` returns the stored task under `RLock`. Never return the internal map. A shallow copy is enough: callers treat snapshot tasks as read-only, and the codebase already copies the frontmatter map before modifying it (`taskWithFrontmatterValue`).
   - Thread-safety: `ConsumeMessage` may be called from one goroutine per partition concurrently, and `Snapshot`/`Get` run concurrently with it. Use the mutex for every map access.

3. **Add the shared decode routine (one parser, not two).** In the same file, add:

   ```go
   var errEmptyTaskEvent = stderrors.New("empty task event value")
   var errEmptyTaskIdentifier = stderrors.New("task event has empty taskIdentifier")

   // decodeTaskEvent decodes one task event value. It is the single decode path
   // shared by the live consumer and the task view.
   func decodeTaskEvent(ctx context.Context, value []byte) (lib.Task, error)
   ```

   Behaviour: empty `value` returns `errEmptyTaskEvent`; invalid JSON returns `errors.Wrapf(ctx, err, "unmarshal task event")`; a decoded task with an empty `TaskIdentifier` returns `errEmptyTaskIdentifier`; otherwise the task. Then refactor `parseAndFilter` in `pkg/handler/task_event_handler.go` to call it instead of the inline `json.Unmarshal`/empty checks, preserving the existing skip behaviour and log lines exactly:

   ```go
   task, err := decodeTaskEvent(ctx, msg.Value)
   if err != nil {
       switch {
       case stderrors.Is(err, errEmptyTaskEvent):
           glog.V(3).Infof("skip empty message at offset %d", msg.Offset)
       case stderrors.Is(err, errEmptyTaskIdentifier):
           glog.Warningf("task event at offset %d has empty TaskIdentifier, skipping", msg.Offset)
       default:
           glog.Warningf("failed to unmarshal task event at offset %d: %v", msg.Offset, err)
       }
       return lib.Task{}, nil, true, nil
   }
   ```

   `parseAndFilter` must not gain any other behaviour change. The live consumer's `ConsumeMessage` path and its group/offset semantics stay exactly as they are.

4. **Add the counter** in `pkg/metrics/metrics.go`:

   ```go
   // TaskViewSkippedTotal counts task event messages the in-memory task view
   // could not decode. A skipped message never blocks readiness.
   var TaskViewSkippedTotal = promauto.NewCounter(prometheus.CounterOpts{
       Name: "executor_task_view_skipped_total",
       Help: "Total number of task event messages the task view could not decode.",
   })
   ```

   Do not add any other metric in this prompt.

5. **Add the factory** in `pkg/factory/factory.go`:

   ```go
   // CreateTaskView returns the in-memory task view and the run.Func that captures
   // the topic's high-water marks and then replays the compacted task event topic
   // from the oldest offset on every partition. The reader is a non-group simple
   // consumer: it never joins CONSUMER_GROUP and never commits offsets, so it cannot
   // steal partitions from, or move the offsets of, the live consumer.
   func CreateTaskView(
       saramaClient sarama.Client,
       topicPrefix base.TopicPrefix,
       currentDateTimeGetter libtime.CurrentDateTimeGetter,
   ) (handler.TaskView, run.Func)
   ```

   Implementation shape (keep the factory free of business logic):
   - `topic := lib.TaskV1SchemaID.EventTopic(topicPrefix)`
   - `view := handler.NewTaskView(func(ctx context.Context, topic libkafka.Topic) (libkafka.PartitionOffsets, error) { return libkafka.HighWaterMarks(ctx, saramaClient, topic) }, topic, currentDateTimeGetter)`
   - `consumer := libkafka.NewSimpleConsumer(saramaClient, topic, libkafka.OffsetOldest, view, log.DefaultSamplerFactory)`
   - return `view` and a `run.Func` that calls `view.CaptureHighWaterMarks(ctx)` (wrapping an error with `errors.Wrapf(ctx, err, "capture task view high-water marks")`) and then returns `consumer.Consume(ctx)`.
   - Do **not** create an offset manager, do **not** use `libkafka.Group`, and do **not** call `NewOffsetConsumerHighwaterMarks` here. The simple consumer is the required non-group reader.
   - `handler.TaskView` satisfies `libkafka.MessageHandler` structurally; no adapter is needed.

6. **Wire it into `main.go`** after `saramaClient` is created and before the live consumer is built:

   ```go
   _, runTaskView := factory.CreateTaskView(saramaClient, a.TopicPrefix, currentDateTimeGetter)
   ```

   Add `runTaskView` to the `service.Run` argument list (next to `consumer.Consume`). Do not pass the view into `factory.CreateConsumer` in this prompt. Do not change the live consumer, `CONSUMER_GROUP`, `OffsetConsumerHighwaterMarks`, or the handler.

7. **Tests — `pkg/handler/task_view_test.go`** (`package handler_test`). Add a stderr-capture helper used by this and later prompts:

   ```go
   // captureStderr runs fn with glog's stderr output redirected into a pipe and
   // returns everything written. Specs in this suite run serially, so the global
   // os.Stderr swap cannot race another spec's logging.
   func captureStderr(fn func()) string
   ```

   It must: set the `logtostderr` flag to `"true"` (restore `"false"` after), save `os.Stderr`, install an `os.Pipe` writer, run `fn`, call `glog.Flush()`, restore `os.Stderr`, close the writer, read the pipe, and return the text. Use it only in synchronous specs.

   Add a `Describe("TaskView", ...)` covering AC3:
   - Build every view in the tests with `libtime.NewCurrentDateTime()` as the third argument to `NewTaskView`, and import `libtime "github.com/bborbe/time"` wherever `NewTaskView` is constructed.
   - **Replay, latest-wins, tombstone, readiness.** Build the view with marks `{0: 3, 1: 1}`. Feed, in this order: partition 0 offset 0 = create task `tid-a` (frontmatter version 1); partition 0 offset 1 = update `tid-a` (frontmatter version 2); partition 0 offset 2 = create `tid-b`; partition 1 offset 0 = tombstone for `tid-b` with `Key: []byte("tid-b")` and empty `Value`. After the third message assert `Ready()` is false (partition 0 has reached its mark but the tombstone on partition 1 has not been applied). After the tombstone assert `Ready()` is true. Assert `Snapshot()` has exactly one task, `tid-a`, with the version-2 frontmatter, and that `Get("tid-b")` returns false. Use `json.Marshal(task)` for valid values and set `Partition`, `Offset`, and `Key` on every `sarama.ConsumerMessage`.
   - **Empty partition does not hold readiness.** Marks `{0: 1, 1: 0}`; feed partition 0 offset 0; assert `Ready()` true.
   - **After ready, every partition keeps applying.** Marks `{0: 1, 1: 0}`; feed partition 0 offset 0 (ready); then feed partition 0 offset 1 and partition 1 offset 0; assert both new tasks are in `Snapshot()`.
   - **Decode failure removes the entry and is counted.** Start ready; feed a valid `tid-a`, then a message with `Key: []byte("tid-a")` and `Value: []byte("not-json")`; assert `Get("tid-a")` is false and `testutil.ToFloat64(metrics.TaskViewSkippedTotal)` increased by exactly 1.
   - **Ready is logged exactly once.** With marks `{0: 0}` (immediately ready), wrap `CaptureHighWaterMarks` in `captureStderr` and assert `strings.Count(out, "event=task_view_ready") == 1` and that the line contains `tasks=0` and `partitions=1`. Then consume one message and assert the count is still 1.
   - **Capture error is returned.** Pass a `HighwaterMarksFunc` returning an error; assert `CaptureHighWaterMarks` returns an error and `Ready()` stays false.

8. **Do not** add a reconcile change, a `TASK_VAULT` setting, ownership logic, git-rest deletion, or any consumer wiring of the view in this prompt. Those are the next two prompts.
</requirements>

<constraints>
- The view's reader must not join or commit to `CONSUMER_GROUP` or any other consumer group. It never steals partitions from the live consumer and never moves its offsets.
- The topic `{TOPIC_PREFIX}-agent-task-v1-event` is compacted with infinite retention; the view depends on that. Do not add a retention or compaction workaround.
- The view decodes exactly as the live event path decodes, through the single `decodeTaskEvent` routine. Do not add a second parser.
- Do not add reconnect-without-exit logic. An unrecoverable reader error is returned to `service.Run`, the process exits, and the pod restart replays the view.
- Do not change `/readiness` or `/healthz`, the live consumer group consumer, the deferred-respawn loop, the job watcher, or the zombie sweeper.
- Do not add a Prometheus metric beyond `TaskViewSkippedTotal`.
- Use `github.com/bborbe/errors` wrapping (`errors.Wrapf(ctx, err, ...)`), never `fmt.Errorf`; use `glog` for logs.
- Tests are Ginkgo/Gomega v2 in `package handler_test`, carry the existing license header, and use counterfeiter mocks where a dependency is faked.
- New code must have ≥80% statement coverage; every error path above is tested.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `cd /workspace && ROOTDIR=/workspace make precommit` — must pass.

Then confirm the contract:

- `grep -n 'github.com/bborbe/agent v0.113.0' go.mod` — prints exactly 1 line.
- `test -f mocks/task_view.go` — the counterfeiter fake was generated.
- `grep -q 'NewSimpleConsumer' pkg/factory/factory.go` — the non-group reader is used.
- `grep -q 'libkafka.HighWaterMarks' pkg/factory/factory.go` — the high-water marks are captured through the library.
- `grep -q 'event=task_view_ready' pkg/handler/task_view.go` — the ready line exists.
- `grep -q 'event=task_view_skip' pkg/handler/task_view.go` — the decode-failure line exists.
- `! awk '/func CreateTaskView/,/^}/' pkg/factory/factory.go | grep -qE 'OffsetManager|Group\('` — the view reader is non-group.
- `grep -q 'executor_task_view_skipped_total' pkg/metrics/metrics.go` — the counter is registered.
- `go test -mod=mod ./pkg/handler/... ./pkg/metrics/...` — exits 0.
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/handler/... && go tool cover -func=/tmp/cover.out` — confirm `NewTaskView`, `CaptureHighWaterMarks`, `ConsumeMessage`, `Ready`, `Snapshot`, `Get` and `decodeTaskEvent` are each exercised (`CreateTaskView` lives in `pkg/factory`, has no test, and is checked by the grep lines above).

Do NOT run `docker`, `make build`, `kubectl`, `dark-factory`, `gh`, or `scripts/*.sh` in this prompt — those are operator-executable.
</verification>
