---
status: completed
summary: Generated one UUID per spawn, recorded it as claude_session_id in the spawn-notification frontmatter, and injected it as CLAUDE_SESSION_ID into the spawned Job env, with mocks, tests, docs and changelog updated.
execution_id: agent-task-executor-anchor-session-id-exec-065-anchor-session-id-at-spawn
dark-factory-version: v0.198.17
created: "2026-11-10T18:15:00Z"
queued: "2026-11-10T17:11:15Z"
started: "2026-11-10T17:47:38Z"
completed: "2026-11-10T17:56:39Z"
---

# Record the agent's claude session id at spawn

<summary>
- Every spawned agent run now carries a `claude_session_id` in its task frontmatter, written before the pod starts.
- The same UUID is injected into the Job environment, so the agent image can force its CLI to run under exactly that id.
- The value rides the existing spawn notification, in the same frontmatter update that already carries `current_job` — no new command, no new write path, no new configuration.
- One id is generated per spawn and used for both the environment and the frontmatter, so the recorded id and the running id cannot diverge.
- Before this change a live agent session could not be attributed to the task it was running: the task file named the Job, not the session.
</summary>

<objective>
Give every spawned agent run an anchorable identity: generate a UUID at spawn, write it as `claude_session_id` in the spawn notification, and inject it into the Job as `CLAUDE_SESSION_ID` so the agent's CLI adopts it. A consumer that reads the task file can then match a live session to the task it is working.
</objective>

<context>
Read `docs/agent-crd-specification.md` for the shared env-var contract and `docs/claim-lease-protocol.md` for the spawn/claim gates.

Read `pkg/result_publisher.go` — `PublishSpawnNotification` (line ~509) builds a `taskcmd.UpdateFrontmatterCommand` carrying `spawn_notification`, `current_job` and `job_started_at`, then calls `p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd)`. Its interface doc comment is at lines 75-77. It is the pattern to extend, not to copy.

Read `pkg/handler/task_event_handler.go` — `spawnIfNeeded` (line ~860) delegates to `spawnIfNeededClaimed` (line ~888), which calls `spawnClaimedJob` (line ~1010). `spawnClaimedJob` calls `h.jobSpawner.SpawnJob(...)` (line ~1018), then `finishSpawn(ctx, task, config, jobName)` (line ~1041), which calls `PublishSpawnNotification` (line ~1063). The id must be generated once in `spawnClaimedJob` before `SpawnJob` and threaded through both calls.

Read `pkg/spawner/job_spawner.go` — `buildJobEnvBuilder` (line ~698, called from `SpawnJob` at line ~149) is the only place the Job's env is assembled: it adds `TASK_CONTENT`, `TASK_ID` (line ~728), `KAFKA_BROKERS`, `BRANCH`, `TOPIC_PREFIX`, `PHASE`, `TASK_TYPE` and then `config.Env`. The new variable belongs beside `TASK_ID`. `jobName` is computed at line ~145, before the env builder runs.

Read `pkg/result_publisher_test.go` — `decodeUpdateFrontmatterCommand` (line ~120) unmarshals a captured Kafka message into the command; the `PublishSpawnNotification` spec asserts on the `Updates` payload.

Read `pkg/job_watcher_internal_test.go` — `fakeResultPublisher` (line ~448) hand-implements the whole `ResultPublisher` interface, because the counterfeiter fake cannot be used there (import cycle).

`CLAUDE_SESSION_ID` is consumed by the agent image entrypoint (`bborbe/agent-claude` and its siblings), which passes it to the CLI as `--session-id`. The name is the contract with that image; the constraint below fixes it, so it is not changed here.

`github.com/google/uuid` is already a direct dependency of this module (`go.mod:23`).
</context>

<requirements>
1. Generate exactly one session id per spawn. Use `uuid.NewString()`. Generate it in `spawnClaimedJob` (`pkg/handler/task_event_handler.go`, line ~1010) immediately BEFORE `h.jobSpawner.SpawnJob(...)` (line ~1018), then pass that one value to `SpawnJob` (new parameter), to `finishSpawn` (line ~1041), and to `PublishSpawnNotification` (line ~1063). Do NOT generate it inside `SpawnJob`: `SpawnJob` returns only `(string, error)`, so an id generated there cannot reach the caller that writes the frontmatter. Do not generate twice.
2. Add `CLAUDE_SESSION_ID` to the Job env in `buildJobEnvBuilder`, beside `TASK_ID` (line ~728). Its value is the `sessionID` parameter, never read from the task.
3. Change `PublishSpawnNotification` to carry the id and add `"claude_session_id": sessionID` to the same `lib.TaskFrontmatter` as `current_job`. Update its interface doc comment to name all four keys it writes.
4. Log the generated id at `glog.V(2)` with the task identifier and job name. Do not log it anywhere else; it is an identifier, not a secret, but the existing style keeps per-run detail at V(2).
5. Regenerate both counterfeiter mocks with `make generate`; do not hand-edit `mocks/result_publisher.go` or `mocks/job_spawner.go`. Mirror the interface change in EVERY hand-written implementation: `pkg/job_watcher_internal_test.go` (`fakeResultPublisher`), `pkg/handler/claim_wiring_internal_test.go` (`parkRecordingPublisher`), `pkg/handler/claim_spawn_order_internal_test.go` (`stubResultPublisher`). Widening `SpawnJob` also breaks `stubJobSpawner` (`pkg/handler/claim_spawn_order_internal_test.go`), `inactiveSpawner` (`pkg/handler/task_event_handler_current_job_internal_test.go`), every `SpawnJob(...)` call in `pkg/spawner/job_spawner_test.go` and `pkg/spawner/job_namespace_test.go`, and the `PublishSpawnNotificationArgsForCall` destructuring in `pkg/handler/task_event_handler_test.go` (line ~513). Update them all. Do NOT make the new parameter variadic or optional.
6. Tests:
   - `pkg/result_publisher_test.go`: extend the `PublishSpawnNotification` spec (line ~181) to assert `cmd.Updates["claude_session_id"]` equals the id passed in and parses as a UUID (`uuid.Parse`). Change only the `cmd.Updates` assertion at line ~199 from `HaveLen(3)` to `HaveLen(4)` and rename that spec to "sends exactly four keys"; the `HaveLen(3)` at lines ~1017, ~1390 and ~1660 belong to other specs and must stay.
   - `pkg/spawner`: a spec asserting the built env contains `CLAUDE_SESSION_ID` whose value is the id passed to `SpawnJob`.
   - `pkg/handler/task_event_handler_test.go`: in the existing "publishes spawn notification after successful spawn" spec (line ~495), assert the id given to `SpawnJob` (`fakeSpawner.SpawnJobArgsForCall(0)`) equals the id given to `PublishSpawnNotification` (`fakeResultPublisher.PublishSpawnNotificationArgsForCall(0)`); this is the only place both boundaries are observable and is what proves the recorded id and the running id cannot diverge. Add a second spawn in the same spec and assert the two ids differ. Both fakes already exist in that harness (`mocks.FakeJobSpawner`, `mocks.FakeResultPublisher`), and `SpawnJobArgsForCall` exists in `mocks/job_spawner.go`, so this costs no new scaffolding.
7. Update `docs/agent-crd-specification.md` — the "Shared env vars" list (line ~64) and the `spec.env` row (line ~47) — to include `CLAUDE_SESSION_ID`.
8. Add a `## Unreleased` CHANGELOG bullet in the project's existing style, naming the field and the env var.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT hand-edit `mocks/result_publisher.go` or `mocks/job_spawner.go`; both are counterfeiter output, regenerated by `make generate`.
- Do NOT add a Config field, CRD knob, feature flag, or metric. The id is generated per spawn and written once; there is no configuration surface.
- Do NOT change the existing `current_job`, `job_started_at`, or `spawn_notification` semantics in `PublishSpawnNotification`.
- Do NOT add a guard against overwriting a live run's id: a task whose `current_job` is active is already suppressed by `suppressForActiveCurrentJob` (`pkg/handler/job_lookup_denied.go`) before `SpawnJob` runs, so at notification time the recorded job is never the one being notified for. A second guard would be unreachable.
- `CLAUDE_SESSION_ID` is read by the agent image's entrypoint, not this repo. The name is the contract with every agent image — do not rename it.
</constraints>

<verification>
- `make test` passes from the repository root.
- `make precommit` passes.
- `grep -rn 'CLAUDE_SESSION_ID' pkg/ docs/` shows the env builder, its test, and the CRD spec.
- `grep -n 'claude_session_id' pkg/result_publisher.go` shows the frontmatter key.
- `grep -c 'sends exactly four keys' pkg/result_publisher_test.go` prints `1`.
</verification>
