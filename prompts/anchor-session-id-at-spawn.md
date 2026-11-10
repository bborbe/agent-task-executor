---
status: draft
created: "2026-11-10T18:15:00Z"
---

# Record the agent's claude session id at spawn

<summary>
- Every spawned agent run now carries a `claude_session_id` in its task frontmatter, written before the pod starts.
- The same UUID is injected into the Job as `CLAUDE_SESSION_ID`, so the pod forces the CLI to run under exactly that id.
- The value is written by the existing spawn notification, in the same frontmatter update that already carries `current_job` — no new command, no new write path.
- A run that is re-driven gets a fresh id only when the previous Job is gone; a still-running Job's id is never overwritten.
- Before the fix, ~6.0k task files carried `current_job` and 8 carried `claude_session_id`, so no consumer could join a live session to the task it was working.
</summary>

<objective>
Give every spawned agent run an anchorable identity: generate a UUID at spawn, write it as `claude_session_id` in the spawn notification, and pass it to the Job as the `CLAUDE_SESSION_ID` env var so the agent's CLI adopts it with `--session-id`. This closes the gap where a live agent session cannot be attributed to the task it is running, because the task file names the Job but not the session.
</objective>

<context>
Read `CLAUDE.md` for project conventions.

Read `pkg/result_publisher.go` — `PublishSpawnNotification` (line ~483) builds a `taskcmd.UpdateFrontmatterCommand` carrying `spawn_notification`, `current_job` and `job_started_at`, then calls `p.publishRaw(ctx, taskcmd.UpdateFrontmatterCommandOperation, cmd)`. It is the pattern to extend, not to copy. Its doc comment at line ~63 must be updated to name the new key.

Read `pkg/handler/task_event_handler.go` — `spawnIfNeeded` calls `h.jobSpawner.SpawnJob(...)`, which returns `jobName`, then `finishSpawn(ctx, task, config, jobName)` publishes the notification (line ~1063). The id must be generated once and reach both the Job env and the notification, so this call chain is where the threading decision lives.

Read `pkg/spawner/job_spawner.go` — `buildTaskEnv` (around line 700) is the only place the Job's env is assembled: it adds `TASK_CONTENT`, `TASK_ID`, `KAFKA_BROKERS`, `BRANCH`, `TOPIC_PREFIX`, `PHASE`, `TASK_TYPE` and then `config.Env`. The new variable belongs beside `TASK_ID`.

Read `pkg/result_publisher_test.go` — `decodeUpdateFrontmatterCommand` unmarshals a captured Kafka message into the command; the `PublishSpawnNotification` spec asserts on the `Updates` payload. Extend that spec.

Read `pkg/job_watcher_internal_test.go` — `fakeResultPublisher` hand-implements the whole `ResultPublisher` interface (the counterfeiter fake cannot be used because of an import cycle), so any interface change must be mirrored there or the `pkg` test package stops compiling.

`github.com/google/uuid` is already a direct dependency of this module.
</context>

<requirements>
1. Generate exactly one session id per spawn. Use `uuid.NewString()`. Generate it at the point where the job name becomes known, and pass the same value to both the env builder and the spawn notification — two independently generated ids would write one and run another, which is the defect this change exists to prevent. If that requires widening `SpawnJob`, `buildTaskEnv` or `finishSpawn`, widen them; do not generate twice.
2. Add `CLAUDE_SESSION_ID` to the Job env in `buildTaskEnv`, beside `TASK_ID`. Its value is the generated id, never read from the task.
3. Change `PublishSpawnNotification` to carry the id and add `"claude_session_id": sessionID` to the same `lib.TaskFrontmatter` as `current_job`. Update its interface doc comment to name all four keys it writes.
4. **Never overwrite a live run's id.** `PublishSpawnNotification` must not replace a non-empty `claude_session_id` on the task's in-memory frontmatter when the recorded id belongs to a Job that is still active. Read the existing value the way `targetVaultFromTask` reads `target_vault` (`task.Frontmatter.String("claude_session_id")`), and skip the key when it is non-empty and the task's current `current_job` matches the Job being notified for. A re-drive after the previous Job finished writes a fresh id — that is correct, because it is a new session.
5. Log the generated id at `glog.V(2)` with the task identifier and job name. Do not log it anywhere else; it is an identifier, not a secret, but the existing style keeps per-run detail at V(2).
6. Regenerate the counterfeiter mock with `make generate`; do not hand-edit `mocks/result_publisher.go`. Mirror the interface change in the hand-written `fakeResultPublisher` in `pkg/job_watcher_internal_test.go`.
7. Tests:
   - `pkg/result_publisher_test.go`: extend the `PublishSpawnNotification` spec to assert `cmd.Updates["claude_session_id"]` is a non-empty, UUID-shaped string, and that two calls produce two different ids.
   - A spec for the live-run guard: a task whose frontmatter already carries a non-empty `claude_session_id` and a `current_job` equal to the notified Job publishes an update that omits the key (or carries the existing value — assert whichever the implementation does, and say which in the spec name).
   - `pkg/spawner`: a spec asserting the built env contains `CLAUDE_SESSION_ID` with the id passed in, and that its value equals the one written to the frontmatter for the same spawn.
8. Add a `## Unreleased` CHANGELOG bullet in the project's existing style, naming the field and the env var.
</requirements>

<verification>
- `make test` passes from the repository root.
- `make precommit` passes.
- `grep -rn 'CLAUDE_SESSION_ID' pkg/` shows the env builder and its test.
- `grep -n 'claude_session_id' pkg/result_publisher.go` shows the frontmatter key.
- The live-run guard spec fails if the skip is removed — confirm by deleting the guard locally.
</verification>
