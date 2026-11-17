---
status: completed
spec: [022-clear-ghost-current-job]
summary: Added a ghost current_job reconcile step that clears a current_job naming a Job absent from the cluster only when active-Job, JobExists and claim reads agree and no local spawn exists (re-checked under the per-task claim lock), with decision-table and summary specs plus changelog.
execution_id: agent-task-executor-ghost-current-job-exec-060-spec-022-clear-ghost-current-job
dark-factory-version: v0.198.17
created: "2026-11-08T14:55:47Z"
queued: "2026-11-08T15:03:34Z"
started: "2026-11-08T15:12:33Z"
completed: "2026-11-08T15:20:22Z"
---

# Clear ghost current_job on the reconcile pass

<summary>
- An open task whose `current_job` names a Job that no longer exists anywhere now has that field cleared by the executor's periodic reconcile pass. Until now the field stayed set forever once the Job disappeared unobserved, for example after TTL garbage collection across a restart, the weekly cluster reboot, or a manual delete.
- The clear happens only when every independent read agrees that nothing runs. No Job for the task is active in this cluster, the named Job is absent, and the task's shared claim reads free. The executor also has no spawn of the task recorded or in flight.
- The executor's own concurrent spawn can never race the clear. The final "no local spawn" check is repeated under the same per-task lock the spawn path takes for its claim.
- The only write is `current_job: ""`. Status, phase, claim, body, `job_started_at` and dispatch are all untouched.
- Completed or aborted records, records on another stage, and records with an empty `current_job` are never examined.
- Any read failure, unreadable claim, unresolvable Config or publish failure leaves the field alone and is retried on the next pass. No outcome fails the pass.
- Every pass that completes its drive loop logs one summary line. Every examined record also logs one cleared line or one kept line that names the guard which kept it.
- No new flags, env vars, metrics or wiring changes are added.
- A decision-table test suite pins every guard, the check order, the lock re-check and the log lines.
- The change is recorded in the changelog.
</summary>

<objective>
Add one step to `ReconcileOnce`. It runs after the drive loop completes and clears a "ghost" `current_job` (a field naming a Job that runs nowhere) on every open record this executor owns. It acts only when three independent remote reads agree, and only after the executor's own spawn path has been excluded under its per-task lock. Every other path that clears `current_job` reacts to an event the executor observed. A Job removed before such an event leaves the field set indefinitely, which misleads every reader and keeps the ghost-claim regression metric above 0.
</objective>

<context>
Read `CLAUDE.md` if present, and `README.md`, for project conventions.

Read these files in full before changing anything:

- `pkg/handler/task_event_handler.go` covers the following:
  - `ReconcileOnce` (the pass; the drive loop is the `for _, candidate := range candidates` loop, followed by `metrics.ReconcilePassesTotal.WithLabelValues("ok").Inc()`);
  - `ownedTasks`, `passDone`, `isJobActive`, `resolveConfig`, `resolveConfigForReconcile`;
  - `markSpawnPending`, `clearSpawnPending`, `lockClaimRelease`, `spawnPendingFor`;
  - `spawnIfNeededClaimed` (see how it calls `markSpawnPending` and then `lockClaimRelease` around `acquireSpawnClaim`);
  - `storeSpawnedJob`, `IsTerminalStatus`, `stageSkipResult`;
  - the `taskEventHandler` struct (`taskStore *pkg.TaskStore`, `branch base.Branch`, `jobSpawner spawner.JobSpawner`, `resultPublisher pkg.ResultPublisher`, `resolver pkg.ConfigResolver`).
  - **This file is 1992 lines and the repo's revive `file-length-limit` is 2000.** Everything new goes in new files. `task_event_handler.go` gains at most 4 lines. The requirement 6 `dupl` fallback may edit `resolveConfigForReconcile`, but the file must stay ≤ 2000 lines.
- `pkg/handler/task_event_handler_claim_probe.go` covers `claimProbe(ctx, task) (claimProbeResult, error)` and the constants `claimProbeUnavailable`, `claimProbeHeld` and `claimProbeFree`. It also covers `claimTargetFor` and `countClaimProbeFailure` (the probe already logs and counts its own failures).
- `pkg/handler/job_namespace.go` covers `jobNamespaceForTask(ctx, config, taskID) (libk8s.Namespace, error)`. Note that `jobNamespace` returns `h.namespace` for a nil config; this step must NOT rely on that fallback.
- `pkg/handler/task_event_handler_previous_assignee.go` covers `inTypePreviousAssignee(ctx, previousAssignee, taskType string) (string, error)`. It logs at V(2) and increments no metric.
- `pkg/handler/job_cache.go` covers `activeJobsFor`, the per-pass `ListActiveJobs` cache that `isJobActive` uses for a non-empty assignee.
- `pkg/result_publisher.go` covers the `ResultPublisher` interface and `PublishClearCurrentJob(ctx context.Context, task lib.Task, jobName string) error`. It writes only `current_job: ""`, and its 1 h LRU dedupe is keyed by `jobName`, so a repeat within the hour returns nil without writing. **Do not change this file.**
- `pkg/spawner/job_spawner.go` covers the `JobSpawner` interface. The relevant method is `JobExists(ctx context.Context, namespace k8s.Namespace, jobName string) (bool, error)`, which returns `(false, nil)` on NotFound.
- `pkg/job_namespace.go` covers `ResolveJobNamespace`. A Config whose `JobNamespace` equals the executor namespace is an error, and that is how a test produces an invalid namespace.
- Test style references:
  - `pkg/handler/task_reconcile_test.go` and `pkg/handler/task_event_handler_test.go` (external `handler_test` package, Counterfeiter fakes from `mocks`, `handler.NewTaskEventHandler(...)`, `testTaskVault = "openclaw"`).
  - `pkg/handler/task_event_handler_claim_test.go` (`newClaimHarness`, `noopClaimJobControl`, `handler.WithClaimClientFactory`, `handler.WithClaimJobControl`).
  - `pkg/handler/index_terminal_deferred_test.go` (`deferredGateCaptureStderr`, the glog-capture pattern at V(2)).
  - `pkg/handler/redrive_streak_window_internal_test.go` (a Ginkgo `Describe` in the internal `package handler`).
  - `pkg/handler/index_terminal_log_internal_test.go` (internal `ReconcileOnce` driven by an embedded-interface `TaskView`).
  - `pkg/handler/task_event_handler_current_job_internal_test.go` (`readOnlyClaimClient`, `inactiveSpawner`, `goneSpawner`).
  - `pkg/handler/claim_job_control_internal_test.go` (`staticResolver`).
  - `pkg/handler/claim_spawn_order_internal_test.go` (`stubResultPublisher`).
  - `pkg/handler/claim_wiring_internal_test.go` (`noopClaimRefClient`).
  - `pkg/handler/view_readiness_internal_test.go` (`noopJobControl` usage).
- `docs/claim-lease-protocol.md` covers claim semantics (read only).

Coding-plugin guides (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` covers Ginkgo v2/Gomega and Counterfeiter conventions.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` covers `github.com/bborbe/errors` (`errors.Wrapf(ctx, err, ...)`, never `fmt.Errorf`).
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md` covers `glog.V(n)` levels and the `event=` key=value style.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-concurrency-patterns.md` covers the lock-and-recheck pattern.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` covers the `## Unreleased` bullet format.

Verified facts (do not re-derive):
- **Internal and external Ginkgo specs share one suite.** `TestHandler` / `RunSpecs` lives in `pkg/handler/task_event_handler_test.go` (`package handler_test`). A Ginkgo `Describe` declared in an internal `package handler` test file IS run by that suite, because both packages link into one test binary. This was confirmed by running `go test ./pkg/handler/ -v -run TestHandler -args -ginkgo.focus='redriveStreakExpired'`, which reported `Ran 3 of 393 Specs`. The "Plain Go tests, not Ginkgo" header comment in several internal files is stale. Ignore it.
- Internal test files (`package handler`) cannot import `mocks`, because `mocks/task_view.go` and `mocks/task_event_handler.go` import `pkg/handler`. Internal specs use hand-written fakes.
- Counterfeiter fakes used externally:
  - `mocks.FakeJobSpawner`: `JobExistsReturns`, `JobExistsCalls`, `JobExistsCallCount`, `ListActiveJobsReturns`, `IsJobActiveReturns`.
  - `mocks.FakeConfigResolver`: `ResolveReturns`, `ResolveByTaskTypeReturns`.
  - `mocks.FakeResultPublisher`: `PublishClearCurrentJobReturns`, `PublishClearCurrentJobCallCount`, `PublishClearCurrentJobArgsForCall`.
  - `mocks.FakeVaultAPIClaimClient`, which fakes `claim.ClaimClient`: `ReadReturns`, `ReadCalls`, `ReadCallCount`.
  - `mocks.FakeClaimRefClient`.
  - `mocks.FakeTaskView`: `ReadyReturns`, `SnapshotReturns`, `GetReturns`.
- `claim.ErrClaimFree` (in `pkg/claim/vault_api_client.go`). When `ClaimClient.Read` returns it, `claimProbe` yields `claimProbeFree`. A `*claim.Claim{Holder: "sun", LeaseUntil: libtime.DateTime(time.Now().Add(2 * time.Minute))}` yields `claimProbeHeld`. Any other error yields `(claimProbeUnavailable, err)`.
- `pkg.ClaimModeOff` and `pkg.ClaimModeShadow` make `claimProbe` return `(claimProbeUnavailable, nil)` without reading.
- `domain.TaskPhaseHumanReview` (`"human_review"`) is NOT in `defaultTriggerPhases`. A ghost fixture with `phase: human_review` is therefore refused by the gather phase (`ReconcileRefusedGatePhaseNotTriggered`). The drive loop never spawns it, and every publisher call in the pass comes from the new step.
- In the gather phase, `reconcileTask` → `resolveConfigForReconcile` behaves as follows:
  - An unknown assignee increments `metrics.TaskEventsTotal{skipped_unknown_assignee}` +1 and `metrics.SkippedUnknownAssigneeTotal{<assignee>}` +1.
  - An empty assignee whose `task_type` resolves to `ErrConfigNotFound` increments `TaskEventsTotal{skipped_unknown_assignee}` +1 only.
  - A Config with an invalid `JobNamespace` increments neither, because `rejectInvalidJobNamespace` runs only on the spawn path.
  - The new step must add nothing on top of these gather increments.
- `CHANGELOG.md` starts with `## v0.57.1`. There is no `## Unreleased` section yet.
</context>

<requirements>
1. **Create `pkg/handler/ghost_current_job.go`** (`package handler`, with the BSD license header used by every file in the package). Put the whole step in this file. Split it into small methods so that `funlen` (80 lines / 50 statements), `gocognit` (20) and `nestif` (4) pass. Use this shape:

   ```go
   // Guard labels for event=ghost_current_job_kept. Closed set.
   const (
   	ghostGuardStoreEntry          = "store_entry"
   	ghostGuardNamespaceUnresolved = "namespace_unresolved"
   	ghostGuardJobActive           = "job_active"
   	ghostGuardJobExists           = "job_exists"
   	ghostGuardClaimHeld           = "claim_held"
   	ghostGuardClaimUnprobeable    = "claim_unprobeable"
   	ghostGuardReadFailed          = "read_failed"
   	ghostGuardPublishFailed       = "publish_failed"
   )

   // clearGhostCurrentJobs is the reconcile pass's ghost current_job step.
   func (h *taskEventHandler) clearGhostCurrentJobs(passCtx context.Context, owned []lib.Task)
   ```

   Write a doc comment on `clearGhostCurrentJobs` that states the following:
   - why the step exists: the informer has no delete handler, spawned Jobs carry no ownerReference, and the store is empty after a restart;
   - that it writes only `current_job: ""`;
   - that it runs only on passes that complete the drive loop;
   - the residual cross-cluster window, for which no vault-side compare-and-set exists. That window is bounded by the foreign cluster's held claim, by `IsJobActive`, and by the foreign job watcher writing the field at terminal.

2. **Candidate filter.** Add `func (h *taskEventHandler) isGhostCandidate(task lib.Task) bool`. It returns true only when all of these hold:
   - `task.Frontmatter.CurrentJob() != ""`;
   - `!IsTerminalStatus(task.Frontmatter.Status())`;
   - `task.Frontmatter.Stage() == string(h.branch)`;
   - `!h.isForeignCluster(task)`. A record tagged for another cluster belongs to that cluster's executor, so this step never clears it.

   The stage rule deliberately ignores `h.stageFilter`. It is stricter than dispatch's `stageSkipResult`; state that in a comment. Non-candidates get no reads and no log lines.

3. **Step loop.** `clearGhostCurrentJobs` does the following:
   - It builds the candidate slice from `owned` in order.
   - It iterates the candidates. Before each one it checks `h.passDone(passCtx)`. On expiry it sets `unreached = len(candidates) - i` and stops.
   - For each reached candidate it increments `scanned`, then calls the evaluation in requirement 4 and the publish in requirement 5. It increments `cleared` on success and `kept` otherwise.
   - It emits exactly one per-record line per reached candidate:
     - cleared: `glog.Infof("event=ghost_current_job_cleared task=%s job=%s", task.TaskIdentifier, jobName)`
     - kept: `glog.V(2).Infof("event=ghost_current_job_kept task=%s job=%s guard=%s", task.TaskIdentifier, jobName, guard)`. When an error caused the guard (`read_failed`, `publish_failed`, a resolution error under `namespace_unresolved`, or a probe error under `claim_unprobeable`), append ` err=%v` with the error after `guard=<label>`. Do not emit a second line for the same candidate.
   - It always ends with exactly one summary line, even when every count is 0 and even after a mid-step expiry:
     `glog.V(2).Infof("event=ghost_current_job_pass scanned=%d cleared=%d kept=%d unreached=%d", ...)`.
     The invariant `cleared + kept == scanned` must hold.
   - It never returns an error and never panics on any outcome.

4. **Evaluation, in this exact order.** Add `func (h *taskEventHandler) ghostCurrentJobGuard(ctx context.Context, task lib.Task, jobName string) (string, error)`. It returns `("", nil)` when every check passes. Otherwise it returns the first failing guard label, plus the causing error where one exists.
   1. **Local spawn state.** Add `func (h *taskEventHandler) hasLocalSpawnState(taskID lib.TaskIdentifier) bool`, returning `true` when `h.taskStore.Load(taskID)` reports ok or `h.spawnPendingFor(taskID)` is true. When it returns true, the guard is `store_entry`.
   2. **Config and namespace.** Resolve the Config with the non-counting resolver from requirement 6.
      - A nil Config or a resolution error → `namespace_unresolved`.
      - Otherwise `namespace, err := h.jobNamespaceForTask(ctx, config, task.TaskIdentifier)`; an error → `namespace_unresolved`.
      - There is no fallback to `h.namespace` for a nil Config.
   3. **Active Job.** `active, err := h.isJobActive(ctx, task, config)`. An error → `read_failed`; `active` → `job_active`.
   4. **Named Job.** `exists, err := h.jobSpawner.JobExists(ctx, namespace, jobName)`. An error → `read_failed`; `exists` → `job_exists`.
   5. **Claim probe.** `result, err := h.claimProbe(ctx, task)`.
      - An error → `claim_unprobeable`.
      - `claimProbeHeld` → `claim_held`.
      - `claimProbeFree` → pass.
      - Any other value (`claimProbeUnavailable`) → `claim_unprobeable`.

   These are at most 3 remote reads per candidate per pass: the active-Job read (usually served from the per-pass `ListActiveJobs` cache), `JobExists`, and the claim probe. Do not add any other remote read. Do not wrap the probe's error a second time in a counting call, because `claimProbe` already counts and logs it via `countClaimProbeFailure`.

5. **Publish under the claim lock.** Add `func (h *taskEventHandler) publishGhostClear(ctx context.Context, task lib.Task, jobName string) (string, error)`:

   ```go
   unlock := h.lockClaimRelease(task.TaskIdentifier)
   defer unlock()
   if h.hasLocalSpawnState(task.TaskIdentifier) {
   	return ghostGuardStoreEntry, nil
   }
   if err := h.resultPublisher.PublishClearCurrentJob(ctx, task, jobName); err != nil {
   	return ghostGuardPublishFailed, err
   }
   return "", nil
   ```

   Comment it with the race argument. The spawn path calls `markSpawnPending` and only then takes `lockClaimRelease` for its claim. Either the mark came first, and this re-check sees it, or the mark comes later, and the spawn's claim and spawn notification follow the published clear. `PublishClearCurrentJob` must be called while the lock is held. Pass the snapshot `task` unchanged.

6. **Non-counting Config resolution.** Add `func (h *taskEventHandler) resolveConfigQuiet(ctx context.Context, task lib.Task) (*pkg.AgentConfiguration, error)` in the new file. It mirrors `resolveConfigForReconcile`'s lookup rules exactly, but increments no metric and emits no log line of its own:
   - When `task.Frontmatter.Assignee() != ""`:
     - call `h.resolver.Resolve(ctx, string(assignee))`;
     - `stderrors.Is(err, pkg.ErrConfigNotFound)` → `(nil, nil)`;
     - another error → `(nil, errors.Wrapf(ctx, err, "resolve agent config for task %s", task.TaskIdentifier))`;
     - success → `(&resolved, nil)`.
   - When the assignee is empty:
     - an empty `task.Frontmatter.TaskType()` → `(nil, nil)`;
     - otherwise read `previous_assignee` via `task.Frontmatter.String("previous_assignee")` and pass it through `h.inTypePreviousAssignee(ctx, raw, string(taskType))`, returning its error;
     - then call `h.resolver.ResolveByTaskType(ctx, string(taskType), previousAssignee)`. `ErrConfigNotFound` → `(nil, nil)`; another error → `(nil, err)`; success → `(&resolved, nil)`.

   **Leave `resolveConfig` and `resolveConfigForReconcile` byte-for-byte unchanged**, so the Kafka path's and the reconcile gather's counting cannot change. If `make precommit`'s `dupl` linter flags the new function as a duplicate, resolve it as follows:
   - extract the shared lookup into one non-counting helper in `ghost_current_job.go`;
   - have `resolveConfigForReconcile` call that helper for its `task_type` branch;
   - keep every existing `metrics.TaskEventsTotal` / `metrics.SkippedUnknownAssigneeTotal` increment and every existing log line in `resolveConfig` / `resolveConfigForReconcile` on exactly the same conditions as today;
   - keep the line count of `task_event_handler.go` at or below 2000.

7. **Wire the step into `ReconcileOnce`** in `pkg/handler/task_event_handler.go`. Put it immediately after the closing brace of the drive loop `for _, candidate := range candidates { ... }` and immediately before `metrics.ReconcilePassesTotal.WithLabelValues("ok").Inc()`:

   ```go
   	// Ghost current_job step: see ghost_current_job.go.
   	h.clearGhostCurrentJobs(passCtx, owned)
   ```

   No other line in `task_event_handler.go` changes, except the requirement 6 `dupl` fallback (if and only if `dupl` fires). Every early return (view not ready, unstamped or mismatched vault, aborted gather, aborted drive loop) therefore skips the step and emits no summary line. The `ok` accounting and the `event=reconcile` line still follow the step unchanged, even when the step stopped on expiry.

8. **External decision-table specs.** Create `pkg/handler/ghost_current_job_test.go` (`package handler_test`) with a top-level `Describe("ghost current_job", ...)`. Every spec in this requirement and in requirement 10 must sit under a `Describe`/`Context` whose full text contains `ghost current_job`. Use `DescribeTable`/`Entry` or plain `It`s; each variant listed below must be its own spec.

   Harness, built in `BeforeEach` through a local helper that takes a `pkg.ClaimMode` and a `stageFilter bool`:
   - `fakeSpawner := new(mocks.FakeJobSpawner)` with `ListActiveJobsReturns(map[lib.TaskIdentifier]bool{}, nil)`, `IsJobActiveReturns(false, nil)` and `JobExistsReturns(false, nil)`.
   - `fakeResolver := new(mocks.FakeConfigResolver)` with `ResolveReturns(pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"}, nil)`. The Config sets no `TaskType`/`TaskTypes`, so the gather's type gate stays inert.
   - `fakePublisher := new(mocks.FakeResultPublisher)`.
   - `fakeClaimClient := new(mocks.FakeVaultAPIClaimClient)` with `ReadReturns(nil, claim.ErrClaimFree)`.
   - `fakeView := new(mocks.FakeTaskView)` with `ReadyReturns(true)` and `GetReturns(lib.Task{}, false)`.
   - `taskStore := pkg.NewTaskStore()`, the real store.
   - The handler is `handler.NewTaskEventHandler(fakeSpawner, "dev", base.Branch("prod"), "", stageFilter, true, fakeResolver, fakePublisher, taskStore, libtime.NewCurrentDateTime(), fakeView, testTaskVault, handler.DefaultReconcilePassTimeout, handler.DefaultReconcileInterval, mode, func() claim.ClaimRefClient { return new(mocks.FakeClaimRefClient) }, handler.WithClaimJobControl(noopClaimJobControl{}), handler.WithClaimClientFactory(func() claim.ClaimClient { return fakeClaimClient }))`. Default `mode` is `pkg.ClaimModeOn` and default `stageFilter` is `true`.
   - Add a local glog capture helper that mirrors `deferredGateCaptureStderr`: `logtostderr=true`, `v=2`, an `os.Stderr` pipe and `glog.Flush()`, restoring `v=0` and `logtostderr=false` afterwards. Give it a distinct name, e.g. `ghostCaptureLog`, so it does not collide with other test files.

   Base record (a fresh copy per spec). Hold it in a variable named `baseTask`, because the harness imports the `base` package (`base.Branch("prod")`):

   ```go
   lib.Task{
   	TaskIdentifier: "ghost-task-1",
   	Vault:          testTaskVault,
   	Frontmatter: lib.TaskFrontmatter{
   		"status":      "in_progress",
   		"phase":       string(domain.TaskPhaseHumanReview),
   		"assignee":    "claude",
   		"stage":       "prod",
   		"current_job": "ghost-job-1",
   	},
   }
   ```

   Comment that `human_review` keeps the drive loop from spawning, so every publisher call in the pass is the step's.

   Rows. Each runs `ReconcileOnce(ctx)`, which must return nil, inside the capture helper:

   | Row | Varied | Assert |
   |---|---|---|
   | a | nothing | `PublishClearCurrentJobCallCount() == 1`; `ArgsForCall(0)` has `jobName == "ghost-job-1"` and task id `ghost-task-1`; log contains `event=ghost_current_job_cleared task=ghost-task-1 job=ghost-job-1` and `event=ghost_current_job_pass scanned=1 cleared=1 kept=0 unreached=0` |
   | b | `JobExistsReturns(true, nil)` | 0 publishes; `fakeClaimClient.ReadCallCount() == 0`; log contains `guard=job_exists` |
   | c1 | `ListActiveJobsReturns(map[lib.TaskIdentifier]bool{"ghost-task-1": true}, nil)` | 0 publishes; `JobExistsCallCount() == 0`; `guard=job_active` |
   | c2 | record `assignee: ""` and `task_type: "ghost-type"`; `ResolveByTaskTypeReturns(pkg.AgentConfiguration{Assignee: "", Image: "my-image:latest"}, nil)`; `IsJobActiveReturns(true, nil)` | 0 publishes; `JobExistsCallCount() == 0`; `IsJobActiveCallCount() >= 1`; `guard=job_active` |
   | d | `ReadReturns(&claim.Claim{Holder: "sun", LeaseUntil: libtime.DateTime(time.Now().Add(2 * time.Minute))}, nil)` | 0 publishes; `guard=claim_held` |
   | e1 | `ReadReturns(nil, stderrors.New("vault-api down"))` | 0 publishes; `guard=claim_unprobeable` |
   | e2 | handler built with `pkg.ClaimModeOff` | 0 publishes; `guard=claim_unprobeable` |
   | f1 | `JobExistsReturns(false, stderrors.New("api down"))` | 0 publishes; `guard=read_failed` |
   | f2 | `ListActiveJobsReturns(nil, stderrors.New("api down"))` | 0 publishes; `guard=read_failed` |
   | g | `fakeClaimClient.ReadCalls(func(context.Context, claim.ClaimTarget) (*claim.Claim, error) { taskStore.Store("ghost-task-1", baseTask); return nil, claim.ErrClaimFree })` | 0 publishes; `guard=store_entry`; `ReadCallCount() == 1` (the store entry was inserted after the first local check, so only the locked re-check can see it) |
   | i1 / i2 | `status: completed` / `status: aborted` | 0 publishes; `JobExistsCallCount() == 0`; no `ghost-task-1` in any `ghost_current_job_` line; `scanned=0` |
   | j1 | `stage: dev` | 0 publishes; `JobExistsCallCount() == 0`; `scanned=0` |
   | j2 | `stage: dev` with the handler built with `stageFilter=false` | same as j1 (the step's stage rule ignores `STAGE_FILTER`) |
   | k | `current_job: ""` | 0 publishes; `JobExistsCallCount() == 0`; `scanned=0` |
   | l1 | record `assignee: "ghost-unknown-agent"`; `ResolveReturns(pkg.AgentConfiguration{}, pkg.ErrConfigNotFound)` | 0 publishes; `JobExistsCallCount() == 0`; `guard=namespace_unresolved`; across the pass, `TaskEventsTotal{skipped_unknown_assignee}` delta `== 1` and `SkippedUnknownAssigneeTotal{ghost-unknown-agent}` delta `== 1` (the gather's own increments; a counting step would make them 2) |
   | l2 | record `assignee: ""`, `task_type: "ghost-type"`; `ResolveByTaskTypeReturns(pkg.AgentConfiguration{}, pkg.ErrConfigNotFound)` | 0 publishes; `guard=namespace_unresolved`; `TaskEventsTotal{skipped_unknown_assignee}` delta `== 1`; `TaskEventsTotal{error}` delta `== 0` |
   | l3 | `ResolveReturns(pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest", JobNamespace: "dev"}, nil)` (equals the executor namespace, so it is invalid) | 0 publishes; `JobExistsCallCount() == 0`; `guard=namespace_unresolved`; `TaskEventsTotal{skipped_unknown_assignee}` and `TaskEventsTotal{error}` deltas `== 0` |
   | m | `PublishClearCurrentJobReturns(stderrors.New("kafka down"))`; run `ReconcileOnce` twice | each pass returns nil; `PublishClearCurrentJobCallCount() == 2`; log contains `guard=publish_failed` twice |
   | n | the record carries a `cluster` tag naming a different cluster than the handler's (see `taskCluster` / `isForeignCluster`) | 0 publishes; `JobExistsCallCount() == 0` |

   Read metric deltas with `testutil.ToFloat64(metrics.TaskEventsTotal.WithLabelValues("skipped_unknown_assignee"))` and so on, before and after the pass.

9. **Internal spawn-pending specs (row h).** Create `pkg/handler/ghost_current_job_internal_test.go` (`package handler`, Ginkgo, modelled on `redrive_streak_window_internal_test.go`) with `Describe("ghost current_job locked re-check", ...)`. Build the handler with `NewTaskEventHandler(...)` and type-assert it to `*taskEventHandler`. Use these internal fakes:
   - spawner: `goneSpawner{}` (IsJobActive false, ListActiveJobs empty, JobExists false);
   - resolver: `staticResolver{config: pkg.AgentConfiguration{Assignee: "claude", Image: "img"}}`;
   - view: a new `ghostView struct { TaskView; tasks []lib.Task }` with `Ready() bool { return true }` and `Snapshot() []lib.Task { return v.tasks }`;
   - publisher: a new `ghostRecordingPublisher struct { stubResultPublisher; clears []string }` whose pointer-receiver `PublishClearCurrentJob` appends `jobName` and returns nil;
   - claim mode `pkg.ClaimModeOn`, `claimRefClientFactory` `func() claim.ClaimRefClient { return noopClaimRefClient{} }`, `WithClaimJobControl(noopJobControl{})`, and `WithClaimClientFactory(func() claim.ClaimClient { return readOnlyClaimClient{read: ...} })`;
   - a local V(2) capture helper with a distinct name, e.g. `ghostCaptureInternalLog`, using Gomega `Expect` (the existing `captureRefusalLog` takes a `*testing.T` and cannot be called from Ginkgo).

   Declare `var h *taskEventHandler` before construction so the claim-read closure can reach it. Use the same base record as requirement 8, with vault equal to the handler's task vault.
   - `It("keeps the field when a spawn is marked pending during the claim read")`: the read func calls `h.markSpawnPending("ghost-task-1")` and returns `nil, claim.ErrClaimFree`. Assert no clear was published and the captured log contains `guard=store_entry`.
   - `It("keeps the field without remote reads when a spawn is already pending")`: call `h.markSpawnPending("ghost-task-1")` before the pass, and make the read func set a flag. Assert no clear, `guard=store_entry`, and the flag unset (the probe was never reached).
   - `It("clears when nothing is pending")`: the read func returns `nil, claim.ErrClaimFree`. Assert `clears == []string{"ghost-job-1"}`. This is the control that proves the fakes reach the publish.

10. **Summary-line specs** in `pkg/handler/ghost_current_job_test.go`, under a `Context("ghost current_job pass summary", ...)` nested in the requirement 8 `Describe`:
    - **Two zero-candidate passes.** `SnapshotReturns(nil)`, run `ReconcileOnce` twice inside one capture, and assert `strings.Count(out, "event=ghost_current_job_pass scanned=0 cleared=0 kept=0 unreached=0") == 2`.
    - **View not ready.** `ReadyReturns(false)`, then assert `strings.Count(out, "event=ghost_current_job_pass") == 0` and `PublishClearCurrentJobCallCount() == 0`.
    - **Context expires mid-step.**
      - Use two base-shaped records, `ghost-task-1`/`ghost-job-1` and `ghost-task-2`/`ghost-job-2`, in that snapshot order.
      - Run `ctx, cancel := context.WithCancel(context.Background())`.
      - Set `fakeSpawner.JobExistsCalls(func(context.Context, libk8s.Namespace, string) (bool, error) { cancel(); return false, nil })`.
      - Assert the log contains `event=ghost_current_job_pass scanned=1 cleared=1 kept=0 unreached=1`, `PublishClearCurrentJobCallCount() == 1`, and that `ghost-task-2` appears in no `ghost_current_job_cleared`/`ghost_current_job_kept` line.

11. **Existing specs.** Run the whole `pkg/handler` suite. The step now performs reads on every pass. When a pre-existing spec fails only because of extra `IsJobActive`/`ListActiveJobs`/`JobExists` calls, or an extra `PublishClearCurrentJob` call made by the step, fix that spec's fixture, not the production code. Prefer narrowing the old assertion to the calls it is about (for example, filter `PublishClearCurrentJobArgsForCall` by the expected jobName) over changing inputs the drive loop also reads. Change an input only when the spec under test does not depend on it, and add a one-line comment saying why. Never weaken a guard or reorder the checks to satisfy an old assertion.

12. **CHANGELOG.** Add a `## Unreleased` section directly above `## v0.57.1` in `CHANGELOG.md`, containing one bullet that begins with `- fix:`. The bullet says the following:
    - the reconcile pass now clears a `current_job` that names a Job absent from the cluster;
    - it does so only when no Job for the task is active here, the named Job is gone, the shared claim reads free, and this executor has no spawn recorded or in flight (re-checked under the per-task claim lock);
    - it writes only `current_job: ""`;
    - it logs `event=ghost_current_job_pass` per pass and `event=ghost_current_job_cleared` / `event=ghost_current_job_kept guard=<label>` per record;
    - claim modes `off` and `shadow` never clear;
    - the incident: on 2026-11-06, one prod record named `pr-reviewer-agent-99750c2d-20261103233145` while the cluster held no agent Jobs.

13. **Self-check before finishing.** Re-run every command in `<verification>` and confirm each passes. Walk the spec's decision table (rows a–m), the summary-line criterion and the per-record-line criterion against your specs, and confirm each is asserted.
</requirements>

<constraints>
- `ResultPublisher` and `PublishClearCurrentJob` are unchanged. `pkg/result_publisher.go` is not edited.
- No new flags, env vars or metrics. No `main.go` change.
- Dispatch is unchanged. Do not edit `pkg/handler/task_event_handler_current_job.go`, `pkg/handler/task_event_handler_claim_probe.go`, `pkg/handler/job_lookup_denied.go` or `pkg/result_publisher.go`. `suppressForActiveCurrentJob`, `jobProvablyFinished`, `currentJobFinishedWithoutStoreEntry`, the claim probe and the spawn path keep their current behavior.
- The step never releases, renews or writes any distributed claim. It never touches records whose `status` is `completed` or `aborted`. It writes no field other than `current_job`, so `job_started_at` stays as it is.
- Records whose claim lease is unreadable (`claim_held`) or whose claim key cannot be derived (`claim_unprobeable`) are left alone. Bounding their dispatch suppression is out of scope.
- No vault-side compare-and-set. `UpdateFrontmatterCommand` (`bborbe/agent` `command/task`, v0.113.0) carries no precondition.
- Use at most 3 remote reads per candidate per pass: the active-Job read, `JobExists` and the claim probe.
- The step must not increment `TaskEventsTotal` or `SkippedUnknownAssigneeTotal`. The existing callers' counting in `resolveConfig` / `resolveConfigForReconcile` is unchanged.
- Write tests with Ginkgo v2/Gomega. Use Counterfeiter fakes for interfaces in the external package. The task store is always the real `*pkg.TaskStore`.
- Wrap errors with `github.com/bborbe/errors` (`errors.Wrapf(ctx, err, ...)`), never `fmt.Errorf`.
- `pkg/handler/task_event_handler.go` must stay at or below 2000 lines (revive `file-length-limit`). New code goes in new files.
- Do NOT commit. dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit`. It must pass.

Run `go test ./pkg/handler/ -v -args -ginkgo.focus='ghost current_job' > /tmp/ghost.out 2>&1; grep -qE 'Ran (2[5-9]|[3-9][0-9]) of' /tmp/ghost.out && grep -q 'SUCCESS!' /tmp/ghost.out && echo GHOST_OK`. It must print `GHOST_OK`. The count fails the command when the focus selects fewer than the 25 specs requirements 8–10 define, and `SUCCESS!` fails it when a selected spec fails.

Run `test "$(wc -l < pkg/handler/task_event_handler.go)" -le 2000 && echo LEN_OK`. It must print `LEN_OK`.

Run `grep -c 'clearGhostCurrentJobs(passCtx, owned)' pkg/handler/task_event_handler.go`. It must print `1`.

Run `awk '/^## /{sec=$0} /^- fix: .*current_job/{print sec; exit}' CHANGELOG.md`. It must print `## Unreleased`.
</verification>
