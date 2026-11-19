---
status: completed
spec: [024-run-claim-spawn-gate]
summary: Added a read-only run_claim liveness gate to the spawn funnel, refusing a second Job for a task with a live run_claim while deferring without writes, parks, or redrive charge.
execution_id: bcb622d7-36f8-5aa2-bf9b-a723cccf2ecc-exec-068-spec-024-run-claim-spawn-gate
dark-factory-version: dev
created: "2026-11-19T09:08:58Z"
queued: "2026-11-19T09:20:22Z"
started: "2026-11-19T09:20:24Z"
completed: "2026-11-19T09:45:03Z"
branch: dark-factory/run-claim-spawn-gate
---

# Refuse a second spawn while a task carries a live `run_claim`

<summary>
- The executor now refuses to start a second Job for a task whose frontmatter carries a recent `run_claim`.
- The refusal is a deferral: no Job is created, no result is published, nothing is parked, and the redrive budget is not touched.
- A missing `run_claim`, one at or beyond the TTL, or one that does not parse as RFC3339 leaves today's behaviour unchanged and the spawn proceeds.
- The existing `current_job` gate still runs first and still wins when both signals are present; the new log event is not emitted on that path.
- A suppressed spawn is logged at info with the task id, the claim timestamp and the claim's age.
- A suppressed spawn is counted in the existing task-events counter under a new label.
- The task file is never written by the gate.
- Once the claim ages past the TTL, the next reconcile pass re-drives the task with no operator action.
- The gate lives inside the single spawn funnel that the Kafka consumer, the deferred-respawn loop and the reconcile pass all pass through, so all three paths get it without caller changes.
- Tests pin every branch, including the "writes nothing" guarantee and the expiry-then-spawn recovery.
</summary>

<objective>
Add a read-only `run_claim` liveness gate to `spawnIfNeededClaimed`, the one funnel every spawn path uses. A task whose frontmatter carries a `run_claim` younger than `DefaultRunClaimTTL` is suppressed as a deferral, so a trading run that has started but not yet published cannot be joined by a second Job for the same idea. The gate must not park, fail, write, or advance the redrive streak; the reconcile floor re-drives the task once the claim expires.
</objective>

<context>
Read `CLAUDE.md` if present, and `README.md`, for project conventions.

Read these files in full before changing anything:

- `pkg/handler/task_event_handler.go`
  - `spawnIfNeededClaimed` is the single spawn funnel. Its current gates, in order, are `skipForeignVaultTask`, `rejectInvalidJobNamespace`, `suppressForActiveCurrentJob`, the per-assignee spawn lock, `isJobActive`, `suppressOrPark`, `redriveStreakCapped`, `deferIfNotAdmitted`, `triggerBudgetRefusesSpawn`, `finalSpawnRefusals`, `markSpawnPending` + `acquireSpawnClaim`, then `spawnClaimedJob`.
  - `spawnOutcome` / `spawnOutcomeSuppressed` / `spawnOutcomeSpawned` / `spawnOutcomeCapped` and `spawned()`.
  - `spawnIfNeeded` (Kafka path) and the reconcile call site both route through `spawnIfNeededClaimed`; `evalDeferredRespawns` in `pkg/handler/task_event_handler_deferred.go` calls it directly. Do not change any caller.
  - The file is currently 1971 lines and the repo's revive `file-length-limit` is 2000. Put all new production code in a new file; `task_event_handler.go` gains only the one call-site replacement.
- `pkg/handler/job_lookup_denied.go`
  - `suppressForActiveCurrentJob(ctx, task, config, claim) (bool, error)` is the existing `current_job` gate. **Leave it byte-for-byte unchanged.**
  - `withinRespawnGraceWindow` shows the sibling log style: `glog.Infof("event=respawn_grace_window task=%s current_job=%s elapsed=%.0fs", ...)` and `metrics.TaskEventsTotal.WithLabelValues("respawn_grace_window").Inc()`.
- `pkg/handler/task_event_handler_deferred.go`
  - `deferredEntry{task lib.Task, ...}` at the top: the deferred replay may carry a snapshot taken at park time. That is expected; a snapshot without `run_claim` simply spawns, which is the pre-spec behaviour. Do not add snapshot refresh work.
- `pkg/metrics/metrics.go`
  - `TaskEventsTotal` is a `*prometheus.CounterVec` with the single label `result`. Increment it with `metrics.TaskEventsTotal.WithLabelValues("skipped_run_claim").Inc()`.
- `github.com/bborbe/agent` v0.141.1, `agent_task-frontmatter.go` (module cache)
  - `func (f TaskFrontmatter) String(key string) (string, bool)` returns `ok == false` for an absent key or a non-string value.
  - `func (f TaskFrontmatter) JobStartedAt() (time.Time, error)` parses a quoted RFC3339 string with `time.Parse(time.RFC3339, v)`.
- Test style references:
  - `pkg/handler/task_event_handler_deferred_test.go` — the `Describe("grace window (spec 036)")` block is the closest model: it builds a handler with `mocks.FakeJobSpawner` / `mocks.FakeConfigResolver` / `mocks.FakeResultPublisher` / `mocks.FakeTaskView`, sets `currentDateTime.SetNow(...)`, drives `h.ConsumeMessage(ctx, buildMsg(task))`, and reads metric deltas with `testutil.ToFloat64(metrics.TaskEventsTotal.WithLabelValues(...))`.
  - `pkg/handler/index_terminal_deferred_test.go` — `deferredGateCaptureStderr(fn func()) string` sets `logtostderr=true` and `v=2`, runs `fn`, flushes glog and returns the captured stderr. It is in `package handler_test`, the same package your new external test file will use; reuse it rather than copying it.
  - `pkg/handler/claim_spawn_order_internal_test.go` — defines `stubJobSpawner` (with `spawned int`) and `stubResultPublisher` in `package handler`, and shows a direct `h.spawnIfNeededClaimed(...)` call.
  - `pkg/handler/claim_job_control_internal_test.go` — defines `staticResolver{config pkg.AgentConfiguration}` in `package handler`.
  - `pkg/handler/claim_wiring_internal_test.go` — defines `noopClaimRefClient` in `package handler`.
  - `pkg/handler/task_event_handler_test.go` — `TestHandler` / `RunSpecs` and `const testTaskVault = "openclaw"` in `package handler_test`.

Coding-plugin guides (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` covers Ginkgo v2/Gomega and Counterfeiter conventions.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md` covers `glog.Infof` and the `event=` key=value style.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-prometheus-metrics-guide.md` covers `CounterVec.WithLabelValues(...).Inc()` and metric test deltas.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-time-injection.md` covers `libtime.CurrentDateTime` and `libtime.NewCurrentDateTime()`.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` covers `github.com/bborbe/errors` (this gate returns no error, so use it only if you introduce one).
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-linting-guide.md` covers the complexity and file-length limits that drive the file split below.

Verified facts (do not re-derive):
- `spawnIfNeededClaimed` is at exactly gocognit's limit of 20 (`.golangci.yml` sets `gocognit.min-complexity: 20`). Adding one more `if` branch to it fails `make precommit`. The new gate is therefore folded behind a single wrapper call at the existing gate's call site, so the funnel gains no branch.
- `DefaultRunClaimTTL` does not exist yet. The spec fixes its value at `2 * time.Hour`, matching `runBudgetSeconds: 7200` in the dev Config.
- `time.Parse(time.RFC3339, raw)` accepts a fractional-second suffix even though the layout has none, so a quoted `2026-11-16T21:03:53.123Z` parses.
- A bare YAML date decodes to a non-string value, so `TaskFrontmatter.String("run_claim")` returns `ok == false` and the gate lets the spawn through. That is the intended "malformed or bare-date" behaviour; do not special-case `time.Time`.
- All three spawn callers discard `spawnOutcomeSuppressed`: the Kafka consumer (`ConsumeMessage`), the deferred loop (clears only an in-memory stamp) and the reconcile tally. A suppressed spawn therefore cannot publish a result, park, or advance `maxConsecutiveRedrives`.
- With `pkg.ClaimModeOff`, `acquireSpawnClaim` returns `true` without touching the lease manager, and `indexGateSuppresses` fails open when `vaultIndexClient` is nil, so a test harness built in claim mode off reaches `SpawnJob` for a task that passes every gate.
- `fakeResultPublisher.Invocations()` returns only methods that were actually called, so `Expect(fakeResultPublisher.Invocations()).To(BeEmpty())` is the exact "nothing was published" assertion.
</context>

<requirements>
1. **Create `pkg/handler/run_claim_gate.go`** (`package handler`, with the BSD license header used by every file in the package). Put the constant and both methods in this file. Use exactly this shape:

   ```go
   // DefaultRunClaimTTL is how long a run_claim frontmatter value is treated as
   // live. It matches the dev run budget (runBudgetSeconds: 7200), so one claim
   // covers the whole window a single trading run may occupy.
   const DefaultRunClaimTTL = 2 * time.Hour

   // suppressForActiveRun answers the two frontmatter liveness gates in order:
   // the existing current_job gate first, then the run_claim gate. It exists
   // because spawnIfNeededClaimed is at gocognit's limit of 20; folding both
   // gates behind one call keeps the funnel's complexity unchanged.
   func (h *taskEventHandler) suppressForActiveRun(
   	ctx context.Context,
   	task lib.Task,
   	config *pkg.AgentConfiguration,
   	claim *deferredClaim,
   ) (bool, error) {
   	suppress, err := h.suppressForActiveCurrentJob(ctx, task, config, claim)
   	if err != nil || suppress {
   		return suppress, err
   	}
   	return h.suppressForLiveRunClaim(task), nil
   }

   // suppressForLiveRunClaim answers the run_claim gate. A task whose frontmatter
   // carries a run_claim younger than DefaultRunClaimTTL is suppressed: the
   // trading run has already started and the executor must not start a second
   // Job for the same idea. A missing, stale or unparseable claim lets the spawn
   // proceed unchanged. The gate is read-only; it writes no frontmatter, parks
   // nothing and publishes nothing, so the reconcile floor re-drives the task
   // once the claim expires.
   func (h *taskEventHandler) suppressForLiveRunClaim(task lib.Task) bool {
   	raw, ok := task.Frontmatter.String("run_claim")
   	if !ok || raw == "" {
   		return false
   	}
   	claimedAt, err := time.Parse(time.RFC3339, raw)
   	if err != nil {
   		return false
   	}
   	age := h.currentDateTime.Now().Time().Sub(claimedAt)
   	if age >= DefaultRunClaimTTL {
   		return false
   	}
   	glog.Infof(
   		"event=spawn_suppressed_run_claim task=%s run_claim=%s age=%.0fs",
   		task.TaskIdentifier,
   		claimedAt.UTC().Format(time.RFC3339),
   		age.Seconds(),
   	)
   	metrics.TaskEventsTotal.WithLabelValues("skipped_run_claim").Inc()
   	return true
   }
   ```

   Notes on the shape:
   - A claim in the future (negative age, from clock skew) counts as live and suppresses; that is the safe direction and is what `now - claim < DefaultRunClaimTTL` means.
   - Do not add a `ctx` parameter to `suppressForLiveRunClaim`; it performs no I/O.
   - Do not add a metric pre-initialisation entry, a new metric family, a config field, an opt-out flag, or a TTL knob. The spec fixes the TTL at one global constant.
   - Required imports: `context`, `time`, `lib "github.com/bborbe/agent"`, `"github.com/golang/glog"`, `pkg "github.com/bborbe/agent-task-executor/pkg"`, `"github.com/bborbe/agent-task-executor/pkg/metrics"`.

2. **Wire the wrapper into `spawnIfNeededClaimed`** in `pkg/handler/task_event_handler.go`. Replace the existing `current_job` gate call and its comment with exactly this call and comment, leaving the surrounding order untouched (after `rejectInvalidJobNamespace`, before the per-assignee spawn lock):

   Old:
   ```go
   	// If current_job is set in frontmatter, a prior spawn notification was written
   	// to the task file. Verify the job is still active; if not, proceed to spawn.
   	if suppress, err := h.suppressForActiveCurrentJob(ctx, task, config, claim); err != nil ||
   		suppress {
   		return spawnOutcomeSuppressed, err
   	}
   ```

   New:
   ```go
   	// The two frontmatter liveness gates, in order: current_job first (a prior
   	// spawn notification was written to the task file), then the trading run's
   	// run_claim (the run has started but not yet published). See run_claim_gate.go.
   	if suppress, err := h.suppressForActiveRun(ctx, task, config, claim); err != nil || suppress {
   		return spawnOutcomeSuppressed, err
   	}
   ```

   No other line of `task_event_handler.go` changes. Do not add a second `if` for the new gate; the gocognit limit makes that fail `make precommit`. Do not change `suppressForActiveCurrentJob` or any other gate. The wrapper preserves the ordering the spec requires: `current_job` runs first, and when it suppresses the `run_claim` event is never emitted.

3. **External decision-table specs.** Create `pkg/handler/run_claim_gate_test.go` (`package handler_test`) with a top-level `var _ = Describe("run_claim spawn gate", func() { ... })`.

   Harness, built in `BeforeEach`:
   - `fakeSpawner := new(mocks.FakeJobSpawner)` with `IsJobActiveReturns(false, nil)`, `ListActiveJobsReturns(map[lib.TaskIdentifier]bool{}, nil)`, `CountActiveJobsReturns(0, nil)` and `SpawnJobReturns("job-run-claim-1", nil)`.
   - `fakeResolver := new(mocks.FakeConfigResolver)` with `ResolveReturns(pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"}, nil)`.
   - `fakePublisher := new(mocks.FakeResultPublisher)`.
   - `fakeView := new(mocks.FakeTaskView)` with `ReadyReturns(true)`, `SnapshotReturns(nil)` and `GetReturns(lib.Task{}, false)`.
   - `taskStore := pkg.NewTaskStore()`, the real store.
   - `clock := libtime.NewCurrentDateTime()`, declared as `libtime.CurrentDateTime` so specs can call `clock.SetNow(...)`.
   - The handler is `handler.NewTaskEventHandler(fakeSpawner, "dev", base.Branch("prod"), "", true, true, fakeResolver, fakePublisher, taskStore, clock, fakeView, testTaskVault, handler.DefaultReconcilePassTimeout, handler.DefaultReconcileInterval, pkg.ClaimModeOff, nil)`, stored as `handler.TaskEventHandler`.

   Helpers in the `Describe`:
   - `buildTask := func(extra lib.TaskFrontmatter) lib.Task` starts from `lib.TaskFrontmatter{"status": "in_progress", "phase": string(domain.TaskPhaseExecution), "assignee": "claude"}`, copies every key from `extra`, and returns `lib.Task{TaskIdentifier: "tid-run-claim", Vault: testTaskVault, Frontmatter: fm}`.
   - `buildMsg := func(task lib.Task) *sarama.ConsumerMessage` marshals the task to JSON exactly as `task_event_handler_deferred_test.go` does.
   - `runClaimMetric := func() float64 { return testutil.ToFloat64(metrics.TaskEventsTotal.WithLabelValues("skipped_run_claim")) }`.
   - `activeJobMetric := func() float64 { return testutil.ToFloat64(metrics.TaskEventsTotal.WithLabelValues("skipped_active_job")) }`.

   Every spec drives `h.ConsumeMessage(ctx, buildMsg(task))`, which must return nil. Wrap any spec that asserts on the log in `deferredGateCaptureStderr(func() { ... })` (defined in `pkg/handler/index_terminal_deferred_test.go`, same package).

   Specs:
   - **Live claim suppresses.** `clock.SetNow(libtimetest.ParseDateTime("2026-05-16T20:19:16Z"))`; task with `run_claim: "2026-05-16T20:19:16Z"`. Record `before := runClaimMetric()`. Run inside the capture helper. Assert:
     - `fakeSpawner.SpawnJobCallCount() == 0`;
     - `fakePublisher.Invocations()` is empty (no frontmatter command, no park, no spawn notification);
     - `runClaimMetric() - before == 1`;
     - the captured output contains `event=spawn_suppressed_run_claim task=tid-run-claim run_claim=2026-05-16T20:19:16Z age=0s`.
   - **Missing claim spawns.** No `run_claim` key. Assert `SpawnJobCallCount() == 1` and the `skipped_run_claim` delta is 0.
   - **Claim at the TTL spawns.** `clock.SetNow(libtimetest.ParseDateTime("2026-05-16T22:19:16Z"))` and task `run_claim: "2026-05-16T20:19:16Z"` (age is exactly `handler.DefaultRunClaimTTL`). Assert `SpawnJobCallCount() == 1` and the `skipped_run_claim` delta is 0.
   - **Malformed claim spawns.** Task `run_claim: "not-a-timestamp"`. Assert `SpawnJobCallCount() == 1` and the `skipped_run_claim` delta is 0.
   - **`current_job` wins when both are set.** `fakeSpawner.IsJobActiveReturns(true, nil)`; task with `current_job: "pod-A"` and a live `run_claim`. Record both metric baselines. Run inside the capture helper. Assert `SpawnJobCallCount() == 0`, `runClaimMetric()` delta 0, `activeJobMetric()` delta 1, and the captured output does not contain `spawn_suppressed_run_claim`.
   - **Re-driven once the claim expires.** `clock.SetNow(libtimetest.ParseDateTime("2026-05-16T20:19:16Z"))`; task with a live `run_claim`. First `ConsumeMessage` must leave `SpawnJobCallCount() == 0`. Then `clock.SetNow(libtimetest.ParseDateTime("2026-05-16T22:19:16Z"))` and send the same task again; assert `SpawnJobCallCount() == 1` and the total `skipped_run_claim` delta across both passes is 1.

4. **Internal suppressed-outcome spec.** Create `pkg/handler/run_claim_gate_internal_test.go` (`package handler`) with `func TestLiveRunClaimReturnsSuppressedOutcome(t *testing.T)`. This is a plain Go test, not Ginkgo, so it can call the unexported funnel directly. Build the handler with `NewTaskEventHandler(spawner, "dev", "prod", "self", true, true, staticResolver{config: pkg.AgentConfiguration{Assignee: "claude", Image: "img"}}, stubResultPublisher{}, pkg.NewTaskStore(), libtime.NewCurrentDateTime(), nil, "v", DefaultReconcilePassTimeout, DefaultReconcileInterval, pkg.ClaimModeOff, func() claim.ClaimRefClient { return noopClaimRefClient{} })`, type-assert it to `*taskEventHandler`, and use a task with `Vault: "v"`, `status: in_progress`, `phase: execution`, `assignee: claude` and `run_claim: "2999-01-01T00:00:00Z"`. Call `h.spawnIfNeededClaimed(ctx, task, &pkg.AgentConfiguration{Assignee: "claude", Image: "img"}, nil)` and assert the returned outcome is `spawnOutcomeSuppressed` and `spawner.spawned == 0`.

   Reuse the internal fakes already defined in sibling `package handler` test files: `stubJobSpawner` and `stubResultPublisher` from `claim_spawn_order_internal_test.go`, `staticResolver` from `claim_job_control_internal_test.go`, `noopClaimRefClient` from `claim_wiring_internal_test.go`. Do not redefine them.

5. **Run the whole `pkg/handler` suite.** The gate is read-only and sits before the assignee lock, so it should add no remote reads and no publisher calls on any existing fixture that has no `run_claim`. If a pre-existing spec nevertheless fails because of an extra call, fix that spec's fixture rather than weakening the gate or reordering checks.

6. **Self-check before finishing.** Re-run every command in `<verification>`. Walk the spec's Acceptance Criteria 1-9 and confirm each is asserted by a spec from requirement 3 or 4. Do not attempt the Post-Deploy (Rung-2) criterion; it is operator-executed after merge.
</requirements>

<constraints>
- No change to `park_succeeded.go`, `recent_success.go`, or the redrive budget.
- No change to the `current_job` gate's behaviour or ordering. `suppressForActiveCurrentJob` stays byte-for-byte unchanged; it is still called first.
- The new gate is read-only: it must not write frontmatter on either path.
- Factory composition stays pure; the gate is a method on `taskEventHandler`, not a new dependency, field, option, or constructor argument.
- `run_claim` is a new key name shared with the companion trading change; it must not be renamed on one side only.
- No change to the claim store, the dispatch lease, `claim_mode`, `maxConsecutiveRedrives`, `maxConcurrentJobs`, the run window, or `park_succeeded` behaviour.
- No new flags, env vars, config fields, tunable thresholds, or metric families. The only metric change is `TaskEventsTotal.WithLabelValues("skipped_run_claim").Inc()`.
- Do not add a second branch to `spawnIfNeededClaimed`; it is at gocognit's limit of 20. Use the `suppressForActiveRun` wrapper.
- Do not add more than one call-site edit to `pkg/handler/task_event_handler.go`; it is at 1971 of 2000 lines (revive `file-length-limit`). All new production code goes in `pkg/handler/run_claim_gate.go`.
- **Accepted cost (do not "fix" it):** a run that *fails* leaves `run_claim` set, so its redrive is suppressed until the TTL — up to 2 h for a run that died in 30 s. This is the trade the gate makes; it is not a defect to be worked around in implementation.
- The deferred-respawn replay may carry a pre-write snapshot with no `run_claim`; the gate then spawns, which is the pre-spec behaviour. Do not add snapshot refresh work.
- Wrap errors with `github.com/bborbe/errors` (`errors.Wrapf(ctx, err, ...)`), never `fmt.Errorf`, if you introduce an error path.
- Write tests with Ginkgo v2/Gomega for the external suite; use Counterfeiter fakes for interfaces in `package handler_test`. The task store is always the real `*pkg.TaskStore`.
- Do NOT commit. dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `make test`. It must pass, including the new external and internal specs.

Run `make precommit`. It must pass. In particular, `gocognit`, `funlen`, `nestif` and revive's `file-length-limit` must stay clean.

Run `grep -q 'event=spawn_suppressed_run_claim' pkg/handler/run_claim_gate.go && echo GATE_LOG_OK`. It must print `GATE_LOG_OK`.

Run `test "$(grep -c 'suppressForActiveRun' pkg/handler/task_event_handler.go)" -eq 1 && echo WIRED_OK`. It must print `WIRED_OK`.

Run `go test ./pkg/handler/ -v -args -ginkgo.focus='run_claim spawn gate' > /tmp/runclaim.out 2>&1; grep -q 'SUCCESS!' /tmp/runclaim.out && echo FOCUS_OK`. It must print `FOCUS_OK`. The log line itself is asserted inside the spec via the captured stderr; glog output is redirected to the capture pipe during that spec, so do not expect it in the `go test` output.
</verification>
