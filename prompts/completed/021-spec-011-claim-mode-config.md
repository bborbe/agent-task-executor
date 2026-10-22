---
status: completed
spec: [011-claim-mode-dispatch-wiring]
summary: Added the CLAIM_MODE contract (off|shadow|on, default off), a lazily-constructed injected claim ref client seam, and the claim-mode harness, with all call sites updated and the full suite passing.
execution_id: agent-task-executor-claim-wiring-exec-021-spec-011-claim-mode-config
dark-factory-version: v0.198.7
created: "2026-10-21T23:30:14Z"
queued: "2026-10-22T00:03:40Z"
started: "2026-10-22T00:03:41Z"
completed: "2026-10-22T00:09:28Z"
branch: dark-factory/claim-mode-dispatch-wiring
---

# Add CLAIM_MODE configuration and claim client injection

<summary>
- The executor gains a `CLAIM_MODE` setting with three values: `off`, `shadow`, `on`; the default is `off`.
- An unset variable means `off`, but an explicitly empty variable is rejected, as is any value outside the three.
- An invalid value stops the process at startup and names both the rejected value and the accepted set.
- The claim client is built through an injected constructor, never at package init or startup, and never when the mode is `off`.
- One claim lease manager is created lazily per process, on first use, so an `off` executor never builds a claim client.
- Nothing about dispatch changes yet: this prompt lands an inert change that later prompts build on.
- All existing constructor call sites are updated so the project still compiles and the full suite still passes.
- The 13-series claim counter set is untouched.
</summary>

<objective>
Land the `CLAIM_MODE` contract and the injected claim-client seam so a later prompt can wire the lease gate into dispatch, while an `off` executor remains byte-for-byte identical to today and never constructs a claim client.
</objective>

<context>
Read `CLAUDE.md` for project conventions, then read these files before changing anything:

- `main.go` — the `application` struct, the startup validation block at the top of `application.Run`, and the `factory.CreateConsumer` call site. Note the existing `GitRestURL` and `GitRestGatewaySecret` fields and how `ReadGitRestGatewaySecret` is called.
- `pkg/claim/lease_manager.go` — `NewLeaseManager`, `LeaseManager`, `HeldClaim`, `TakeoverResult`.
- `pkg/claim/ref_client.go` — `ClaimRefClient`, `NewClaimRefClient`, `RequestTimeout`.
- `pkg/claim/key.go` — `DeriveClaimKey`.
- `pkg/handler/task_event_handler.go` — `TaskEventHandler`, `NewTaskEventHandler`, the `taskEventHandler` struct, and `spawnIfNeededClaimed`.
- `pkg/factory/factory.go` — `CreateGitRestClient` and `CreateConsumer`.
- `pkg/metrics/metrics.go` — `ClaimTotal` and its pre-initialised 13 result labels.
- `pkg/zombie_reason.go` and `pkg/task_type_filter.go` — the project's style for a closed string type in package `pkg`.
- `pkg/job_watcher_internal_test.go` — a `package pkg` internal test file. **Caution, measured 2026-10-22: its Ginkgo specs do not run** (`go test ./pkg` reports 0 of them), because `pkg/suite_test.go` is `package pkg_test` and the registry does not reach across. Do not copy this file's style for behaviour that must actually be tested.
- `main_internal_test.go` — the `package main` startup-validation specs. **Its comment claiming the specs run is wrong**: `go test .` reports 0 of them. Use plain `testing` with `t.Run` in any internal test file you add.

Project docs: `docs/claim-lease-protocol.md`.

Coding docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-enum-type-pattern.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-factory-pattern.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-prometheus-metrics-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md`
</context>

<requirements>
1. Create `pkg/claim_mode.go` (package `pkg`) with exactly this public contract:

```go
// ClaimMode is the closed set of dispatch claim modes.
type ClaimMode string

const (
	ClaimModeOff    ClaimMode = "off"
	ClaimModeShadow ClaimMode = "shadow"
	ClaimModeOn     ClaimMode = "on"
)

// ErrClaimModeInvalid is returned by ParseClaimMode for an unrecognised or
// explicitly empty value.
var ErrClaimModeInvalid = stderrors.New("claim mode invalid")

// ParseClaimMode parses the CLAIM_MODE value. present reports whether the
// variable exists in the environment, which distinguishes an unset variable
// (off) from an explicitly empty one (invalid).
func ParseClaimMode(ctx context.Context, value string, present bool) (ClaimMode, error)
```

   Behaviour:
   - `present == false` returns `ClaimModeOff, nil` regardless of `value`.
   - `present == true` and `value` is exactly one of `off`, `shadow`, `on` returns that mode.
   - Every other case, including `value == ""`, returns an error that wraps `ErrClaimModeInvalid` and whose message contains the offending value quoted (`%q`) and the literal accepted set `off|shadow|on`.
   - Do NOT lowercase or otherwise normalise the input: `ON` must fail.
   - Use `stderrors "errors"` for the sentinel and `github.com/bborbe/errors` for the wrap, matching `pkg/claim/lease_manager.go`.

2. Create `pkg/claim_mode_test.go` (package `pkg_test`, Ginkgo + Gomega, discovered by the existing `pkg/suite_test.go`) with a table test:
   - absent (`present=false`, `value=""`) → `ClaimModeOff`, no error
   - `off`, `shadow`, `on` (`present=true`) → the same mode, no error
   - present with `""`, `ON`, `shadoww`, `true` → error, `errors.Is(err, pkg.ErrClaimModeInvalid)` is true, and the message contains the rejected value and `off|shadow|on`
   Assert the exact mode value and the error identity (stdlib `errors.Is`), not just `HaveOccurred()`.

3. In `main.go`, add a package-level constant next to `ErrTaskGlobEmpty`:

```go
// claimModeEnv is the environment variable carrying the executor's claim mode.
const claimModeEnv = "CLAIM_MODE"
```

   Add a startup helper and call it as the first validation in `application.Run`, before the `JobTTLSecondsAfterFinished` check and before any Kubernetes client construction:

```go
// parseClaimModeFromEnv reads CLAIM_MODE, distinguishing an absent variable
// (off) from an explicitly empty one (invalid).
func parseClaimModeFromEnv(ctx context.Context) (pkg.ClaimMode, error) {
	value, present := os.LookupEnv(claimModeEnv)
	return pkg.ParseClaimMode(ctx, value, present)
}
```

   On error, return `errors.Wrapf(ctx, err, "parse claim mode")` from `Run`. Store the parsed mode in a local variable.

4. Add specs to `main_internal_test.go` (package `main`, no new `RunSpecs`):
   - unset variable → `pkg.ClaimModeOff`
   - empty variable → error whose message contains `off|shadow|on` (use `os.Setenv(claimModeEnv, "")` and restore with `DeferCleanup`)
   - `ON` → error whose message contains `ON`
   Use `os.LookupEnv`/`os.Unsetenv` so the unset case really is unset.

5. Create `pkg/handler/claim_wiring.go` (package `handler`) containing:

```go
// ClaimRefClientFactory builds the git-rest claim ref client. It is called at
// most once per process, and only when CLAIM_MODE is shadow or on.
type ClaimRefClientFactory func() claim.ClaimRefClient

// unmanagedJobControl is the JobControl handed to the dispatch-only lease
// manager. Claim and Takeover never call it; Recover and the fence do, and the
// lease-lifecycle spec owns those paths. It fails loud so a future wiring that
// starts them cannot silently run without Job control.
type unmanagedJobControl struct{}
```

   Imports in `claim_wiring.go`: `context`, `github.com/bborbe/errors`, `libtime "github.com/bborbe/time"`, `github.com/bborbe/agent-task-executor/pkg/claim`, and `pkg "github.com/bborbe/agent-task-executor/pkg"`.

   `unmanagedJobControl` implements `claim.JobControl`:
   - `IsJobRunning(ctx context.Context, taskID string) (bool, error)` returns `false` and an `errors.Errorf(ctx, "claim job control not wired: IsJobRunning(%s)", taskID)`
   - `DeleteJob(ctx context.Context, taskID string) error` returns `errors.Errorf(ctx, "claim job control not wired: DeleteJob(%s)", taskID)`

   Add the lazy accessor:

```go
// leaseManager returns this process's single lease manager, constructing it on
// first use. It refuses to construct anything in claim mode off.
func (h *taskEventHandler) leaseManager(ctx context.Context) (claim.LeaseManager, error)
```

   Behaviour:
   - If `h.claimMode == pkg.ClaimModeOff`, return an error naming mode `off`; the factory must not be called.
   - Guard construction with a `sync.Once` stored on the handler, and cache both the manager and the construction error so repeated calls never rebuild or re-log.
   - If `h.claimRefClientFactory == nil`, return `errors.Errorf(ctx, ...)` naming the mode.
   - Otherwise call the factory exactly once and construct with:
     `claim.NewLeaseManager(h.cluster, refClient, unmanagedJobControl{}, h.currentDateTime, libtime.NewWaiterDuration())`.
   - Wrap a constructor error with `errors.Wrapf(ctx, err, "create claim lease manager")`. An empty `h.cluster` makes `NewLeaseManager` fail; propagate that error.

6. Extend the `taskEventHandler` struct in `pkg/handler/task_event_handler.go` with:

```go
claimMode             pkg.ClaimMode
claimRefClientFactory ClaimRefClientFactory
claimManagerOnce      sync.Once
claimManager          claim.LeaseManager
claimManagerErr       error
```

   `sync` is already imported there; add `github.com/bborbe/agent-task-executor/pkg/claim` to that file for the `claim.LeaseManager` field.

7. Append two parameters to `NewTaskEventHandler` and assign them:

```go
claimMode pkg.ClaimMode,
claimRefClientFactory ClaimRefClientFactory,
```

   Append them after `reconcileInterval` so existing positional arguments keep their meaning. Do not add functional options.

8. In `pkg/factory/factory.go`:
   - Add `CreateClaimRefClientFactory(baseURL, gatewaySecret string) handler.ClaimRefClientFactory` returning a closure that calls `claim.NewClaimRefClient(baseURL, gatewaySecret, "agent-task-executor")`. The initiator string is the same identity `CreateGitRestClient` uses.
   - Append `claimMode pkg.ClaimMode` and `claimRefClientFactory handler.ClaimRefClientFactory` to `CreateConsumer` and pass them through to `NewTaskEventHandler`.
   - Import `github.com/bborbe/agent-task-executor/pkg/claim`.

9. In `main.go`, after the git-rest gateway secret is read and before `factory.CreateConsumer` is called:
   `claimRefClientFactory := factory.CreateClaimRefClientFactory(a.GitRestURL, gitRestGatewaySecretValue)`.
   Pass the parsed `claimMode` and `claimRefClientFactory` to `CreateConsumer`.

10. Update every existing call site of `NewTaskEventHandler` (currently `pkg/handler/task_event_handler_test.go`, `pkg/handler/task_event_handler_deferred_test.go`, `pkg/handler/task_event_handler_respawn_race_test.go`, `pkg/handler/task_reconcile_test.go`) to pass `pkg.ClaimModeOff, nil` as the final two arguments. Do not change what those tests assert.

11. Create `pkg/handler/claim_wiring_internal_test.go` (package `handler`, **plain `testing` with `t.Run` — not Ginkgo**, because Ginkgo specs in an internal test package of this repo do not run; the file must not import `mocks`):
    - mode `off`: `leaseManager(ctx)` returns an error mentioning `off`; a factory that panics is never invoked.
    - mode `shadow` with a factory returning `nil`: the first call returns a non-nil manager and no error; the second call returns the same manager and the factory counter stays at 1.
    - mode `shadow` with an empty `cluster`: the call returns an error containing `cluster`.
    - `unmanagedJobControl` methods return errors containing `not wired`.
    Construct the handler as `&taskEventHandler{...}` with only the fields these methods read; do not call `NewTaskEventHandler`.

    Decision, pinned — do not add a startup check for `CLUSTER`: `pkg/claim.NewLeaseManager` already rejects an empty cluster, and this prompt propagates that error rather than duplicating the validation. An `on` executor with an empty `CLUSTER` therefore fails closed and dispatches nothing, which is the safe direction and is visible as `error` on the claim counters.

12. Create `pkg/handler/task_event_handler_claim_test.go` (package `handler_test`) with a reusable harness used by prompts 2 and 3:
    - `newClaimHarness(mode pkg.ClaimMode) *claimHarness` builds a real handler through `handler.NewTaskEventHandler` with `mocks.FakeJobSpawner`, `mocks.FakeConfigResolver`, `mocks.FakeResultPublisher`, `mocks.FakeGitRestClient`, a `pkg.TaskStore`, `libtime.NewCurrentDateTime()`, branch `prod`, cluster `self`, `stageFilter=true`, `runUntagged=true`, task glob `24 Tasks/*.md`, `handler.DefaultReconcilePassTimeout`, `handler.DefaultReconcileInterval`, the given mode, and a recording factory that returns a `*mocks.FakeClaimRefClient` and counts its calls.
    - The fake spawner must default to `SpawnJobReturns("job-1", nil)`, `IsJobActiveReturns(false, nil)`, `ListActiveJobsReturns(map[lib.TaskIdentifier]bool{}, nil)`, `CountActiveJobsReturns(0, nil)`.
    - The fake resolver must default to `ResolveReturns(pkg.AgentConfiguration{Assignee: "claude", Image: "img"}, nil)`.
    - Expose the client, the factory-call counter, and a `consume(task lib.Task)` helper that marshals the task to a `*sarama.ConsumerMessage` and calls `ConsumeMessage`.
    - Add local helpers `claimResultLabels` (the 13 labels), `snapshotClaims()`, and `expectClaimDelta(before, after, expected)` using `prometheus/testutil` and `metrics.ClaimTotal`, mirroring `pkg/claim/lease_manager_test_helpers_test.go` (that file is `package claim_test` and is not importable).
    - Add the AC2 spec: with mode `off`, consuming a qualifying task creates exactly one Job, the factory counter is 0, `PutCallCount() == 0`, `DeleteCallCount() == 0`, `ListCallCount() == 0`, and every one of the 13 series has delta 0. A qualifying task is: status `in_progress`, phase `string(domain.TaskPhaseExecution)`, assignee `claude`, stage `prod`, no `cluster` field, no `ref`, no `max_triggers`.

13. Add a `CLAIM_MODE` row to the env-var table in `README.md` (the table whose rows are `GITREST_URL`, `GITREST_GATEWAY_SECRET`, `TASK_GLOB`), appended after `TASK_GLOB`, with Default `off` and Meaning: `Claim dispatch mode: off (no claim client constructed, today's behaviour), shadow (claims, never blocks), on (a held claim is a precondition of the Job). Any other value stops the process at startup.`

14. Add a `CHANGELOG.md` entry under `## Unreleased` (create the section immediately below the title if absent):
    `- feat: add CLAIM_MODE (off|shadow|on, default off) and an injected claim ref client constructor; off constructs no claim client`

15. Before finishing, walk every acceptance criterion above against the change and re-run `<verification>`.
</requirements>

<constraints>
- `pkg/claim` is called, never modified. Its constants, fence arithmetic and tests are frozen by spec 009.
- `off` must not construct the ref client at all — not at startup, not on first dispatch. A misconfigured git-rest URL must not be able to fail an `off` executor.
- One `LeaseManager` per process, constructed once, with the executor's `CLUSTER` value.
- The ref client's base URL and gateway headers reuse the existing `GITREST_URL` and git-rest gateway secret; do not add an endpoint, a secret, or a new configuration field for them.
- No new metric. The closed 13-series `executor_claim_total` set already exists and is pre-initialised.
- No claim release, restart recovery, lease `Run` loop, mismatch audit, or `spec.rerunOnLostClaim` wiring — `specs/claim-lease-lifecycle-wiring.md` owns those.
- Do NOT change dispatch behaviour in this prompt: no claim request is made anywhere yet.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
```
ROOTDIR=/workspace make precommit
go test -race -count=1 ./...
grep -rn 'CLAIM_MODE' main.go pkg/
grep -rn 'NewTaskEventHandler(' --include='*.go' .
go test -race -count=1 -run 'Test' -v ./pkg/handler | grep -q -- '--- PASS'
```
- `make precommit` exits 0.
- `go test -race -count=1 ./...` exits 0.
- The internal test you added actually runs: the last command must find a `--- PASS` line, because a Ginkgo file in an internal test package of this repo is silently skipped (measured 2026-10-22).
- The `CLAIM_MODE` grep returns the `claimModeEnv` constant declaration and the `os.LookupEnv` read site in `main.go`.
- The `NewTaskEventHandler(` grep returns the definition and every call site; no call site still passes the old argument count.
- `grep -n 'claim.NewClaimRefClient' pkg/factory/factory.go` returns exactly one production call site (inside the factory closure).
</verification>
