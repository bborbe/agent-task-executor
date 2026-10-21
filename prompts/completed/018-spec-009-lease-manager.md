---
status: completed
spec: [009-claim-lease-client-library]
summary: Implemented the claim lease state machine (claim/release/takeover/tick/fence/renew/restart recovery/loop) with 13-result metrics and deterministic Ginkgo tests on the fake clock and in-process fake git-rest.
execution_id: agent-task-executor-claim-client-exec-018-spec-009-lease-manager
dark-factory-version: v0.198.7
created: "2026-10-21T18:52:26Z"
queued: "2026-10-21T19:14:22Z"
started: "2026-10-21T19:23:03Z"
completed: "2026-10-21T19:36:37Z"
branch: dark-factory/claim-lease-client-library
---

<summary>
- Adds the lease manager: it claims a task's key on git-rest, renews the claim every 60 seconds by compare-and-swap, and deletes the task's Job when the claim is lost.
- A holder that cannot renew stops its own Job no later than 60 seconds before its lease ends; the fence is evaluated before any network call and again after every request.
- A second executor may take over an expired claim only 30 seconds after the lease ends, with one compare-and-swap write, so exactly one taker wins.
- A restarted executor re-adopts the claims whose Job still runs, releases the finished ones, and fences the ones too close to their lease end.
- A create or renew whose response was lost is read back: if the ref carries this writer's exact (cluster, task id, lease_until) triple, the claim is recovered at the listed sha; otherwise it is lost.
- When deleting a Job fails on the lost or fence path, the claim stays in a pending-deletion state, is never renewed, and the deletion is retried every tick until it succeeds.
- Every outcome is counted in `executor_claim_total{result}`; all 13 series are exercised by the tests.
- The loop wrapper waits 10 seconds through an injected `WaiterDuration`, starts no goroutine, and stops on context cancellation.
- The manager's state is safe for concurrent use by the tick loop and claim/release/takeover callers under `go test -race`.
- Nothing in the live dispatch path calls it yet.
</summary>

<objective>
Build the lease state machine on top of the prompt-1 ref client: claim, release, takeover, tick/renew/fence, pending Job deletion, restart recovery and the tick loop, all driven by an injected clock and an injected Job control, with deterministic Ginkgo tests on the in-process fake git-rest (AC1–AC6, AC10).
</objective>

<context>
Read the spec `/workspace/specs/in-progress/009-claim-lease-client-library.md` fully, especially Desired Behavior 2–7 and AC1–AC6 and AC10. This prompt implements them; do not invent lease rules beyond the spec.

Read these files fully before changing anything:
- `/workspace/prompts/1-spec-009-ref-client.md` — the prompt that produced the ref client and the fake git-rest. The fake API in `pkg/claim/fake_git_rest_test.go` (`newFakeGitRest`, `Seed`, `Ref`, `DeleteRef`, `Requests`, `ResetRequests`, `SetFault`, `SetPutBarrier`, `fakeRequest`, `fakeFault`, `fakeRef`) is load-bearing for this prompt.
- `/workspace/pkg/claim/ref_client.go` — `ClaimRefClient`, `ClaimRef`, `ErrLeaseConflict`, `RequestTimeout`, `formatLeaseUntil`.
- `/workspace/pkg/claim/fake_git_rest_test.go` and `/workspace/pkg/claim/ref_client_test.go` — the fake and the test style.
- `/workspace/pkg/metrics/metrics.go` — the `promauto.NewCounterVec` pattern and `init()` pre-initialisation style.
- `/workspace/pkg/spawner/job_spawner.go` — the `IsJobActive` concept; the claim package defines its own small `JobControl` interface (do NOT import the spawner).
- `/workspace/pkg/zombie_sweeper_test.go` (first ~50 lines) and `/workspace/pkg/result_publisher_test.go` (first ~50 lines) — the `libtime.NewCurrentDateTime()` + `SetNow` fake-clock pattern.
- `/workspace/pkg/spawner/spawner_suite_test.go` — the suite + go:generate pattern.

Verified external APIs (read from the module source; do not paraphrase):
- `libtime "github.com/bborbe/time"` → `type CurrentDateTimeGetter interface { Now() DateTime }`; `type DateTime stdtime.Time` with `Time() stdtime.Time`, `Add(HasDuration) DateTime`, `Sub(HasTime) Duration`, `After(HasTime) bool`, `Before(HasTime) bool`, `Equal(DateTime) bool`, `IsZero() bool`; `type Duration stdtime.Duration` with `Duration() stdtime.Duration`; constants `Second`, `Minute`; `type WaiterDuration interface { Wait(ctx context.Context, duration Duration) error }`.
- `libtimetest "github.com/bborbe/time/test"` → `ParseDateTime(value interface{}) libtime.DateTime` (tests only).
- `"github.com/bborbe/errors"` → `Errorf`, `Wrapf`, `Is`.
- `stderrors "errors"` for sentinels.
- `"github.com/prometheus/client_golang/prometheus"` and `promauto`.
- `"github.com/prometheus/client_golang/prometheus/testutil"` → `ToFloat64(c prometheus.Collector) float64` (tests only).

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-time-injection.md` — injected getter, `SetNow`, never `time.Now` in production.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-context-cancellation-in-loops.md` — non-blocking context checks in loops.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — wrapping and sentinels.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-prometheus-metrics-guide.md` — counter naming and pre-initialisation.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega, external test package.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-precommit.md` — funlen 80 lines / 50 statements, nestif 4, gocognit 20, golines 100.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules.
</context>

<requirements>
1. Create `/workspace/pkg/claim/job_control.go` (license header) with exactly this surface:

```go
package claim

//counterfeiter:generate -o ../../mocks/claim_job_control.go --fake-name FakeClaimJobControl . JobControl

// JobControl is the small slice of Kubernetes Job control the lease manager
// needs. It is injected so the lease logic can be tested on a fake clock.
type JobControl interface {
	// IsJobRunning reports whether a non-terminal Job exists for taskID.
	IsJobRunning(ctx context.Context, taskID string) (bool, error)
	// DeleteJob deletes the Job for taskID. Deleting an absent Job is not an
	// error (idempotent).
	DeleteJob(ctx context.Context, taskID string) error
}
```

Regenerate mocks with `go generate -mod=mod ./pkg/claim/...` so `/workspace/mocks/claim_job_control.go` contains `FakeClaimJobControl`. Do not hand-write it.

2. Create `/workspace/pkg/claim/lease_manager.go` (license header) with the constants, sentinel, exported types, interface and constructor below. Add unexported state and helpers in this file and `lease_manager_tick.go` (requirement 3).

```go
package claim

const (
	LeaseDuration = 300 * libtime.Second
	TickInterval  = 10 * libtime.Second
	RenewInterval = 60 * libtime.Second
	FenceMargin   = 60 * libtime.Second
	FenceLead     = 10 * libtime.Second
	TakeoverGrace = 30 * libtime.Second
)

// ErrLeaseNotExpired is returned by Takeover when the listed claim is still
// inside its lease or its lease_until cannot be parsed as RFC3339.
var ErrLeaseNotExpired = stderrors.New("lease not expired")

// HeldClaim is the manager's view of a claim it holds.
type HeldClaim struct {
	Key        string
	TaskID     string
	Sha        string
	LeaseUntil libtime.DateTime
}

// TakeoverResult is returned by Takeover. PreviousTaskID is the task id of the
// expired claim that was replaced, or "" when the key was absent and a create
// was used instead.
type TakeoverResult struct {
	Key            string
	TaskID         string
	Sha            string
	LeaseUntil     libtime.DateTime
	PreviousTaskID string
}

// LeaseManager owns this executor's claims for one cluster name.
type LeaseManager interface {
	Claim(ctx context.Context, key string, taskID string) (*HeldClaim, error)
	Release(ctx context.Context, key string) error
	Takeover(ctx context.Context, key string, taskID string) (*TakeoverResult, error)
	Tick(ctx context.Context) error
	Run(ctx context.Context) error
	Recover(ctx context.Context) error
	// HeldClaims returns a snapshot sorted by Key. Claims in pending Job
	// deletion or pending release are included: they are still held until the
	// pending action completes or the fence drops them.
	HeldClaims() []HeldClaim
}

// NewLeaseManager constructs a LeaseManager. An empty cluster is an error.
// waiter is used only by Run; tests pass libtime.NewWaiterDuration() or the
// counterfeiter fake.
func NewLeaseManager(
	cluster string,
	refClient ClaimRefClient,
	jobControl JobControl,
	currentDateTime libtime.CurrentDateTimeGetter,
	waiter libtime.WaiterDuration,
) (LeaseManager, error)
```

Do NOT add configuration knobs for any constant. Do NOT add an opt-out flag. The constants are invariants from the spec.

3. Implement the state machine with these exact rules.

**Shared helpers.**
- `fenceAt(leaseUntil libtime.DateTime) libtime.DateTime` = `leaseUntil.Add(-FenceMargin)`.
- `fenceDue(now, leaseUntil libtime.DateTime) bool` = `now.After(fenceAt(leaseUntil).Add(-FenceLead))` (i.e. `now > lease_until − 70 s`).
- `renewDue(now, lastRenewAt libtime.DateTime) bool` = `now.Sub(lastRenewAt) >= RenewInterval`.
- `readBack(ctx, key string) (*ClaimRef, error)`: call `refClient.List(ctx, key)` and return the entry whose `Ref` equals `"refs/"+key` exactly; return `(nil, nil)` when no entry matches. A sibling under `refs/<key>/x` must never match.
- `ownTriple(ref *ClaimRef, taskID, leaseUntilStr string) bool`: `ref != nil && ref.Cluster == m.cluster && ref.TaskID == taskID && ref.LeaseUntil == leaseUntilStr`.
- Internal state per held claim: `key`, `taskID`, `sha`, `leaseUntil`, `leaseUntilStr`, `lastRenewAt`, `renewNextTick bool`, `pendingJobDeletion bool`, `pendingDeletionPath string` (`"lost"` or `"fence"`), `releasePending bool`, `needsRunningCheck bool`.
- All public methods take one `sync.Mutex` for their whole duration. Unexported helpers assume the lock is held. `HeldClaims` returns copies.

**Claim (Desired Behavior 2).**
1. If the key is already held by this manager, return `nil, ErrLeaseConflict` without a network call.
2. `now := currentDateTime.Now()`; `leaseUntil := now.Add(LeaseDuration)`; `leaseUntilStr := formatLeaseUntil(leaseUntil)`.
3. `sha, err := refClient.Put(ctx, key, "", cluster, taskID, leaseUntil)`.
4. `err == nil` → store the claim (`lastRenewAt = now`, `renewNextTick = false`), count `claimed`, return `&HeldClaim{...}`.
5. `errors.Is(err, ErrLeaseConflict)` → count `conflict`, return `nil, err`.
6. Any other error → read back `List(ctx, key)`:
   - read-back error → count `error`, return `nil,` the read-back error wrapped with the original.
   - `ownTriple(ref, taskID, leaseUntilStr)` → store the claim at `ref.Sha`, count `recovered`, return success.
   - otherwise (absent, sibling only, or any other write) → count `error`, return `nil,` the original error.
   The create never sends a `DELETE`.

**Release (Desired Behavior 2).**
1. Not held → return `nil`.
2. `refClient.Delete(ctx, key, held.sha)`.
3. `nil` → drop the claim, count `released`, return `nil`.
4. `errors.Is(err, ErrLeaseConflict)` → drop the claim, count `already_released`, return `nil`.
5. Any other error → set `releasePending = true`, count `error`, return the error. The claim stays held; ticks retry the DELETE instead of renewing until the fence.

**Takeover (Desired Behavior 6).**
1. If the key is already held by this manager, return `nil, ErrLeaseConflict` without a network call.
2. `ref, err := readBack(ctx, key)`; a read-back error → count `error`, return `nil, err`.
3. `ref == nil` → claim the key via the internal claim path (as in `Claim`, without re-locking), then return a `TakeoverResult` with `PreviousTaskID: ""`. The counters are the create counters (`claimed` or `recovered`).
4. Parse `ref.LeaseUntil` with `stdtime.Parse(stdtime.RFC3339, ref.LeaseUntil)`. A parse error → count `takeover_too_early`, log a WARN naming `ref.Ref`, return `nil, ErrLeaseNotExpired` without any write.
5. `now := currentDateTime.Now()`. If `now.Before(leaseUntil.Add(TakeoverGrace))` → count `takeover_too_early`, return `nil, ErrLeaseNotExpired` without any write. At exactly `lease_until + 30 s` takeover is allowed (`Before` is false).
6. `newLease := now.Add(LeaseDuration)`; `newLeaseStr := formatLeaseUntil(newLease)`.
7. `sha, err := refClient.Put(ctx, key, ref.Sha, cluster, taskID, newLease)`.
8. `err == nil` → store the claim, count `taken_over`, return `&TakeoverResult{PreviousTaskID: ref.TaskID}`.
9. `errors.Is(err, ErrLeaseConflict)` → count `conflict`, return `nil, err`. No further request.
10. Any other error → read back:
    - read-back error or no own triple → count `error`, return `nil,` the original error (not held).
    - own triple → store at the listed sha, count `recovered`, return `&TakeoverResult{PreviousTaskID: ref.TaskID}`.
    No `DELETE` is ever sent on the takeover path.

**Tick (Desired Behavior 3–5).** `Tick` reads time only from `currentDateTime.Now()`. It returns an error only when `ctx.Err() != nil`; per-claim failures are handled internally and counted. Execute these steps in this exact order:

1. **Retry pending Job deletions** for claims that were already `pendingJobDeletion` at tick start (claims that become pending in step 2 wait until the next tick):
   - `jobControl.DeleteJob(ctx, taskID)`.
   - error → count `error`, keep pending.
   - success → path `"lost"`: drop the claim, send no ref DELETE. Path `"fence"`: one best-effort `refClient.Delete(ctx, key, sha)` (ignore any error), then drop. Do not count any outcome counter here.
2. **Evaluate the fence for every held claim, before any git-rest renew request**, by calling `m.evaluateFence(ctx, now)`. It skips claims already `pendingJobDeletion` (including any whose retry failed in step 1). For each other claim where `fenceDue(now, leaseUntil)`:
   - If `releasePending`: do NOT delete the Job (it already ended); send one best-effort `refClient.Delete(ctx, key, sha)` (ignore the result), drop the claim, count `fenced`.
   - Else: `jobControl.DeleteJob(ctx, taskID)`.
     - error → count `error` and count `fenced`, mark `pendingJobDeletion = true` with path `"fence"`, keep the claim (no ref DELETE, no renew).
     - success → one best-effort `refClient.Delete(ctx, key, sha)` (ignore the result), drop the claim, count `fenced`.
3. **Retry pending releases** for claims still held with `releasePending` after step 2: `refClient.Delete(ctx, key, sha)`; `nil` → drop, `released`; conflict → drop, `already_released`; other → `error`, keep pending. Step 2 runs before this step, so a claim that reaches its fence tick sends exactly one DELETE (the fence one), not a retry plus a fence DELETE.
4. **Re-ask pending running checks** for held claims with `needsRunningCheck` (after the fence, so a near-expiry claim is fenced before its Job state is trusted):
   - `jobControl.IsJobRunning(ctx, taskID)`.
   - error → count `error`, keep the flag and the claim.
   - running → clear the flag, count `adopted`.
   - not running → attempt a release DELETE with the held sha: `nil` → drop, count `released`; `ErrLeaseConflict` → drop, count `already_released`; other → count `error`, set `releasePending = true`, keep the claim.
5. **Renew due claims, one after another.** Re-read `now := currentDateTime.Now()` before each claim (the clock can advance inside a hanging request). A claim is due when it is not `pendingJobDeletion`, not `releasePending`, and (`renewNextTick` or `renewDue(now, lastRenewAt)`). For each due claim:
   a. `newLease := now.Add(LeaseDuration)`; `newLeaseStr := formatLeaseUntil(newLease)`.
   b. `deadline := m.requestDeadline(now)`; `reqCtx, cancel := context.WithDeadline(ctx, deadline.Time())`; `sha, err := refClient.Put(reqCtx, key, held.sha, cluster, taskID, newLease)`; `cancel()`. Set `lastRenewAt = now`, `renewNextTick = false`.
   c. `err == nil` → adopt `sha` and `newLease`/`newLeaseStr`, count `renewed`.
   d. `errors.Is(err, ErrLeaseConflict)` → count `claim_lost`, log key/task id/held sha at WARN, `jobControl.DeleteJob(ctx, taskID)`; error → count `error`, mark `pendingJobDeletion = true` path `"lost"`; success → drop. Never send a ref DELETE on this path.
   e. Any other error → call `m.evaluateFence(ctx, currentDateTime.Now())` again for every held claim (a claim fenced at this point is not read back). If this claim was fenced/dropped, skip the read-back. Otherwise read back `List(ctx, key)` with a fresh request deadline computed from `currentDateTime.Now()`:
      - read-back error → count `renew_failed`, keep the claim at the old sha and lease.
      - own triple `(cluster, taskID, newLeaseStr)` → adopt the listed sha, count `recovered`.
      - found at the old held sha → count `renew_failed`, keep the claim.
      - absent or any other write → count `claim_lost`, log, `jobControl.DeleteJob`; error → count `error` + pending path `"lost"`; success → drop.
   f. After the renew PUT and after the read-back, call `m.evaluateFence(ctx, currentDateTime.Now())` again for every held claim. `evaluateFence(ctx context.Context, now libtime.DateTime)` implements exactly the fence body from step 2, skips claims already marked `pendingJobDeletion` (they were fenced or lost on an earlier tick and must not be counted twice), and counts `fenced`/`error` once per claim fenced.

**Request deadline.** `requestDeadline(now libtime.DateTime) libtime.DateTime` scans every held claim that is not `pendingJobDeletion` (release-pending and running-check-pending claims are still fenced, so they count) and takes the earliest `fenceAt(leaseUntil)`. Return `now.Add(RequestTimeout)` unless an earliest exists and is earlier, in which case return the earliest. This caps a hanging request at the earliest fence point, so no fence is delayed. Every `refClient` call made inside `Tick` (Put, List and Delete, including the best-effort fence DELETEs in steps 1, 2 and `evaluateFence`, the release retries in step 3 and the release DELETE in step 4) runs under `context.WithDeadline(ctx, m.requestDeadline(currentDateTime.Now()).Time())`, cancelled right after the call. Example: at `T0+220 s` with a claim whose `fenceAt` is `T0+240 s`, the deadline is 20 s, not 30 s.

**Fence rule.** A claim is fenced when `fenceDue(now, leaseUntil)` is true. This makes a 10-second tick land the deletion at or before `fenceAt = lease_until − 60 s`. A fenced claim is never renewed.

**Run (Desired Behavior 3).**
```go
func (m *leaseManager) Run(ctx context.Context) error
```
Loop: if `ctx.Err() != nil` return nil; call `Tick(ctx)` (log and continue on error); call `m.waiter.Wait(ctx, TickInterval)`; if the wait returns an error and `ctx.Err() != nil` return nil, otherwise return the error. Do NOT call `Recover` from `Run` (the later wiring spec calls `Recover` first). Start no goroutine.

**Recover (Desired Behavior 7).**
1. `refs, err := refClient.List(ctx, "claims/")`; error → count `error`, return the error.
2. For each entry whose `Cluster == m.cluster` only (never touch another cluster):
   - `leaseUntil, perr := stdtime.Parse(stdtime.RFC3339, ref.LeaseUntil)`. A parse error → count `error`, log a WARN naming `ref.Ref`, and treat the lease as already past (`leaseUntil = now.Add(-LeaseDuration)`) so the fence branch below applies.
   - `now := currentDateTime.Now()`.
   - If `fenceDue(now, leaseUntil)`:
     - `running, rerr := jobControl.IsJobRunning(ctx, ref.TaskID)`.
     - `rerr != nil` → count `error`; attempt `DeleteJob` anyway (fencing is the safe action).
     - If `running` (or `rerr != nil`): `DeleteJob`; error → count `error` and `fenced`, store a pending deletion at the listed sha with path `"fence"`; success → one best-effort `refClient.Delete(ctx, strings.TrimPrefix(ref.Ref, "refs/"), ref.Sha)` (ignore the result), count `fenced`.
     - If not running: no Job deletion; one best-effort ref DELETE with the listed sha (ignore the result); count `fenced`.
   - Else (not fenced): call `running, rerr := jobControl.IsJobRunning(ctx, ref.TaskID)`.
     - `rerr != nil` → count `error`, store the claim at the listed sha and `lease_until` with `needsRunningCheck = true`, `renewNextTick = true`, `lastRenewAt = now`. Do NOT release or delete the ref.
     - `running` → store the claim at the listed sha and `lease_until` with `lastRenewAt = now`, `renewNextTick = true`; count `adopted`.
     - not running → `refClient.Delete(ctx, key, ref.Sha)`; `nil` → `released`; conflict → `already_released`; other → `error` and do not hold the claim (the ref is left to expire).
3. Recovery never sends a `PUT` with `expected_sha == ""`. Claims of other clusters are never adopted, released or deleted.

4. Add `executor_claim_total` to `/workspace/pkg/metrics/metrics.go`:

```go
// ClaimTotal counts claim lease outcomes by result. The result set is closed;
// keys and task ids are never label values.
var ClaimTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "executor_claim_total",
		Help: "Total number of claim lease outcomes by result.",
	},
	[]string{"result"},
)
```

Use exactly these 13 result values: `claimed`, `conflict`, `renewed`, `recovered`, `renew_failed`, `claim_lost`, `fenced`, `taken_over`, `takeover_too_early`, `released`, `already_released`, `adopted`, `error`. Do NOT add an `init()` pre-initialisation in this prompt (prompt 3 adds it); the tests create the series through `WithLabelValues`.

5. Write the tests. Use `package claim_test`, Ginkgo `Describe`/`Context`/`It` and `DescribeTable` (the repo uses Ginkgo, not stdlib table tests). Tests never sleep; advance time only with `currentDateTime.SetNow(...)`. Use `libtimetest.ParseDateTime` for fixtures. Split scenarios into several `_test.go` files so no function exceeds funlen 80 lines / 50 statements (for example `lease_manager_claim_test.go`, `lease_manager_tick_test.go`, `lease_manager_takeover_test.go`, `lease_manager_restart_test.go`, `lease_manager_recovery_test.go`, `lease_manager_loop_test.go`).

Add shared test helpers in a `_test.go` file:
```go
var claimResults = []string{
	"claimed", "conflict", "renewed", "recovered", "renew_failed",
	"claim_lost", "fenced", "taken_over", "takeover_too_early",
	"released", "already_released", "adopted", "error",
}

func snapshotClaims() map[string]float64
func expectClaimDelta(before, after map[string]float64, expected map[string]float64)
```
`snapshotClaims` reads `testutil.ToFloat64(metrics.ClaimTotal.WithLabelValues(name))` for every one of the 13 names (this also creates the series). `expectClaimDelta` asserts `after[name]-before[name] == expected[name]` (default 0) for all 13 names. Every scenario test snapshots before its action and calls `expectClaimDelta` with **every** series that actually changes in that scenario (including `error` where a failure is injected).

Add a fake-clock helper and a fake JobControl recorder:
```go
func newTestClock(t0 libtime.DateTime) libtime.CurrentDateTime
func advance(clock libtime.CurrentDateTime, d libtime.Duration)
func tickAt(mgr claim.LeaseManager, clock libtime.CurrentDateTime, target libtime.DateTime)
```
`tickAt` advances the clock to `target` in `TickInterval` steps and calls `Tick` at each step, so the "every 10 s" cadence is explicit.

Add `countSuccessfulPuts(requests []fakeRequest) int` (PUT requests whose recorded `Status == 200`).

**AC1 — create race (`lease_manager_claim_test.go`).**
- `fake := newFakeGitRest()`, `fake.SetPutBarrier(2)`.
- Two managers with cluster names `"a"` and `"b"`, same `claim.NewClaimRefClient(fake.URL(), "", "")`, a fake JobControl, the same clock.
- Run both `Claim(ctx, "claims/task/race", "t-race")` concurrently (test goroutines + `sync.WaitGroup`).
- Assert exactly one returns a non-nil held claim and nil error; the other returns an error with `errors.Is(err, claim.ErrLeaseConflict)`.
- Assert every recorded PUT body has `expected_sha == ""`.
- Assert `fake.Ref("refs/claims/task/race").Sha` equals the winner's `HeldClaim.Sha`.
- Assert `expectClaimDelta` with `claimed +1`, `conflict +1`.
- Harness self-check in the same file: reset the fake; manager A claims `claims/task/selfcheck`; then a test-only upsert claimer lists the key, sends A's sha as `expected_sha` via `client.Put`, and gets 200. Assert `countSuccessfulPuts(fake.Requests()) == 2`, proving the same holder-counting helper the race assertion uses can report two winners.
- The whole file must pass `go test -race -count=1`.

**AC2 — lost claim and fence (`lease_manager_tick_test.go`).**
- (a) Stale renew: claim at T0 holds S0; seed the ref to another holder's sha (cluster `"other"`, task `"t-other"`, lease T0+300 s); tick at T0+60. Assert the fake JobControl recorded exactly one `DeleteJob` for the task id, the manager holds nothing, no `DELETE` request names the key, `claim_lost +1`.
- (b1) Aligned fence: claim at T0; `fake.SetFault(func(fakeRequest) *fakeFault { return &fakeFault{Status: 500} })`. Tick every 10 s from T0+10 to T0+230: assert `DeleteJob` count is 0 after each. Reset requests before each tick and record which ticks send PUTs: renew PUTs must be at T0+60, T0+120, T0+180 and no other tick. Tick at T0+240: assert `DeleteJob` count is 1, no renew PUT at that tick (fence evaluated before the network), and exactly one `DELETE` with `expected_sha=S0` answered 500 (ignored). Assert the claim is no longer held; tick at T0+250 and assert no request names the key. `expectClaimDelta`: `renew_failed +3`, `fenced +1`.
- (b2) Mid-phase fence: claim at T0+45; same 500 fault; ticks at T0+50 … T0+270 show 0 deletions; the tick at T0+280 shows 1 deletion. `fenced +1`.
- (b3) Hanging renew: use `mocks.FakeClaimRefClient` (not the fake server). The clock is T0.
  - `PutStub`: for the create (`expected_sha == ""`) return `"S0"`, nil. For Y's renews return a fast non-conflict error. For X's renews before T0+220 return a fresh sha and nil, and record the new held sha and the `leaseUntil` argument (format it with `time.RFC3339`) as X's last successful lease. For X's renew at the tick whose now is T0+220: read `ctx.Deadline()`, record `remaining := deadline.Sub(clock.Now().Time())`, advance the clock by `remaining`, return `context.DeadlineExceeded`.
  - `ListStub`: for prefix `claims/task/y` return a 500 error; for prefix `claims/task/x` return one `ClaimRef` with `Ref: "refs/claims/task/x"`, `Sha` = X's current held sha, `Cluster` = manager cluster, `TaskID` = X's task id, `LeaseUntil` = the formatted lease of X's last successful renew (recorded in `PutStub`). At the hanging tick the read-back therefore shows X at its old held sha, which is the `renew_failed` branch.
  - `DeleteStub`: return nil.
  - Claim Y at T0, claim X at T0+35. Tick every 10 s through T0+220.
  - Assert the recorded remaining time is > 0 and ≤ 20 s (Y's `fenceAt` is T0+240, X renewed successfully at T0+100 and T0+160, so its own `fenceAt` is T0+400; the 30 s cap is larger too).
  - Assert `DeleteJob` for Y is recorded at fake time T0+240 (the fake JobControl records `clock.Now()` at call), not later; X is still held; `renew_failed` total +4 (Y at 60/120/180, X at the hanging tick); `fenced +1` (Y).
- (c) Job deletion fails: as (b1), but `DeleteJob` returns an error on its first call. At T0+240: `fenced +1`, `error +1`, the claim is still in `HeldClaims()`, no ref DELETE, no renew PUT. At T0+250: second `DeleteJob` succeeds, then exactly one best-effort `DELETE` with `expected_sha=S0`, the claim is gone, and no counter changes at T0+250. Repeat the same shape for the lost path: `claim_lost +1` and `error +1` at T0+60, the retry at T0+70 succeeds, no ref DELETE at either tick.

**AC3 — takeover (`lease_manager_takeover_test.go`).**
- Seed `refs/claims/task/K` with sha SX, cluster `"other"`, task `"t-old"`, `lease_until` = T (RFC3339).
- Two managers. At fake times T and T+29: both `Takeover` return `errors.Is(err, claim.ErrLeaseNotExpired)`; the fake records zero PUT and zero DELETE requests; `takeover_too_early` delta +4.
- At T+30: arm `fake.SetPutBarrier(2)`, run both `Takeover` concurrently. Assert exactly two PUTs on K, each with `expected_sha=SX`, and no DELETE. Exactly one manager holds K and its result reports `PreviousTaskID == "t-old"`; the other returns `ErrLeaseConflict` and sends no further request. Delta `taken_over +1`, `conflict +1`.
- Unparseable row: seed `refs/claims/task/K` with `lease_until` `"not-a-time"` and, separately, `""`; `Takeover` at any fake time returns `errors.Is(err, claim.ErrLeaseNotExpired)`, the fake records zero PUT/DELETE, `takeover_too_early +1` per call.
- Absent row: seed only `refs/claims/task/K/x` (sibling), not `refs/claims/task/K`. `Takeover` sends one PUT with `expected_sha == ""`, returns `PreviousTaskID == ""`, and counts `claimed +1`. The sibling's sha must not be used as `expected_sha`.

**AC4 — restart (`lease_manager_restart_test.go`).**
- Seed five refs: A `refs/claims/task/a` (self, t1, lease far future), B `refs/claims/task/b` (self, t2, far future), C `refs/claims/task/c` (other, t3), D `refs/claims/task/d` (self, t4, `lease_until = now + 65 s` so `now = fenceAt − 5 s`), E `refs/claims/task/e` (self, t5, far future). Keep the shas.
- Fake JobControl: `IsJobRunning` returns true for t1 and t4, false for t2; for t5 returns an error on the first call and true on the second. `DeleteJob` succeeds.
- `mgr.Recover(ctx)`.
- Assert: zero PUTs with `expected_sha == ""`; exactly one `DeleteJob` (t4); exactly two DELETE requests (B with B's listed sha, D with D's listed sha); no PUT or DELETE names C; A is held at A's listed sha; D is not held; E is held; deltas `adopted +1` (A), `released +1` (B), `fenced +1` (D), `error +1` (E's failed running check).
- Tick at T0+10: renew PUTs for A and E carry their listed shas and get 200 (`renewed +2`); t5's running check is re-asked, now answers running, `adopted +1`. Afterward the fake's refs are A, C and E only.
- Row D′: same fixture, but `DeleteJob` for t4 fails on its first call and succeeds on the second. After `Recover`: no ref DELETE for D, D is in `HeldClaims()` as pending, `fenced +1` and `error +1`. At the next tick: a second `DeleteJob` for t4, then the best-effort DELETE with D's listed sha; D is gone. A manager that drops D when the Job deletion fails would send D's ref DELETE too early — assert it does not.

**AC5 — lost-response recovery (`lease_manager_recovery_test.go`).** `DescribeTable` with nine rows. Use the real ref client against the fake server; drive each fault through `fake.SetFault`.
- Setup helper: claim key K at T0 with task `"t-r"`, advance 60 s, then `Tick`.
- Row 1: fault on the renew PUT (`expected_sha != ""`) with `{Status: 500, Apply: true}`. Read-back finds the manager's own triple → held at S1, zero Job deletions, `recovered +1`; clear the fault, advance 60 s, `Tick` → the renew carries `expected_sha=S1` and gets 200, `renewed +1`.
- Row 2: seed the ref to another holder's write before the tick; fault PUT `{Status: 500}` (not applied). → one `DeleteJob`, `claim_lost +1`.
- Row 3: fault PUT `{Status: 500}` (not applied), ref still S0 → held at S0, zero Job deletions, `renew_failed +1`; the next renew carries `expected_sha=S0`.
- Row 4: after claiming, `fake.DeleteRef(K)` and `fake.Seed("refs/"+K+"/x", <a sha>, cluster, taskID, <the lease_until the renew wrote>)`; fault PUT `{Status: 500}`. Exact-ref match sees K absent → one `DeleteJob`, `claim_lost +1`. A prefix matcher would adopt the sibling — assert it does not.
- Row 5: fault PUT 500 and fault GET 500 → still held at S0, `renew_failed +1`.
- Row 6: create fault `{Status: 500, Apply: true}` → read-back lists the manager's own triple → held at the listed sha, `Claim` returns success, `recovered +1`.
- Row 7: create fault `{Status: 500}` (not applied) → read-back shows K absent → `Claim` returns a non-nil error that is not `ErrLeaseConflict`, K is not held, `error +1`.
- Row 8: takeover `PUT` with `expected_sha=SX`, fault `{Status: 500, Apply: true}` → read-back lists the own triple → held, result reports the previous `task_id`, `recovered +1`.
- Row 9: same takeover not applied, ref still at SX → error returned, not held, `error +1`.

**AC6 — release (`lease_manager_release_test.go`).**
- Success: claim, then `Release` → one DELETE with the held sha; 200 → not held, `released +1`.
- Conflict: fault DELETE 409 → `Release` returns nil, not held, `already_released +1`.
- Server failure: fault DELETE 500 → `Release` returns a non-nil error that is not `ErrLeaseConflict`, the claim stays held, `error +1`; the next tick sends another DELETE (not a renew PUT) with the same sha.
- Release-to-fence: claim at T0, fault every DELETE with 500, call `Release` at T0, tick every 10 s from T0+10 to T0+230. At T0+240 the fence path applies: zero `DeleteJob`, exactly one best-effort DELETE with the held sha (the fence DELETE), the claim is gone, `fenced +1`, and at T0+250 no request names the key. Failed release DELETEs count `error` (initial release + one per tick through T0+230 = 24); assert that exact `error` delta and explain it in a comment.

**AC10 — loop (`lease_manager_loop_test.go`).**
- Construct a manager with `mocks.FakeClaimRefClient` (`ListStub` returns `[]ClaimRef{}, nil`), a fake JobControl, the clock, and `libtimemocks.WaiterDuration` (import `libtimemocks "github.com/bborbe/time/mocks"` — the repo's own `mocks` package holds `FakeClaimRefClient`).
- `WaitStub`: record the duration, cancel the test context on the first call, and return `ctx.Err()`.
- Run `mgr.Run(ctx)` in a test goroutine; `Eventually(func() bool { return done })` within 1 second.
- Assert `WaitCallCount() >= 1`, the duration returned as the second value of `WaitArgsForCall(0)` equals `claim.TickInterval` exactly (10 s), and `Run` returned nil.
- Production-code greps (in the verification section) must show zero direct `time.Now/Sleep/After/AfterFunc/NewTimer/NewTicker/Since/Until/Tick` and zero `go func`.
</requirements>

<constraints>
- The executor's runtime behaviour is unchanged: nothing outside `pkg/claim` and its generated mock imports the package. Do NOT wire the manager into `main.go`, the task event handler, the reconcile loop, the spawner or the Job watcher.
- Time comes only from the injected `github.com/bborbe/time` `CurrentDateTimeGetter`; the loop's wait only from the injected `WaiterDuration`. Tests use `SetNow`, the `WaiterDuration` fake, and never sleep.
- No raw `go func` and no direct `time.Now/Sleep/After/AfterFunc/NewTimer/NewTicker/Since/Until/Tick` in production code of `pkg/claim`.
- `409` is never retried. The ref client is called at most once per operation; read-back is a separate, explicit request.
- Do NOT add knobs for lease length, tick interval, renew interval, fence margin, takeover grace or request timeout. They are invariant constants.
- Keys and task ids never become metric label values. Only the closed 13-value `result` label exists.
- The manager's state is safe for concurrent use by the tick loop and claim/release/takeover callers; `go test -race` must stay green.
- A claim fenced at or before `fenceAt` is never renewed and is dropped without a ref DELETE when its Job deletion fails (pending-deletion state).
- Do NOT add a CHANGELOG entry in this prompt — the feature-level `feat:` bullet lands in prompt 3.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `go test -mod=mod -race -count=1 ./pkg/claim/...` iteratively after each change, then `make precommit` ONCE at the very end.

- `go test -mod=mod -race -count=1 ./pkg/claim/...` — exits 0.
- `grep -l 'FakeClaimJobControl' mocks/*.go` — returns at least one file.
- `grep -rnE 'time\.(Now|Sleep|After|AfterFunc|NewTimer|NewTicker|Since|Until|Tick)\(' pkg/claim --include='*.go' | grep -v '_test.go'` — returns 0 lines.
- `grep -rn 'go func' pkg/claim --include='*.go' | grep -v '_test.go'` — returns 0 lines.
- `grep -rn 'executor_claim_total' pkg/metrics/` — returns at least one line.
- `grep -c 'claimed\|conflict\|renewed\|recovered\|renew_failed\|claim_lost\|fenced\|taken_over\|takeover_too_early\|released\|already_released\|adopted\|error' pkg/claim/*_test.go` — every one of the 13 result names appears in the test files.
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/claim/... && go tool cover -func=/tmp/cover.out` — `Claim`, `Release`, `Takeover`, `Tick`, `Recover`, `Run`, `requestDeadline`, `evaluateFence`, `readBack` each exercised; ≥80% statement coverage for new code.
- `make precommit` — exits 0.

Do NOT run `docker`, `make build`, `make buca`, `kubectl`, `dark-factory`, `gh`, or `scripts/*.sh` commands in this prompt — those are operator-executable and belong on the spec's verification ladder.
</verification>

Implementation notes (settled):
- Convert every `stdtime.Parse` result to `libtime.DateTime(parsed)` before using it with libtime durations or `fenceDue` (Takeover step 5, Recover).
- `NewLeaseManager` has no ctx: build the empty-cluster error with `stderrors.New`.
- Recover with an unparseable `lease_until` on an own-cluster entry treats it as fence-due: count `error` +1, then fence it like any other fence-due entry (never adopt).
- A failed release DELETE counts `error` +1 per failure.
