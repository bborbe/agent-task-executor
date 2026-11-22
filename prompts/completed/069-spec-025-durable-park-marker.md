---
status: completed
spec: [025-bug-rearm-clobbered-by-stale-park-reemission]
summary: Moved park-publish dedup into a durable per-completion claim.ParkStore keyed by success completion, kept the per-pod map as the fallback floor, wired handler helpers/options, added claim and handler tests, and recorded an Unreleased changelog entry.
execution_id: 5f9bfd78-31e8-5f03-be25-0d5a3c166713-exec-069-spec-025-durable-park-marker
dark-factory-version: dev
created: "2026-11-22T09:30:00Z"
queued: "2026-11-22T09:44:12Z"
started: "2026-11-22T09:44:13Z"
completed: "2026-11-22T10:02:40Z"
branch: dark-factory/bug-rearm-clobbered-by-stale-park-reemission
---

# Durable per-completion park marker

<summary>
- A park is published at most once per successful Job completion, even after the pod restarts or a different cluster runs the next pass.
- The "this success was already parked" record moves from one pod's memory into the shared claim store, keyed by the success completion, so a fresh pod reads it.
- A second, genuinely later completion of the same task still gets its own park; the stored completion value advances.
- A success with no recorded park still parks exactly once — the negative control that proves the park path was not removed.
- When the shared store cannot be read, the pod's own memory is the floor: the first park still publishes and a repeat on that pod is still suppressed.
- When the shared store cannot be written, the park still stands; the write failure is logged and never re-enables the task-file rewrite.
- A deployment with no shared claim store keeps today's behaviour exactly.
- The task file is not touched by a suppressed park.
</summary>

<objective>
Move the park-publish dedup from the per-pod `TaskStore.parked` map into a durable `claim.ParkStore` in the shared git-rest claim namespace, keyed per success completion, while keeping the in-memory map as a safety floor. A park re-emitted for a success that already produced one must be suppressed across restarts and clusters, so a deliberate re-arm is not rewritten back to `completed`.
</objective>

<context>
Read `CLAUDE.md` if present, and `README.md`, for project conventions.

Read these files in full before changing anything:

- `pkg/handler/park_succeeded.go` — the whole park path. The dedup block is `if publishedAt, ok := h.taskStore.ParkPublishedAt(...)` and the record write is `h.taskStore.MarkParkPublished(...)`. `succeeded` is a `pkg.SucceededJob` with fields `Phase` (string) and `CompletedAt` (time.Time). `now` is a `time.Time`. Do not change any other decision input (`taskShowsPriorRun`, `last_result_status`, the grace window, `maxRedrivesWithoutAdvance`).
- `pkg/task_store.go` — `parkRecord`, `MarkParkPublished`, `ParkPublishedAt`. **Keep all three exactly as they are**; they become the per-pod floor. Do not delete the map.
- `pkg/claim/success_store.go` — the template this store mirrors: frozen prefix, sentinel cluster, `validateClaimComponent`, `parseSuccessRef`, `keepOnly`, the 409-is-idempotent handling, `redriveListForTask` / `redriveDeleteRef` reuse, `libtime` clock injection.
- `pkg/claim/ref_client.go` — `ClaimRefClient` (`List`, `Put(ctx, key, expectedSha, cluster, taskID, leaseUntil)`, `Delete`) and `ClaimRef{Ref, Sha, Cluster, TaskID, LeaseUntil}`. The PUT body carries only `expected_sha`, `cluster`, `task_id`, `lease_until` — there is no free-text payload, so the completion time rides in the key and the publish time rides in `lease_until`.
- `pkg/handler/success_record.go` — `successStore()` (lazy build from `claimRefClientFactory`, `sync.Once`, injected-store-wins) and `successRecordRetention`. Mirror this shape for the park store.
- `pkg/handler/task_event_handler_options.go` — `HandlerOption`, `WithSuccessStore`.
- `pkg/handler/task_event_handler.go` — the `taskEventHandler` struct fields near `successRecordStore` / `successStoreOnce`. This file is 1988 lines against revive's `file-length-limit: 2000`; add only the two struct fields here (about two lines) and put every helper in a new file.
- `pkg/handler/park_succeeded_internal_test.go` — the `parkFixture` / `parkRecordingPublisher` shape and the existing park specs, which must keep passing.
- `pkg/handler/claim_wiring_internal_test.go` — `parkRecordingPublisher` (fields `parkedSucceeded`, `parkedRedriveCap`, `failParkSucceeded`).
- `pkg/handler/claim_job_control_internal_test.go` — `fakeTaskView`, `staticResolver`.
- `pkg/handler/redrive_classifier_internal_test.go` — `newDecisionHandler` (no opts parameter; build your own handler in the new test file).
- `pkg/claim/success_store_test.go` — the `fakeGitRest` / `newTestClock` / `leaseTestT0` Ginkgo patterns and `successRef`.
- `docs/claim-lease-protocol.md` § *Parking* — the park paths this marker records (a later prompt extends the doc; do not edit it here).

Read these coding guides from the coding plugin at `/home/node/.claude/plugins/marketplaces/coding/docs/`:
- `go-testing-guide.md` — Ginkgo v2/Gomega conventions
- `go-glog-guide.md` — `event=` key=value style and V-levels
- `go-error-wrapping-guide.md` — `github.com/bborbe/errors` (`errors.Wrapf(ctx, err, ...)`), never `fmt.Errorf`
- `go-time-injection.md` — `libtime.CurrentDateTimeGetter` / `libtime.NewCurrentDateTime`

Verified facts (do not re-derive):
- `libtime.DateTime` is a `time.Time`-shaped type with `.Time() time.Time`, `.Add(libtime.Duration) libtime.DateTime`, and `libtime.Duration` has `.Duration() time.Duration`. `libtime.ParseDateTime(ctx, string) (libtime.DateTime, error)` parses RFC3339.
- The git-rest `Put` lease is formatted with `time.RFC3339` (whole seconds), so a recovered publish time is exact when the test clock is whole-second.
- Restart recovery (`leaseManager.Recover`) lists through the typed claim client filtered by cluster, and the success store already lives under `claims/` with a sentinel cluster that never equals a real cluster name; a `claims/park/` prefix with the same sentinel shape is therefore invisible to recovery, exactly like `claims/success/`.
- `redriveListForTask` and `redriveDeleteRef` (in `pkg/claim/redrive_refs.go`) are package-private and reusable for any prefix.
</context>

<requirements>
1. Create `pkg/claim/park_store.go` (`package claim`, BSD license header). Mirror `success_store.go`:

   - Frozen constants:
     ```go
     const (
         // ParkRecordPrefix is the frozen key prefix of every park marker.
         // Changing it strands every existing marker.
         ParkRecordPrefix = "claims/park/"
         // ParkRecordCluster is the sentinel cluster field on every park ref.
         // git-rest refuses an empty cluster, and this value must never equal a
         // real executor cluster name.
         ParkRecordCluster = "park-record"
     )
     ```
   - Interface and constructor:
     ```go
     type ParkStore interface {
         // Record stores that a park for the success completed at completedAt was
         // published at publishedAt, keyed by the completion, and deletes the
         // task's strictly older park refs.
         Record(ctx context.Context, taskID string, phase string, completedAt libtime.DateTime, publishedAt libtime.DateTime) error
         // PublishedAt returns when the park for the success completed at
         // completedAt was published, and whether one is recorded.
         PublishedAt(ctx context.Context, taskID string, completedAt libtime.DateTime) (libtime.DateTime, bool, error)
     }

     func NewParkStore(refClient ClaimRefClient, currentDateTime libtime.CurrentDateTimeGetter, retention libtime.Duration) ParkStore
     ```
   - Ref shape: `claims/park/<taskid>/<phase component>/<completed unix seconds>`. Normalise the phase component with `RedriveStreakPhaseComponent(phase)` (so an empty phase becomes `none`). The completion time rides in the key. The publish time rides in `lease_until`, which `Record` sets to `publishedAt.Add(retention)`; `PublishedAt` recovers it as `libtime.DateTime(leaseUntil.Time().Add(-s.retention.Duration()))`. Document this encoding in the file: both writer and reader use the same `retention`, so the recovery is exact.
   - `parseParkRef(ref string) (taskID, phase string, at libtime.DateTime, ok bool)` — five segments under `claims/park/`, last segment a positive integer, mirroring `parseSuccessRef` but checking `parts[1] == "park"`.
   - `Record`:
     - validate task id and the phase component with `validateClaimComponent`;
     - `Put(key, "", ParkRecordCluster, taskID, publishedAt.Add(s.retention))`;
     - a `ErrLeaseConflict` (409) means the same completion is already recorded (two clusters, or a re-delivery) — log `event=park_record_exists task=<id> phase=<component> completed=<RFC3339>` at `glog.V(3)` and treat it as success, not an error; any other error is wrapped with `errors.Wrapf`;
     - then `keepOnly(ctx, taskID, key)`.
   - `PublishedAt`:
     - validate the task id;
     - list `ParkRecordPrefix+taskID` via `redriveListForTask`;
     - for each parsed ref whose `refTaskID == taskID` **and whose completion second equals `completedAt`'s** (completion is unique per task; the phase segment is not part of the match), parse `lease_until`; skip a ref whose lease is absent or unparseable (log `event=park_record_lease_unparsed`); skip an expired ref (`!now.Time().Before(leaseUntil.Time())`); otherwise return the derived publish time with `true`;
     - return `(libtime.DateTime{}, false, nil)` when nothing matches; wrap a list failure with `errors.Wrapf`.
   - `keepOnly(ctx, taskID, keep string)` — mirror `successStore.keepOnly`: parse `keep`, list the task's park refs, delete every ref of the task whose completion is **strictly older** than `keep`'s completion, best-effort (log `event=park_record_prune_failed`, never fail `Record`). This is the bound: one ref per task, so a stale completion can no longer re-emit a park and its deleted marker is never needed. Only a strictly older completion is deleted, so two racing clusters writing the same completion never delete each other's ref.

   Imports: `context`, `stderrors "errors"`, `strconv`, `strings`, `time`, `github.com/bborbe/errors`, `libtime "github.com/bborbe/time"`, `github.com/golang/glog`.

2. Create `pkg/handler/park_store.go` (`package handler`, BSD license header) with three methods on `*taskEventHandler`:
   - `func (h *taskEventHandler) parkStore() claim.ParkStore` — mirror `successStore()` exactly: inside `h.parkStoreOnce.Do`, return early if `h.parkRecordStore != nil`; return early if `!h.claimMode.TakesClaim() || h.claimRefClientFactory == nil`; build `claim.NewParkStore(refClient, h.currentDateTime, libtime.Duration(successRecordRetention))`; return the field. In claim mode off with no injection this returns `nil`, which is the unchanged deployment.
   - `func (h *taskEventHandler) parkPublishedAt(ctx context.Context, taskID lib.TaskIdentifier, completedAt time.Time) (time.Time, bool)`:
     - if `store := h.parkStore(); store != nil`, call `store.PublishedAt(ctx, string(taskID), libtime.DateTime(completedAt))`;
     - on a returned error, log `glog.Warningf("event=park_marker_read_failed task=%s error=%v", taskID, err)` and fall through to memory;
     - on `ok`, return `at.Time(), true`;
     - otherwise fall through to `h.taskStore.ParkPublishedAt(taskID, completedAt)` (the floor).
   - `func (h *taskEventHandler) markParkPublished(ctx context.Context, taskID lib.TaskIdentifier, phase string, completedAt time.Time, publishedAt time.Time)`:
     - call `h.taskStore.MarkParkPublished(taskID, completedAt, publishedAt)` **first** (the floor, so a failed store write still suppresses this pod);
     - if `store := h.parkStore(); store != nil`, call `store.Record(ctx, string(taskID), phase, libtime.DateTime(completedAt), libtime.DateTime(publishedAt))`; on error log `glog.Warningf("event=park_marker_write_failed task=%s error=%v", taskID, err)` and return — the failure is never propagated and never re-enables the write-back path.

   Imports: `context`, `time`, `lib "github.com/bborbe/agent"`, `libtime "github.com/bborbe/time"`, `github.com/golang/glog`, `"github.com/bborbe/agent-task-executor/pkg/claim"`.

3. Add the two struct fields to `taskEventHandler` in `pkg/handler/task_event_handler.go`, immediately beside `successRecordStore claim.SuccessStore` / `successStoreOnce sync.Once`:
   ```go
   	// parkRecordStore is the durable, per-completion park marker; parkStoreOnce
   	// builds it lazily from the claim-ref factory, or an injected fake wins.
   	parkRecordStore claim.ParkStore
   	parkStoreOnce   sync.Once
   ```
   Add no other line to this file.

4. In `pkg/handler/task_event_handler_options.go`, add, beside `WithSuccessStore`:
   ```go
   // WithParkStore supplies the durable park marker directly, bypassing the
   // claim-ref-client factory. It exists for tests that need a fake — the store's
   // error paths have no other way in, since the factory is a boot-time
   // constructor with no failure mode to provoke.
   func WithParkStore(store claim.ParkStore) HandlerOption {
   	return func(h *taskEventHandler) {
   		h.parkRecordStore = store
   	}
   }
   ```

5. In `pkg/handler/park_succeeded.go`, replace the two in-memory calls with the new helpers, changing nothing else:
   - old → new (the dedup read):
     ```go
     	if publishedAt, ok := h.taskStore.ParkPublishedAt(
     		task.TaskIdentifier,
     		succeeded.CompletedAt,
     	); ok {
     ```
     becomes
     ```go
     	if publishedAt, ok := h.parkPublishedAt(
     		ctx,
     		task.TaskIdentifier,
     		succeeded.CompletedAt,
     	); ok {
     ```
   - old → new (the record write, after the successful publish, replacing the single `h.taskStore.MarkParkPublished(...)` line):
     ```go
     	h.markParkPublished(
     		ctx,
     		task.TaskIdentifier,
     		succeeded.Phase,
     		succeeded.CompletedAt,
     		now,
     	)
     ```
   Keep the surrounding `event=park_awaiting_write_back` suppression, the `return true` / `return false` window logic, and the post-publish `return true`.

6. Create `pkg/claim/park_store_test.go` (`package claim_test`, Ginkgo v2/Gomega, BSD license header). Reuse the harness from `success_store_test.go`: `newFakeGitRest()`, `claim.NewClaimRefClient(fake.URL(), "", "")`, `newTestClock(leaseTestT0)`, and `const retention = 24 * libtime.Hour`. Add `parkRef := func(taskID, phase string, at time.Time) string` returning `"refs/" + claim.ParkRecordPrefix + taskID + "/" + phase + "/" + strconv.FormatInt(at.Unix(), 10)`. Cover:
   - **Record then PublishedAt round-trips** — record completion `C`, publish `P` (both whole seconds derived from `leaseTestT0`); `PublishedAt(ctx, "tid-a", C)` returns ok and a time equal to `P`.
   - **Survives a fresh instance (AC 4)** — write through `store1`; construct `store2 := claim.NewParkStore(claim.NewClaimRefClient(fake.URL(), "", ""), clock, retention)`; `store2.PublishedAt(ctx, "tid-a", C)` returns `P`. The store has no memory, so this is the restart proof.
   - **A later completion advances the marker (AC 2)** — record `C1`/`P1`, then `C2`/`P2` (C2 later); `PublishedAt(C1)` is not found, `PublishedAt(C2)` returns `P2`, and the `C1` ref is gone.
   - **One ref per task** — after the two completions, `_, c1Exists := fake.Ref(parkRef("tid-a", "execution", C1.Time()))` is `false` and `_, c2Exists := fake.Ref(parkRef("tid-a", "execution", C2.Time()))` is `true` (`fake.Ref` is the `fakeGitRest` accessor used by `success_store_test.go`).
   - **Same completion twice is idempotent** — `Record` the same completion twice succeeds with no error, `fake.Ref(parkRef(...))` is still `true`, and `PublishedAt` returns the **first** publish time (the second `Put` is a 409 no-op). This is the two-cluster case.
   - **Rejects a task id that would escape the prefix** — `Record(ctx, "bad/id", ...)` and `PublishedAt(ctx, "bad/id", ...)` each return an error.
   - **Skips a malformed ref** — seed a key with the wrong segment count; `PublishedAt` does not return it.

7. Create `pkg/handler/park_marker_internal_test.go` (`package handler`, plain Go tests, BSD license header). Do **not** import `mocks` (that package imports `pkg/handler`; an internal test cannot import it). Define a hand-written `fakeParkStore` implementing `claim.ParkStore` with a `records map[string]map[int64]time.Time` (taskID → completed unix → published), plus independent `readErr error` and `writeErr error` knobs; `Record` returns `writeErr` when set, otherwise stores; `PublishedAt` returns `readErr` when set, otherwise looks up by completion. Guard the map with a `sync.Mutex`.
   Build a fixture that constructs a real handler via `NewTaskEventHandler(...)` in `ClaimModeOff` (nil factory) with `WithParkStore(store)`, using `stubJobSpawner`, `parkRecordingPublisher`, `nil` resolver, `pkg.NewTaskStore()`, a `clock := libtime.NewCurrentDateTime()`, and a `fakeTaskView` holding the task. Copy the task shape from `parkFixture`: `status: in_progress`, `phase: execution`, `assignee: claude`, `current_job: agent-task-park`, `last_result_status: done`. Seed the success via `h.taskStore.MarkSucceeded(id, "execution", completedAt)` (the shared success store is nil in claim mode off, so `lastSuccess` reads memory). Cover:
   - **Recorded park suppresses a re-publish (AC 1, AC 8)** — record a park for the success in the fake store at `now - longAgo` (past the write-back grace) with the handler's memory empty; call `parkSucceededWithoutPhaseMove`; assert it returns `false` (this pass spawns, not re-publishes) and `len(publisher.parkedSucceeded) == 0`; capture stderr with `captureStderr` and assert `strings.Count(out, "event=parked_succeeded") == 0` (no frontmatter write was emitted).
   - **No record still parks exactly once (AC 3)** — empty fake store, success older than the grace, done result; assert `parkSucceededWithoutPhaseMove` returns `true`, `len(publisher.parkedSucceeded) == 1`, and `strings.Count(out, "event=parked_succeeded") == 1`.
   - **A later completion parks again (AC 2, handler level)** — first success `C1` parks (publisher 1, `store.records` holds `C1`); then `h.taskStore.MarkSucceeded(id, "execution", C2)` with `C2` later and the view unchanged; second call publishes (publisher 2) and `store` now answers `C2` (and no longer `C1`).
   - **Store read failure falls back to memory (AC 5)** — set `store.readErr`; first call publishes (`len(publisher.parkedSucceeded) == 1`) and logs `event=park_marker_read_failed`; second call, same handler, is suppressed (`returns true`, publisher still 1) because memory now holds the record.
   - **Store write failure keeps the park (failure mode)** — `readErr` nil, `writeErr` set; first call publishes (`len == 1`) and logs `event=park_marker_write_failed`, and the function still returns `true`; a second call is suppressed from memory.
   Use `deferredGateCaptureStderr` (it sets `v=2`) for any assertion on a `glog.V(2)` line; use `captureStderr` for the bare `event=parked_succeeded` line. Set `clock.SetNow(...)` so `now`, the completion, and the publish time are all relative to a fixed instant.

8. Before finishing, re-run every command in `<verification>`. Walk the spec's Acceptance Criteria 1-5 and 8 and confirm each is asserted above. Do not attempt the Post-Deploy (Rung-2) criteria; they are operator-executed after merge.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass, including every spec in `pkg/handler/park_succeeded_internal_test.go` and `pkg/claim/success_store_test.go`.
- Keep `pkg/task_store.go`'s `parked` map, `MarkParkPublished`, and `ParkPublishedAt` byte-for-byte: the per-pod map is the safety floor.
- Claim mode off is unchanged: no shared store, in-memory only.
- No new task-frontmatter field, config knob, env var, flag, opt-out, or metric. The only new observability is the log lines `park_marker_read_failed`, `park_marker_write_failed`, `park_record_exists`, `park_record_prune_failed`, `park_record_lease_unparsed`.
- The marker is keyed by the success completion, not the task: a later completion must advance the stored value and get its own park.
- Only a strictly older completion is deleted on write; never delete a same-or-newer completion, or two racing clusters would erase each other's ref.
- A store write failure must never re-enable the write-back path: `markParkPublished` swallows the error and the caller still returns `true`.
- A store read failure must fall back to the per-pod map, never to "no record".
- `pkg/handler/task_event_handler.go` is at 1988/2000 lines (revive `file-length-limit`): add only the two struct fields; all helpers go in new files.
- Do not import `mocks` from a `package handler` test file (import cycle).
- Wrap errors with `github.com/bborbe/errors` (`errors.Wrapf(ctx, err, ...)`), never `fmt.Errorf`.
- Use the injected `libtime` clock; do not read the wall clock.
</constraints>

<verification>
- `make precommit` — must exit 0.
- `go test ./pkg/claim/ ./pkg/handler/ -count=1` — must exit 0.
- `grep -q 'event=park_marker_read_failed' pkg/handler/park_store.go && echo READ_FAIL_LOG_OK` — must print `READ_FAIL_LOG_OK`.
- `grep -q 'event=park_marker_write_failed' pkg/handler/park_store.go && echo WRITE_FAIL_LOG_OK` — must print `WRITE_FAIL_LOG_OK`.
- `grep -q 'ParkRecordPrefix' pkg/claim/park_store.go && echo PREFIX_OK` — must print `PREFIX_OK`.
- `grep -q 'MarkParkPublished' pkg/handler/park_store.go && echo FLOOR_OK` — must print `FLOOR_OK`.
- `go test ./pkg/handler/ -v -args -ginkgo.focus='park' > /tmp/park.out 2>&1; grep -q 'SUCCESS!' /tmp/park.out && echo FOCUS_OK` — must print `FOCUS_OK`.
</verification>
