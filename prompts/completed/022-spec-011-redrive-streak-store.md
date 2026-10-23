---
status: completed
spec: [011-bug-redrive-streak-lost-across-clusters-and-restarts]
summary: Added a git-rest-backed per-task, per-phase redrive streak store with Count/Record/Clear, an ErrRefNotFound sentinel for 404 deletes, and full Ginkgo specs covering cross-instance counting, stored shape, race retry, phase reset, clear, foreign entries and validation.
execution_id: agent-task-executor-redrive-streak-exec-022-spec-011-redrive-streak-store
dark-factory-version: v0.198.7
created: "2026-10-23T09:27:00Z"
queued: "2026-10-23T09:42:55Z"
started: "2026-10-23T09:42:56Z"
completed: "2026-10-23T09:48:32Z"
branch: dark-factory/bug-redrive-streak-lost-across-clusters-and-restarts
---

<summary>
- A task's consecutive-spawn count can now be kept in the shared claim store instead of one pod's memory, so any executor in any cluster reads the same count.
- Each recorded spawn adds exactly one entry; a create that loses a race is retried on the next free number, at most three times, and any other error stops the record immediately.
- Recording a spawn at a new phase removes the task's entries at every other phase, which resets the count on a phase change.
- Clearing a task removes all of its entries, which is what a terminal state does.
- Foreign or malformed entries are ignored when counting, recording and resetting, so they can neither cap nor block a task.
- Task ids and phases are validated before any request is sent; an invalid value never reaches the store.
- The behaviour is testable entirely in-process; no new transport, URL or secret is introduced.
- Nothing in the handler reads the new store yet — this step only ships the store and its tests.
</summary>

<objective>
Provide a persisted per-task, per-phase spawn counter that survives restarts and is shared across clusters, with operations to count, record and clear, and with deletion of an already-gone entry treated as success. Cover it with specs for counting across independent instances, the exact stored shape, the race retry, the phase reset, the terminal clear, foreign-entry handling and input validation. The handler and lease manager are not changed.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for the container agent's execution rules (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, external test packages, coverage ≥80% for new code, BSD license header on every Go file). This repo has no project-level `CLAUDE.md`.

Read these files fully before changing anything:
- `/workspace/pkg/claim/ref_client.go` — the `ClaimRefClient` interface (`List(ctx, prefix)`, `Put(ctx, key, expectedSha, cluster, taskID, leaseUntil)`, `Delete(ctx, key, expectedSha)`), the `ClaimRef` struct (`Ref` is the full ref name `refs/claims/...`, `Sha`, `Cluster`, `TaskID`, `LeaseUntil`), the `ErrLeaseConflict` sentinel returned on a 409, `RequestTimeout`, and `escapedKey`. Mirror this file's error-wrapping style and V(3) request logging.
- `/workspace/pkg/claim/key.go` — `validateClaimComponent(ctx, name, value)` is the single authority for what may appear in a claim key (rejects empty, `/`, `..`, a leading `.`, a trailing `.lock`, and any rune outside `[A-Za-z0-9._-]`). It is unexported; the store lives in the same package and must call it.
- `/workspace/pkg/claim/fake_git_rest_test.go` — the in-process fake. `newFakeGitRest()`, `Seed(ref, sha, cluster, taskID, leaseUntil)` takes a FULL ref (`refs/claims/...`), `Ref(ref)`, `Requests()`, `SetFault(func(fakeRequest) *fakeFault)`, `fakeFault{Status, Body, Apply}`. A PUT with an empty `expected_sha` answers 409 when the ref exists; a DELETE whose `expected_sha` does not match answers 409. `list` matches `prefix` as a whole path component and returns refs sorted.
- `/workspace/pkg/claim/lease_manager_test_helpers_test.go` — `newTestClock(t0 libtime.DateTime)`, `rfc3339(at libtime.DateTime)`, `putExpectedSha(request fakeRequest)`, `requestsMatching(requests, method, part)`, `leaseTestT0`.
- `/workspace/pkg/claim/claim_suite_test.go` — the package's single Ginkgo entry point (`RunSpecs(t, "Claim Suite")`). Do not add a second suite.
- `/workspace/pkg/claim/lease_manager_recovery_test.go` — `faultAllPuts(status, apply)` and `faultPut(...)` show the established fault-injection idiom.

Library/symbol facts already verified, do not re-derive:
- `github.com/bborbe/time` exposes `Duration` constants `libtime.Second`, `libtime.Minute`, `libtime.Hour`, `libtime.Day`; `libtime.Hour` is a typed constant, so `const RedriveStreakLeaseDuration = 24 * libtime.Hour` is valid Go.
- `libtime.DateTime.Add(duration HasDuration) DateTime` accepts a `libtime.Duration`.
- `libtime.CurrentDateTimeGetter` is the interface with `Now() DateTime`; the handler already injects one.
- `github.com/bborbe/errors` provides `errors.Wrapf(ctx, err, format, args...)` and `errors.Errorf(ctx, format, args...)`; use `stderrors "errors"` plus `stderrors.Is(err, sentinel)` for sentinel checks, exactly as `ref_client.go` does.
- The module path is `github.com/bborbe/agent-task-executor`; the test package imports `"github.com/bborbe/agent-task-executor/pkg/claim"`.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md` — public interface + private struct + `New*` constructor.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `bborbe/errors` API, sentinel errors with a `stderrors` alias.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2, external `_test` package, coverage ≥80% for new code.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new code.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-precommit.md` — golines 100-char limit, funlen 80, license headers.
</context>

<requirements>
1. Add one sentinel to `/workspace/pkg/claim/ref_client.go`. Next to `ErrLeaseConflict`, add:
   ```go
   // ErrRefNotFound is returned when git-rest answers 404 on a DELETE. It is
   // final: the ref is already gone.
   var ErrRefNotFound = stderrors.New("ref not found")
   ```
   In `Delete`'s status switch, add a case before the default:
   ```go
   case http.StatusNotFound:
       return errors.Wrapf(ctx, ErrRefNotFound, "DELETE refs %s returned 404", key)
   ```
   This is additive: every existing caller still receives a non-nil error, so lease-manager behaviour is unchanged. Do not change `Put`, `List`, `RequestTimeout`, or the auth headers.

2. Create `/workspace/pkg/claim/redrive_streak_store.go` (new file, `package claim`) with the BSD license header copied from the other files in the package. Its imports are `context`, `stderrors "errors"`, `strconv`, `strings`, `github.com/bborbe/errors`, `libtime "github.com/bborbe/time"`, and `github.com/golang/glog`. It contains exactly these constants:
   ```go
   const (
       // RedriveStreakCluster is the sentinel cluster field on every streak ref.
       // git-rest refuses an empty cluster, and this value must never equal a real
       // executor cluster name: the lease manager's recovery and fence compare on
       // the cluster field.
       RedriveStreakCluster = "redrive-streak"
       // RedriveStreakPrefix is the frozen key prefix of every streak ref.
       // Changing it strands every existing ref.
       RedriveStreakPrefix = "claims/redrive/"
       // RedriveStreakPhaseNone is the phase component used when a task carries
       // no phase.
       RedriveStreakPhaseNone = "none"
       // RedriveStreakLeaseDuration is the informational lease of a streak ref.
       // Nothing renews or expires it.
       RedriveStreakLeaseDuration = 24 * libtime.Hour
       // redriveStreakMaxCreateAttempts bounds the create retries on a 409.
       redriveStreakMaxCreateAttempts = 3
   )
   ```

3. In the same file, add the counterfeiter directive and the interface:
   ```go
   //counterfeiter:generate -o ../../mocks/redrive_streak_store.go --fake-name FakeRedriveStreakStore . RedriveStreakStore

   // RedriveStreakStore persists the consecutive-spawn streak of a task per phase
   // in the shared git-rest claim store, so every cluster and every restarted pod
   // reads the same count.
   type RedriveStreakStore interface {
       // Count returns how many streak refs taskID has at phase. Refs at other
       // phases and refs whose key does not parse are not counted.
       Count(ctx context.Context, taskID string, phase string) (int, error)
       // Record appends one spawn of taskID at phase, then deletes the task's
       // refs at every other phase. A 409 create is retried on the next free
       // number at most redriveStreakMaxCreateAttempts times.
       Record(ctx context.Context, taskID string, phase string) error
       // Clear removes every parsed streak ref of taskID, at every phase.
       Clear(ctx context.Context, taskID string) error
   }
   ```
   and the constructor:
   ```go
   // NewRedriveStreakStore creates a RedriveStreakStore over refClient. The
   // injected clock supplies lease_until; no wall clock is read.
   func NewRedriveStreakStore(
       refClient ClaimRefClient,
       currentDateTime libtime.CurrentDateTimeGetter,
   ) RedriveStreakStore {
       return &redriveStreakStore{refClient: refClient, currentDateTime: currentDateTime}
   }
   ```
   The private struct is `redriveStreakStore{refClient ClaimRefClient; currentDateTime libtime.CurrentDateTimeGetter}`.

4. Implement phase mapping and key parsing as package-level helpers in the same file:
   ```go
   // redriveStreakPhaseComponent maps an empty phase to the frozen "none"
   // component; every other value is used verbatim.
   func redriveStreakPhaseComponent(phase string) string {
       if phase == "" {
           return RedriveStreakPhaseNone
       }
       return phase
   }

   // parseRedriveStreakRef parses a full ref ("refs/claims/redrive/<taskid>/<phase>/<n>").
   // ok is false for any key that is not exactly five segments under the frozen
   // prefix, or whose last segment is not an integer >= 1.
   func parseRedriveStreakRef(ref string) (taskID string, phase string, n int, ok bool) {
       key := strings.TrimPrefix(ref, "refs/")
       parts := strings.Split(key, "/")
       if len(parts) != 5 {
           return "", "", 0, false
       }
       if parts[0] != "claims" || parts[1] != "redrive" {
           return "", "", 0, false
       }
       parsed, err := strconv.Atoi(parts[4])
       if err != nil || parsed < 1 {
           return "", "", 0, false
       }
       return parts[2], parts[3], parsed, true
   }
   ```

5. Implement `Count` exactly as follows:
   - Validate `taskID` with `validateClaimComponent(ctx, "task id", taskID)`; return the error on failure and send no request.
   - Map the phase with `redriveStreakPhaseComponent`, then validate it with `validateClaimComponent(ctx, "phase", component)`; return the error on failure and send no request.
   - List with prefix `RedriveStreakPrefix + taskID`; wrap a list error as `errors.Wrapf(ctx, err, "list redrive streak refs for task %s", taskID)`.
   - Return the number of listed refs where `parseRedriveStreakRef` succeeds, the parsed task id equals `taskID`, and the parsed phase equals the component.

6. Implement `Record` exactly as follows:
   - Validate the task id and phase component as in requirement 5, before any request.
   - List once. Compute `leaseUntil := s.currentDateTime.Now().Add(RedriveStreakLeaseDuration)`.
   - Loop `attempt := 1; attempt <= redriveStreakMaxCreateAttempts; attempt++`:
     - Compute `next` = one more than the highest parsed `n` for this task and phase component in the current listing (0 when none), then `key := RedriveStreakPrefix + taskID + "/" + component + "/" + strconv.Itoa(next)`.
     - `Put(ctx, key, "", RedriveStreakCluster, taskID, leaseUntil)` — the expected sha MUST be the empty string.
     - On success, delete the other-phase refs (below) and return nil (deletes never return an error).
     - If the error is not `ErrLeaseConflict`, return `errors.Wrapf(ctx, putErr, "record redrive streak for task %s at phase %s", taskID, component)` with no retry and no deletes.
     - If it is `ErrLeaseConflict` and this was the third attempt, return `errors.Wrapf(ctx, putErr, "record redrive streak for task %s at phase %s after %d attempts", taskID, component, redriveStreakMaxCreateAttempts)`.
     - Otherwise re-list and retry with the recomputed `next`; a re-list error ends the record with that error wrapped and sends no further PUT.
   - Deleting other phases: iterate the latest listing; for each ref that parses, belongs to `taskID`, and whose parsed phase differs from the component, call `Delete(ctx, strings.TrimPrefix(ref.Ref, "refs/"), ref.Sha)`. Ignore `ErrLeaseConflict` and `ErrRefNotFound`. For any other error, log `glog.Warningf("event=redrive_streak_write_failed task=%s key=%s error=%v", taskID, deleteKey, err)` where `deleteKey` is the key passed to `Delete` and continue. Never return a delete error.

7. Implement `Clear` as follows: validate the task id as in requirement 5; list once; delete every ref that parses and belongs to `taskID`, with the same ignore-and-warn rule as requirement 6. Return the list error, if any; otherwise nil. A ref that does not parse, or belongs to another task, is never deleted.

8. Create `/workspace/pkg/claim/redrive_streak_store_test.go` (`package claim_test`) with Ginkgo specs. Build a store with `claim.NewRedriveStreakStore(claim.NewClaimRefClient(fake.URL(), "", ""), newTestClock(leaseTestT0))`; seed with `fake.Seed("refs/claims/redrive/...", sha, cluster, taskID, leaseUntil)`. Cover:
   - AC1(a): seed `refs/claims/redrive/T/ai_review/1` and `.../2`; a freshly constructed store returns `Count(ctx, "T", "ai_review") == 2` and `Count(ctx, "T", "planning") == 0`.
   - AC1(b): store A records `U` at `ai_review` twice; a separately constructed store B over the same fake returns `Count(ctx, "U", "ai_review") == 2`.
   - AC2: after one `Record(ctx, "T", "ai_review")`, `fake.Ref("refs/claims/redrive/T/ai_review/1")` exists with `Cluster == "redrive-streak"`, `TaskID == "T"` and `LeaseUntil == rfc3339(leaseTestT0.Add(24*libtime.Hour))`; every PUT in `fake.Requests()` has `putExpectedSha(request) == ""`.
   - AC3: a fault that answers the first PUT with `&fakeFault{Status: http.StatusConflict, Apply: true}` (the fake writes `…/ai_review/1`, then answers 409 — a peer that won the race) and lets later PUTs through results in a successful `Record` whose second PUT path ends in `claims/redrive/T/ai_review/2`, with both `…/1` and `…/2` present afterwards; a fault answering 409 on every PUT yields exactly 3 PUT requests and a non-nil `Record` error.
   - AC4(a): record `T` at `ai_review` twice then at `planning` once; only `refs/claims/redrive/T/planning/1` remains under the task prefix.
   - AC4(b): `Clear(ctx, "T")` leaves no ref under `refs/claims/redrive/T/`.
   - AC4(c): a seeded ref of another task (`refs/claims/redrive/OTHER/ai_review/1`) is untouched by both the reset in (a) and the clear in (b).
   - AC4(d): a seeded `refs/claims/redrive/T/ai_review/x` is not counted (`Count` returns 0), survives a `Record`, and does not become the base of the new number (the record lands at `.../ai_review/1`).
   - AC5: `Count`, `Record` and `Clear` with task id `"bad/id"`, and `Count`/`Record` with phase `"ai/review"`, each return an error and leave `fake.Requests()` empty.
   - DB1: `Record(ctx, "T", "")` creates `refs/claims/redrive/T/none/1` and `Count(ctx, "T", "") == 1`.
   - DB1 non-409: a fault answering 500 on every PUT yields exactly 1 PUT request and a non-nil `Record` error.
   - DB2 not-found: a fault answering 404 on every DELETE makes `Clear` return nil and leave the ref present (the not-found is ignored).
   - DB2 other error: a fault answering 500 on every DELETE makes `Record` after a phase change, and `Clear`, return nil and log `event=redrive_streak_write_failed`; every remaining delete is still attempted. Capture the log with a test helper copied from `/workspace/pkg/handler/task_view_test.go` `captureStderr`; `pkg/claim` has none yet.
   - A list failure (fault answering 500 on GET) makes `Count`, `Record` and `Clear` return an error; `Record` sends no PUT and `Clear` sends no DELETE.

9. Do not add a reaper, a metrics counter, a config knob, or a retry for non-409 errors. Do not touch `pkg/handler`, `pkg/factory`, `main.go`, `docs/`, or `CHANGELOG.md` in this prompt.

10. Before finishing, re-run every `<verification>` command and confirm each passes; walk each acceptance criterion listed above against the change.
</requirements>

<constraints>
- Claim protocol invariants in `docs/claim-lease-protocol.md` are unchanged for dispatch claims; this prompt touches no dispatch key, lease constant, or fence/takeover behaviour.
- git-rest accepts keys only under `claims/` and refuses an empty cluster field — hence `claims/redrive/` and the `redrive-streak` sentinel. Both values are frozen: changing either strands existing refs.
- No new git-rest client, URL or secret; the store uses the injected `ClaimRefClient` and inherits its 30 s request cap.
- Do NOT store the streak in task frontmatter (agent pods clobber it), do NOT change the cap value (3), and do NOT add a config knob or opt-out.
- Do NOT add new metrics. `executor_redrive_capped_total` is unchanged; this store emits only warning log lines.
- Create retries are capped at 3 attempts; no request is ever unbounded.
- Repo conventions: Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, counterfeiter annotations on new interfaces, injected clock for `lease_until` (no wall-clock reads in tests), license headers, golines 100-char limit.
- New code needs ≥80% statement coverage; every error path above must have a spec.
- Do NOT commit — dark-factory handles git.
- All existing tests in `pkg/claim` must still pass without weakened assertions.
</constraints>

<verification>
Run `go test ./pkg/claim/...` — must exit 0 and include the new store specs.

Run `ROOTDIR=/workspace make precommit` once at the end — must exit 0.

Confirm the new file exists and the mock directive is present:
`grep -n 'claims/redrive/' pkg/claim/redrive_streak_store.go`
`grep -n 'counterfeiter:generate' pkg/claim/redrive_streak_store.go`
</verification>
