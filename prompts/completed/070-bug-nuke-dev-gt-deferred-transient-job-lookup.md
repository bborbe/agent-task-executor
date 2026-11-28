---
status: completed
spec: [bug-nuke-dev-gt]
summary: Classified transient kube-apiserver failures in the deferred-respawn loop so they are logged and skipped instead of restarting the executor, with tick-level defence in depth and no Kafka-path or metric changes
execution_id: 1a74ff56-b75c-5753-a4f8-38edd2ce7947-exec-070-bug-nuke-dev-gt-deferred-transient-job-lookup
dark-factory-version: dev
created: "2026-11-28T00:40:00Z"
queued: "2026-11-28T01:02:10Z"
started: "2026-11-28T01:02:11Z"
completed: "2026-11-28T01:18:33Z"
---

# fix: a transient kube-apiserver failure must not restart the executor

<summary>
- A transient kube-apiserver failure during a deferred-respawn evaluation no longer crashes the executor pod.
- Such failures — 5xx, 429, request/server timeouts, connection errors, context deadlines — are logged and the affected task is skipped for that tick.
- A denied read (401/403) still suppresses the spawn exactly as before, with unchanged log line and metric.
- A genuine, non-transient fault (e.g. a 404 or an unexpected error) still surfaces from the deferred evaluation, so a real defect is not silently swallowed.
- A failed deferred-respawn tick can no longer end the run loop: it is logged and the loop continues; a cancelled context still stops the loop cleanly.
- The Kafka event path's error handling is unchanged — this fix targets the deferred-respawn loop only.
- The skipped task is re-driven by the reconcile floor on a later pass, with no operator action.
- No config field, flag, interface signature, mock, or new metric family changes.
</summary>

<objective>
Make the deferred-respawn loop tolerate a transient kube-apiserver failure. Today a 503 from an active-job read during `evalDeferredRespawns` is a plain error, so `evalDeferredRespawns` returns it, `RunDeferredRespawnLoop` returns the tick error, `service.Run` ends and the executor pod restarts (Sentry NUKE-DEV-GT). Classify transient API-server failures as a distinct non-fatal job-lookup error and log-and-continue on them in `evalDeferredRespawns`; add defence-in-depth log-and-continue on non-context tick errors in `RunDeferredRespawnLoop`.
</objective>

<context>
Read `CLAUDE.md` if present (none exists in this repo) and `README.md` for conventions. Every file in `pkg/handler` starts with the BSD license header:
```go
// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
```

Read these files in full before changing anything:

- `pkg/handler/task_event_handler_deferred.go`
  - `evalDeferredRespawns(ctx) error` collects due entries, then calls `spawnIfNeededClaimed` per entry. Its current `if err != nil` block already carves out two typed errors (`*triggerRefusalWriteError`, `*spawnJobError`) with `stderrors.As`, logging + counting + `clearQueuedAtStampIfNotDeferred` + `continue`, and returns everything else via `errors.Wrapf(ctx, err, "deferred respawn for task %s", ...)`.
  - `RunDeferredRespawnLoop(ctx) error` runs one initial eval (fatal on error, unchanged), then a `time.Ticker` (`deferredRespawnInterval = 30 * time.Second`); its `case <-ticker.C:` currently returns `errors.Wrapf(ctx, err, "deferred respawn loop tick")` on an eval error.
- `pkg/handler/job_lookup_denied.go`
  - `isJobLookupDenied(err) bool` matches only `apierrors.IsUnauthorized || apierrors.IsForbidden`.
  - `wrapJobLookupError(ctx, err, assignee string, namespace libk8s.Namespace, format string, args ...any) error` wraps a denial into `*jobLookupDeniedError`, otherwise a plain `errors.Wrapf`.
  - `jobLookupDeniedError` is the sibling type (carries `assignee`/`namespace` for its metric).
- `pkg/handler/task_event_handler.go`
  - `spawnIfNeededClaimed` is the single spawn funnel every path uses. Its job-read error branches (`isJobActive`, `deferIfNotAdmitted`) call `h.handleJobLookupDenied(err)` and, when it returns false, propagate the error. A `*jobLookupTransientError` is NOT a denial, so it propagates exactly as a plain 503 did before — this is the desired no-regression behaviour on the Kafka path.
- `pkg/handler/task_event_handler_permanent_skip.go`
  - `classifySpawnError` / `isPermanentSpawnError` only match `apierrors.IsInvalid`/`IsBadRequest`, so a transient error is never mistaken for a permanent one.
- `pkg/handler/task_event_handler_deferred_test.go`
  - The `Describe("EvalDeferredRespawns (spec 037)")` block. Its `BeforeEach` sets `fakeSpawner.IsJobActiveReturns(false, nil)` and `fakeSpawner.SpawnJobReturns("job-deferred-1", nil)`. Helpers in scope: `currentDateTime.SetNow(libtimetest.ParseDateTime(...))`, `buildGraceTask(domain.TaskPhaseExecution, 0, 3)` (carries `current_job`, so the spawn path reads via `IsJobActive`), `buildMsg`, and constants `insideGrace`, `graceExpiredR`. `buildGraceTask` uses task id `tid-deferred-037`.
  - The existing spec `"keeps a failed refusal write non-fatal instead of exiting the loop"` is the closest model.
- `pkg/handler/index_terminal_deferred_test.go`
  - `deferredGateCaptureStderr(fn func()) string` (same `package handler_test`) sets `logtostderr=true`/`v=2`, redirects `os.Stderr`, runs `fn`, flushes glog and returns the captured output. Reuse it to assert the new log line; do not copy it.
- `pkg/handler/job_lookup_denied_internal_test.go` — the `package handler` internal-test style (plain `testing`, not Ginkgo).
- `pkg/handler/claim_spawn_order_internal_test.go` — shows the internal fakes; note the comment that internal tests cannot import `mocks` (it imports `pkg/handler`).

Coding-plugin guides (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `github.com/bborbe/errors` wrapping and `stderrors.As` chains.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md` — `glog.Warningf` and the `event=` key=value style.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo v2/Gomega (external suite) and plain `testing` (internal suite).
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-concurrency-patterns.md` — the `for { select { case <-ctx.Done() ... case <-ticker.C ... } }` loop shape.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-linting-guide.md` — `funlen` (lines 80 / statements 50) and `gocognit` (min-complexity 20); this is why the carve-outs are extracted into helpers.

Verified facts (do not re-derive):
- In `k8s.io/apimachinery@v0.38.0-beta.0/pkg/api/errors/errors.go` these all exist and support wrapped errors: `IsServiceUnavailable`, `IsTimeout` (504 / `StatusReasonTimeout`), `IsServerTimeout` (`StatusReasonServerTimeout`), `IsTooManyRequests` (429), `IsInternalError` (500), `IsUnauthorized`, `IsForbidden`, `IsNotFound`. There is no `NewTimeout` constructor — build a 504 with `&apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Reason: metav1.StatusReasonTimeout, Code: http.StatusGatewayTimeout, Message: "timeout"}}`.
- `libk8s.Namespace` is `type Namespace string` (`github.com/bborbe/k8s@v1.14.33/k8s_namespace.go`), so `libk8s.Namespace("dev")` is a valid conversion.
- `context.DeadlineExceeded` implements `net.Error`; checking it explicitly is still wanted so the intent is legible.
- `stderrors.As` matches `*jobLookupTransientError` through the `errors.Wrapf` chain because `github.com/bborbe/errors` errors implement `Unwrap` (the existing `*spawnJobError` carve-out relies on this).
- `evalDeferredRespawns` is ~73 non-blank non-comment lines against `funlen.lines: 80`; adding a fourth inline branch risks the limit, which is why the carve-outs are extracted into a helper.
- The existing test `"RunDeferredRespawnLoop returns error when evalDeferredRespawns fails on ticker tick"` drives the INITIAL eval, not the tick: the clock is already past grace when the loop starts, so the initial eval returns the (plain) publish error immediately. It stays green when only the tick branch changes. Do not modify it.
- A 401/403 in the deferred path is consumed inside `spawnIfNeededClaimed` (`handleJobLookupDenied` returns true → suppressed, nil), so `evalDeferredRespawns` never sees a denied error. Do not add a denied carve-out to the deferred helper.
- The spec says "log-and-continue": the new transient and tick carve-outs are LOG ONLY. Do NOT add a new metric family or new `TaskEventsTotal` label values. The two pre-existing carve-outs keep their existing metrics unchanged.
</context>

<requirements>
1. **Add a transient classifier and error type in a new file `pkg/handler/job_lookup_transient.go`** (`package handler`, BSD license header). Use exactly this content:

   ```go
   // jobLookupTransientError marks a failed active-job read that may succeed on a
   // later attempt: a 5xx, a 429, a timeout, a connection error, or a context
   // deadline. It is the transient sibling of jobLookupDeniedError.
   //
   // Unlike a denial it is worth retrying, but retrying on the next
   // deferred-respawn tick (or the next reconcile pass) is enough. It is a distinct
   // type so evalDeferredRespawns can log and skip the entry instead of returning
   // the error, which would end RunDeferredRespawnLoop and restart the executor pod
   // (Sentry NUKE-DEV-GT: a kube-apiserver 503 during a deferred tick).
   type jobLookupTransientError struct {
   	err error
   }

   func (e *jobLookupTransientError) Error() string { return e.err.Error() }

   func (e *jobLookupTransientError) Unwrap() error { return e.err }

   // isJobLookupTransient reports whether err is a transient API-server failure: a
   // condition that may clear on its own without operator action. It matches the
   // apimachinery transient predicates (service-unavailable, timeout, server
   // timeout, too-many-requests, internal error), any net.Error, and a context
   // deadline. It deliberately does NOT match 401/403 (that is isJobLookupDenied),
   // a 404 (the resource resolved), or an arbitrary programming error.
   func isJobLookupTransient(err error) bool {
   	if err == nil {
   		return false
   	}
   	if apierrors.IsServiceUnavailable(err) ||
   		apierrors.IsTimeout(err) ||
   		apierrors.IsServerTimeout(err) ||
   		apierrors.IsTooManyRequests(err) ||
   		apierrors.IsInternalError(err) {
   		return true
   	}
   	if stderrors.Is(err, context.DeadlineExceeded) {
   		return true
   	}
   	var netErr net.Error
   	return stderrors.As(err, &netErr)
   }
   ```

   Imports for this file: `context`, `stderrors "errors"`, `net`, `apierrors "k8s.io/apimachinery/pkg/api/errors"`. Do NOT add `context.Canceled` — a cancelled context is not transient; the loop's tick defence (requirement 4) handles shutdown separately.

2. **Extend `wrapJobLookupError` in `pkg/handler/job_lookup_denied.go`** so a transient error gets the typed wrapper. Replace the current body (which is `if !isJobLookupDenied(err) { plain }; return denied`) with positive checks in this order: denied first, then transient, then plain.

   Old:
   ```go
   	if !isJobLookupDenied(err) {
   		return errors.Wrapf(ctx, err, format, args...)
   	}
   	return errors.Wrapf(
   		ctx,
   		&jobLookupDeniedError{err: err, assignee: assignee, namespace: namespace},
   		format,
   		args...,
   	)
   ```
   New:
   ```go
   	if isJobLookupDenied(err) {
   		return errors.Wrapf(
   			ctx,
   			&jobLookupDeniedError{err: err, assignee: assignee, namespace: namespace},
   			format,
   			args...,
   		)
   	}
   	if isJobLookupTransient(err) {
   		return errors.Wrapf(ctx, &jobLookupTransientError{err: err}, format, args...)
   	}
   	return errors.Wrapf(ctx, err, format, args...)
   ```

   Also update the two doc comments that are now stale:
   - `isJobLookupDenied`'s comment currently says "a 429 is worth retrying, and a 500 is a real fault that must stay fatal." Replace that clause with: "a 404 means the resource resolved and the grant is fine, and a 429 or 5xx is transient — see isJobLookupTransient."
   - `wrapJobLookupError`'s comment should say a denial becomes `jobLookupDeniedError`, a transient failure becomes `jobLookupTransientError`, and every other error keeps its existing wrapped form and stays fatal.

3. **Add `handleDeferredReplayError` to `pkg/handler/task_event_handler_deferred.go`, directly after `evalDeferredRespawns`, and route the replay error block through it.** Replace the current `if err != nil { ... }` block inside the `for _, replay := range ready` loop with:

   ```go
   		outcome, err := h.spawnIfNeededClaimed(ctx, entry.task, &entry.config, &replay.claim)
   		spawned := outcome.spawned()
   		if err != nil {
   			// A single failed spawn attempt must not exit the process and starve
   			// every other due entry. handleDeferredReplayError carves out the
   			// errors a bad entry may produce (a failed refusal write, a failed Job
   			// create, a transient API-server read); any other error stays fatal.
   			if h.handleDeferredReplayError(entry.task, err) {
   				continue
   			}
   			return errors.Wrapf(
   				ctx, err, "deferred respawn for task %s", entry.task.TaskIdentifier,
   			)
   		}
   ```

   Add this helper. It MUST reproduce the two existing carve-outs byte-for-byte in log text and metric label (existing specs assert them), and add the third as a log-only carve-out:

   ```go
   // handleDeferredReplayError classifies a failed spawn attempt for one deferred
   // entry and reports whether the loop must skip the entry rather than return the
   // error. The three carve-outs are the ones a single bad entry must not turn into
   // a process restart: a failed refusal write, a failed Job create, and a transient
   // API-server read failure. In every case the entry was already removed from the
   // map and is not re-parked; the reconcile floor re-drives the task.
   func (h *taskEventHandler) handleDeferredReplayError(task lib.Task, err error) bool {
   	var refusalErr *triggerRefusalWriteError
   	if stderrors.As(err, &refusalErr) {
   		glog.Warningf(
   			"event=deferred_respawn_refusal_write_failed task=%s err=%v",
   			task.TaskIdentifier, err,
   		)
   		metrics.TaskEventsTotal.WithLabelValues("deferred_respawn_refusal_write_failed").Inc()
   		h.clearQueuedAtStampIfNotDeferred(task.TaskIdentifier)
   		return true
   	}
   	var spawnErr *spawnJobError
   	if stderrors.As(err, &spawnErr) {
   		glog.Warningf(
   			"event=deferred_respawn_spawn_failed task=%s err=%v",
   			task.TaskIdentifier, err,
   		)
   		metrics.TaskEventsTotal.WithLabelValues("deferred_respawn_spawn_failed").Inc()
   		h.clearQueuedAtStampIfNotDeferred(task.TaskIdentifier)
   		return true
   	}
   	var transientErr *jobLookupTransientError
   	if stderrors.As(err, &transientErr) {
   		glog.Warningf(
   			"event=deferred_respawn_transient_error task=%s err=%v",
   			task.TaskIdentifier, err,
   		)
   		h.clearQueuedAtStampIfNotDeferred(task.TaskIdentifier)
   		return true
   	}
   	return false
   }
   ```

   The transient branch is log-only (no metric) per the spec. Keep the pre-existing metrics on the refusal and spawn branches. Do NOT change any other line of `evalDeferredRespawns` (collection, sorting, `indexGateSuppresses`, the `!spawned` skip branch, the `respawn_after_grace_window` log/metric).

4. **Add `handleDeferredTickError` to `pkg/handler/task_event_handler_deferred.go`, directly after `RunDeferredRespawnLoop`, and route the tick branch through it.** Replace the tick branch:

   Old:
   ```go
   		case <-ticker.C:
   			if err := h.evalDeferredRespawns(ctx); err != nil {
   				return errors.Wrapf(ctx, err, "deferred respawn loop tick")
   			}
   ```
   New:
   ```go
   		case <-ticker.C:
   			if err := h.evalDeferredRespawns(ctx); err != nil {
   				if handleDeferredTickError(ctx, err) {
   					return nil
   				}
   			}
   ```

   Add (log-only, no metric):
   ```go
   // handleDeferredTickError decides what a failed deferred-respawn tick does to the
   // loop. A cancelled context stops it gracefully; any other error is logged and
   // the loop continues, so one bad tick cannot end service.Run and restart the
   // executor pod. The reconcile floor re-drives whatever the failed tick skipped.
   // Defence in depth behind evalDeferredRespawns' own typed carve-outs.
   func handleDeferredTickError(ctx context.Context, err error) (stop bool) {
   	if ctx.Err() != nil {
   		return true
   	}
   	glog.Warningf("event=deferred_respawn_loop_tick_error err=%v; continuing", err)
   	return false
   }
   ```

   Leave the INITIAL eval before the ticker unchanged (`return errors.Wrapf(ctx, err, "deferred respawn loop initial eval")`). The spec scopes the defence-in-depth to tick errors only, and the existing initial-eval test must stay green.

5. **Internal classifier and tick tests: create `pkg/handler/job_lookup_transient_internal_test.go`** (`package handler`, BSD license header, plain `testing` — Ginkgo specs in an internal package are not discovered by this repo's suite). Include:
   - A `timeoutNetError` type implementing `net.Error` (`Error() string`, `Timeout() bool { return true }`, `Temporary() bool { return true }`).
   - `TestIsJobLookupTransient`: a table asserting true for `apierrors.NewServiceUnavailable(...)`, `apierrors.NewServerTimeout(schema.GroupResource{Resource: "jobs"}, "list", 1)`, a hand-built 504 `*apierrors.StatusError` (see verified facts), `apierrors.NewTooManyRequests("slow down", 1)`, `apierrors.NewInternalError(stderrors.New("boom"))`, `timeoutNetError{}`, and `context.DeadlineExceeded`; false for `nil`, `apierrors.NewUnauthorized("x")`, `apierrors.NewForbidden(schema.GroupResource{Resource: "jobs"}, "x", stderrors.New("no grant"))`, `apierrors.NewNotFound(schema.GroupResource{Resource: "jobs"}, "x")`, and `stderrors.New("boom")`.
   - `TestWrapJobLookupErrorTypesTransient`: `wrapJobLookupError(ctx, apierrors.NewServiceUnavailable("apiserver not ready"), "claude", libk8s.Namespace("dev"), "check active job")` yields a value matching `stderrors.As(err, &transient)` with `var transient *jobLookupTransientError`, and `isJobLookupDenied(err)` is false.
   - `TestWrapJobLookupErrorKeepsNotFoundPlain`: the same call with `apierrors.NewNotFound(schema.GroupResource{Resource: "jobs"}, "x")` matches neither `*jobLookupTransientError` nor `*jobLookupDeniedError`.
   - `TestHandleDeferredTickError`: with `context.Background()` and `stderrors.New("boom")` it returns false (continue); with a cancelled context (`ctx, cancel := context.WithCancel(context.Background()); cancel()`) it returns true (stop).
   - Imports: `context`, `stderrors "errors"`, `net`, `net/http`, `testing`, `libk8s "github.com/bborbe/k8s"`, `apierrors "k8s.io/apimachinery/pkg/api/errors"`, `metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"`, `"k8s.io/apimachinery/pkg/runtime/schema"`. Do not redefine fakes that already exist in sibling internal tests.

6. **External deferred-loop specs: add two `It` blocks inside `Describe("EvalDeferredRespawns (spec 037)")` in `pkg/handler/task_event_handler_deferred_test.go`.** Add imports `apierrors "k8s.io/apimachinery/pkg/api/errors"` and `"k8s.io/apimachinery/pkg/runtime/schema"` to that file.
   - `It("skips a deferred entry whose job lookup fails transiently instead of returning the error", ...)`: park at `insideGrace`, then make the API server transiently unavailable and assert the loop skips it and logs. This fails on the pre-fix code (the 503 would make `EvalDeferredRespawns` return the error).
     ```go
     	currentDateTime.SetNow(libtimetest.ParseDateTime(insideGrace))
     	task := buildGraceTask(domain.TaskPhaseExecution, 0, 3)
     	Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(Succeed())
     	Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

     	// The API server is now transiently unavailable. Before the fix this 503
     	// propagated out of evalDeferredRespawns, RunDeferredRespawnLoop returned the
     	// tick error, service.Run ended and the executor pod restarted (NUKE-DEV-GT).
     	fakeSpawner.IsJobActiveReturns(false, apierrors.NewServiceUnavailable("apiserver not ready"))
     	currentDateTime.SetNow(libtimetest.ParseDateTime(graceExpiredR))

     	var evalErr error
     	out := deferredGateCaptureStderr(func() { evalErr = h.EvalDeferredRespawns(ctx) })
     	Expect(evalErr).To(BeNil())
     	Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))
     	Expect(out).To(ContainSubstring("event=deferred_respawn_transient_error"))
     	Expect(out).To(ContainSubstring("task=tid-deferred-037"))
     ```
   - `It("still returns a non-transient job lookup error from evalDeferredRespawns", ...)`: park the same way at `insideGrace`, then set `fakeSpawner.IsJobActiveReturns(false, apierrors.NewNotFound(schema.GroupResource{Resource: "jobs"}, "missing"))`, advance to `graceExpiredR`, and assert `Expect(h.EvalDeferredRespawns(ctx)).NotTo(Succeed())`. This is the negative control: a 404 is neither a denial nor transient and must stay fatal.

7. **Leave the Kafka path alone.** Do not change `ConsumeMessage`, `spawnIfNeeded`, `spawnIfNeededClaimed`, `classifySpawnError`, or `handleJobLookupDenied`. A `*jobLookupTransientError` is not a denial, so it propagates through `spawnIfNeededClaimed` exactly as a plain 503 did before. The existing `DescribeTable("still fails the message for a non-identity read failure", ...)` in `pkg/handler/task_event_handler_job_lookup_denied_test.go` (server error, not found, rate limited) must remain green unchanged.

8. **Before finishing**, run every command in `<verification>`. Walk each numbered requirement against the diff. Confirm the existing specs `"keeps a failed refusal write non-fatal instead of exiting the loop"`, `"skips a deferred entry whose SpawnJob fails and spawns the next one"`, and `"RunDeferredRespawnLoop returns error when evalDeferredRespawns fails on ticker tick"` still pass without edits.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- No new Config field, CRD knob, env var, feature flag, interface signature change, Counterfeiter mock regeneration, metric family, or new `TaskEventsTotal` label value. `wrapJobLookupError`, `isJobLookupDenied`, and the handler interfaces are unexported or unchanged in signature.
- The spec says "log-and-continue": the transient and tick carve-outs are log only. Keep the two pre-existing carve-outs' metrics untouched.
- The transient carve-out is deferred-respawn-loop only. The Kafka `ConsumeMessage` path must keep surfacing a transient job-lookup error (the consumer cancels every run func); that is out of scope for this spec and a separate follow-up, not part of this change.
- Do NOT add a denied carve-out to `handleDeferredReplayError`: a 401/403 is consumed inside `spawnIfNeededClaimed` and never reaches `evalDeferredRespawns`.
- Preserve the exact existing log text and metric label for the refusal-write and spawn-failed carve-outs; existing specs assert them.
- Keep the INITIAL eval before the ticker fatal. Only the `case <-ticker.C:` branch becomes log-and-continue.
- The new `handleDeferredReplayError` must keep `evalDeferredRespawns` within `funlen` (80 lines / 50 statements) and `gocognit` (20) — that is why the carve-outs move into a helper rather than a fourth inline branch.
- Use `github.com/bborbe/errors` (`errors.Wrapf`) for error wrapping, never `fmt.Errorf` or `github.com/pkg/errors`.
- Tests: external suite uses Ginkgo v2/Gomega; internal suite uses plain `testing` (an internal-package Ginkgo spec is not discovered here). Do not import `mocks` from a `package handler` internal test (it imports `pkg/handler`). Reuse `deferredGateCaptureStderr` for the log assertion; do not copy it.
- Do NOT modify the existing test `"RunDeferredRespawnLoop returns error when evalDeferredRespawns fails on ticker tick"` — it drives the initial eval, which stays fatal, so it passes unchanged.
- Existing tests must still pass.
</constraints>

<verification>
Run `make test` — must pass, including the new internal tests and the two new external specs.

Run `make precommit` — must pass. In particular `gocognit`, `funlen`, `nestif`, `dupl`, and revive's `file-length-limit` (2000) must stay clean.

Run `go test ./pkg/handler/ -run 'TestIsJobLookupTransient|TestWrapJobLookupError|TestHandleDeferredTickError' -v` — must pass.

Run `go test ./pkg/handler/ -run TestHandler -args -ginkgo.focus='transient'` — must pass (covers the two new deferred specs).

Run `grep -q 'event=deferred_respawn_transient_error' pkg/handler/task_event_handler_deferred.go && grep -q 'event=deferred_respawn_loop_tick_error' pkg/handler/task_event_handler_deferred.go && echo LOG_OK` — must print `LOG_OK`.

Run `grep -q 'jobLookupTransientError' pkg/handler/job_lookup_denied.go && echo WIRED_OK` — must print `WIRED_OK`.

Run `test -z "$(grep -n 'deferred_respawn_transient_error\|deferred_respawn_loop_tick_error' pkg/metrics/metrics.go)" && echo NO_METRIC_OK` — must print `NO_METRIC_OK` (the new carve-outs add no metrics; neither label appears in metrics.go).
</verification>
