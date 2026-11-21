---
status: completed
spec: [bug-nuke-dev-h2]
summary: Made the token-ceiling gate fail open on vault-api read errors and the deferred-respawn loop skip surfaced vault-read errors, with seeded metrics, regression tests, and a changelog entry.
execution_id: 6837f8c6-8d35-590d-95f2-c70ed53aa31a-exec-069-spec-bug-nuke-dev-h2-token-ceiling-fail-open
dark-factory-version: dev
created: "2026-11-21T02:35:00Z"
queued: "2026-11-21T02:42:39Z"
started: "2026-11-21T02:42:40Z"
completed: "2026-11-21T03:08:12Z"
---

<summary>
- A transient vault-API read failure in the token-ceiling check no longer terminates the executor; the spawn proceeds (fail open), matching the repo's documented index-read contract.
- The read failure is logged under a distinct `token_ceiling_read_error` event and counted under its own `result` so an operator can alert on it.
- The deferred-respawn loop now treats a surfaced vault-read failure the same way it treats a failed Job spawn: it logs, counts, and skips that entry instead of exiting the process.
- Both new outcome labels are pre-registered at zero, so a "this never fired" alert reading is honest rather than absent.
- Regression tests cover the gate failing open on both the assignee-list read and the per-document read.
- An end-to-end regression test proves the deferred-respawn loop survives a vault-read error — the exact Sentry `NUKE-DEV-H2` failure path.
- Nothing changes when vault reads succeed: the ceiling still defers over-budget agents, and the existing tests still pass.
</summary>

<objective>
Fix Sentry issue `NUKE-DEV-H2`: a transient vault-api dial failure is currently returned unmodified by the best-effort token-ceiling gate (`tokenSpendSince` -> `deferIfOverTokenCeiling`) and propagates through the deferred-respawn spawn funnel to a fatal `run.Func` error that terminates the executor process. Make the gate fail open on vault-api read errors, and make the deferred-respawn loop treat a surfaced vault-read error as non-fatal (skip) exactly as it already treats `spawnJobError`. The end state: a transient vault read can never terminate the process, and it is observable via a log event and a counter.
</objective>

<context>
This repo is a Go service. There is no CLAUDE.md; follow the existing package conventions and the referenced guides.

Read these files before changing anything (paths are repo-root-relative):
- `pkg/handler/token_ceiling.go` — the gate and `tokenSpendSince` (the bug site).
- `pkg/handler/task_event_handler_deferred.go` — `evalDeferredRespawns`, which already carves `spawnJobError` and `triggerRefusalWriteError` out of the fatal path (mirror that pattern).
- `pkg/handler/task_event_handler.go` — the `spawnJobError` type and `spawnIfNeededClaimed` funnel (around the `spawnJobError` definition and the `deferIfNotAdmitted` call).
- `pkg/handler/concurrency_admission.go` — `deferIfNotAdmitted`, the caller of `deferIfOverTokenCeiling`.
- `pkg/handler/job_lookup_denied.go` — the read-path sibling of `spawnJobError`; shows the established "typed error -> non-fatal" convention.
- `pkg/metrics/metrics.go` — `TaskEventsTotal` and the `init()` / `seedDeferredAndRefusalOutcomes()` seed blocks.
- `pkg/claim/vault_api_index_client.go` — `TokenSpendReader`, `ReadForAssignee`, `ReadDocument` and the error shapes they return.
- `pkg/handler/token_ceiling_internal_test.go` — existing internal tests (package `handler`).
- `pkg/handler/task_event_handler_deferred_test.go` — existing Ginkgo suite (package `handler_test`), especially the `EvalDeferredRespawns (spec 037)` Describe block and its `spawnJobError` test.
- `pkg/handler/grace_clock_test.go` — `graceAnchor` / `graceOffset`, used by the deferred tests.

Reference these coding-plugin guides (read them inside the execution container at the given paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-prometheus-metrics-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`

Project domain doc:
- `docs/executor-metrics-contract.md` — the "closed result set / seed at zero" rule the two new `TaskEventsTotal` result labels must follow.
</context>

<requirements>
## Part A — Token-ceiling gate fails open

1. In `pkg/handler/token_ceiling.go`, add a typed error near `tokenSpendSince`:

```go
// vaultReadError marks a failed vault-api read in the token-ceiling gate. The
// gate is best-effort: a read failure fails open (the spawn proceeds) rather
// than failing the run, and the deferred-respawn loop treats a surfaced
// vaultReadError like spawnJobError instead of exiting the process.
type vaultReadError struct {
	err error
}

func (e *vaultReadError) Error() string { return e.err.Error() }

func (e *vaultReadError) Unwrap() error { return e.err }
```

2. Change `tokenSpendSince` so both read failures are wrapped in `*vaultReadError` (add `"github.com/bborbe/errors"` to the imports). The current body is:

```go
	items, err := h.tokenSpendReader.ReadForAssignee(ctx, assignee)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, item := range items {
		doc, err := h.tokenSpendReader.ReadDocument(ctx, item.ID)
		if err != nil {
			return 0, err
		}
		total += sumRunTokensSince(doc.Frontmatter, since)
	}
	return total, nil
```

Change it to:

```go
	items, err := h.tokenSpendReader.ReadForAssignee(ctx, assignee)
	if err != nil {
		return 0, &vaultReadError{
			err: errors.Wrapf(ctx, err, "read token spend for assignee %s", assignee),
		}
	}
	var total int64
	for _, item := range items {
		doc, err := h.tokenSpendReader.ReadDocument(ctx, item.ID)
		if err != nil {
			return 0, &vaultReadError{
				err: errors.Wrapf(ctx, err, "read token spend document %s", item.ID),
			}
		}
		total += sumRunTokensSince(doc.Frontmatter, since)
	}
	return total, nil
```

Update the `tokenSpendSince` doc comment to state that read failures are returned as `*vaultReadError`.

3. In `deferIfOverTokenCeiling`, replace the current error branch:

```go
	spent, err := h.tokenSpendSince(ctx, config.Assignee, since)
	if err != nil {
		return false, err
	}
```

with a fail-open branch:

```go
	spent, err := h.tokenSpendSince(ctx, config.Assignee, since)
	if err != nil {
		// Fail open: a transient vault-api read failure is best-effort and must
		// not defer the spawn nor fail the run. Returning the error here reaches
		// the deferred-respawn loop as a fatal run.Func error and terminates the
		// process (Sentry NUKE-DEV-H2). Treat an unreadable spend as "not over
		// budget" so the spawn proceeds, exactly as a failed index read does.
		glog.Warningf(
			"event=token_ceiling_read_error task=%s assignee=%s err=%v",
			task.TaskIdentifier, config.Assignee, err,
		)
		metrics.TaskEventsTotal.WithLabelValues("token_ceiling_read_error").Inc()
		return false, nil
	}
```

`glog` and `metrics` are already imported in this file. Do not change the gate's other short-circuits or the deferral logic.

## Part B — Deferred-respawn loop treats a surfaced vault-read error as non-fatal

<!-- REVIEWER NOTE: The spec's two clauses overlap by construction. Part A makes the gate
     swallow the token-ceiling read error, so on today's code path no *vaultReadError
     escapes the funnel into the loop. Part B is therefore defense-in-depth: it guarantees
     that any vault read which surfaces a *vaultReadError up the spawn funnel in future is
     non-fatal, exactly as spawnJobError already is. It is kept because the spec explicitly
     asks the loop to treat transient vault-read errors like spawnJobError. The end-to-end
     test in requirement 7 exercises the observable contract (EvalDeferredRespawns returns
     nil on a vault-read failure); the branch itself mirrors the tested spawnJobError branch. -->

4. In `pkg/handler/task_event_handler_deferred.go`, inside `evalDeferredRespawns`, the `if err != nil` block after `spawnIfNeededClaimed` already carves out `*triggerRefusalWriteError` and `*spawnJobError`. Add a third carve-out immediately after the `*spawnJobError` block (before the final `return errors.Wrapf(...)`):

```go
			var vaultErr *vaultReadError
			if stderrors.As(err, &vaultErr) {
				glog.Warningf(
					"event=deferred_respawn_vault_read_failed task=%s err=%v",
					entry.task.TaskIdentifier, err,
				)
				metrics.TaskEventsTotal.WithLabelValues("deferred_respawn_vault_read_failed").
					Inc()
				// The entry was removed during collection and no spawn replaced
				// it; the reconcile floor re-drives the task. Mirror spawnJobError:
				// skip, do not re-park.
				h.clearQueuedAtStampIfNotDeferred(entry.task.TaskIdentifier)
				continue
			}
```

`stderrors` (aliased stdlib `errors`), `glog` and `metrics` are already imported in this file. Keep the existing `spawnJobError` and `triggerRefusalWriteError` branches unchanged.

## Part C — Pre-register the two new counter labels

5. In `pkg/metrics/metrics.go`, seed both closed result labels at zero so a `rate(...) == 0` alert can fire on a path that never ran (per `docs/executor-metrics-contract.md`):
   - In `init()`, add `TaskEventsTotal.WithLabelValues("token_ceiling_read_error").Add(0)` alongside the other `TaskEventsTotal` seeds.
   - In `seedDeferredAndRefusalOutcomes()`, add `TaskEventsTotal.WithLabelValues("deferred_respawn_vault_read_failed").Add(0)` alongside the other deferred-respawn outcome seeds.
   Do not add any new metric family; both labels use the existing `TaskEventsTotal{result}` counter.

## Part D — Regression tests

6. Add to `pkg/handler/token_ceiling_internal_test.go` (package `handler`). Add the required imports (`context`, `stderrors "errors"`, `lib "github.com/bborbe/agent"`, `libtime "github.com/bborbe/time"`, `libtimetest "github.com/bborbe/time/test"`, `agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"`, `pkg "github.com/bborbe/agent-task-executor/pkg"`, `"github.com/bborbe/agent-task-executor/pkg/claim"`, `"github.com/bborbe/agent-task-executor/pkg/metrics"`, `"github.com/prometheus/client_golang/prometheus/testutil"`).

   Add an in-package stub implementing `claim.TokenSpendReader`:

```go
type stubTokenSpendReader struct {
	forAssignee    []claim.TaskIndexItem
	forAssigneeErr error
	doc            *claim.TaskDocumentItem
	docErr         error
}

func (s *stubTokenSpendReader) ReadForAssignee(
	context.Context, string,
) ([]claim.TaskIndexItem, error) {
	return s.forAssignee, s.forAssigneeErr
}

func (s *stubTokenSpendReader) ReadDocument(
	context.Context, string,
) (*claim.TaskDocumentItem, error) {
	return s.doc, s.docErr
}
```

   Add three tests:

   a. `TestDeferIfOverTokenCeiling_FailsOpenOnAssigneeReadError` — build `h := &taskEventHandler{currentDateTime: now, tokenSpendReader: &stubTokenSpendReader{forAssigneeErr: stderrors.New("dial tcp: connection refused")}, deferredRespawns: map[lib.TaskIdentifier]deferredEntry{}}`, where `now := libtime.NewCurrentDateTime()` with `now.SetNow(libtimetest.ParseDateTime("2026-11-16T10:00:00Z"))` (a Monday). Use `ceiling := int64(100)` and `config := &pkg.AgentConfiguration{Assignee: "claude", TokenCeiling: &ceiling, RunWindow: &agentv1.RunWindow{Timezone: "UTC", WeekdayStart: "00:00", WeekdayEnd: "23:59"}}`. Call `over, err := h.deferIfOverTokenCeiling(ctx, lib.Task{TaskIdentifier: "tid-read-error"}, config)` and assert: `err == nil`, `over == false`, the task is NOT in `h.deferredRespawns`, and `token_ceiling_read_error` incremented by exactly 1 (read via `testutil.ToFloat64`).

   b. `TestDeferIfOverTokenCeiling_FailsOpenOnDocumentReadError` — same as (a) but the stub returns `forAssignee: []claim.TaskIndexItem{{ID: "tid-1"}}` and `docErr: stderrors.New("dial tcp: connection refused")`. Same assertions.

   c. `TestTokenSpendSince_WrapsReadErrorAsVaultReadError` — build `h := &taskEventHandler{tokenSpendReader: &stubTokenSpendReader{forAssigneeErr: stderrors.New("boom")}}`, call `h.tokenSpendSince(ctx, "claude", time.Now())`, assert the error is non-nil and `stderrors.As(err, &vaultErr)` succeeds where `var vaultErr *vaultReadError`. Add a second case with `forAssignee: []claim.TaskIndexItem{{ID: "tid-1"}}` and `docErr` set, asserting the same.

7. Add an end-to-end regression test to `pkg/handler/task_event_handler_deferred_test.go` (package `handler_test`), inside the `Describe("EvalDeferredRespawns (spec 037)", ...)` block. Add a package-level stub in the same file (the `claim`, `agentv1`, `errors`, `testutil`, `metrics`, `libtimetest` and `domain` imports already exist):

```go
type stubTokenSpendReader struct {
	forAssignee    []claim.TaskIndexItem
	forAssigneeErr error
	doc            *claim.TaskDocumentItem
	docErr         error
}

func (s *stubTokenSpendReader) ReadForAssignee(
	context.Context, string,
) ([]claim.TaskIndexItem, error) {
	return s.forAssignee, s.forAssigneeErr
}

func (s *stubTokenSpendReader) ReadDocument(
	context.Context, string,
) (*claim.TaskDocumentItem, error) {
	return s.doc, s.docErr
}
```

   Add this `It` (it reproduces the exact bug: pre-fix, `EvalDeferredRespawns` returns an error and the loop exits):

```go
It("keeps a transient vault-read error in the token ceiling non-fatal (fail open)", func() {
	reader := &stubTokenSpendReader{}
	h = handler.NewTaskEventHandler(
		fakeSpawner, "dev", base.Branch("prod"), "", true, true,
		fakeResolver, fakeResultPublisher, taskStore, currentDateTime, fakeView,
		testTaskVault, handler.DefaultReconcilePassTimeout, handler.DefaultReconcileInterval,
		pkg.ClaimModeOff, nil,
		handler.WithTokenSpendReader(reader),
	)
	ceiling := int64(100)
	fakeResolver.ResolveReturns(pkg.AgentConfiguration{
		Assignee:     "claude",
		Image:        "my-image:latest",
		TokenCeiling: &ceiling,
		RunWindow: &agentv1.RunWindow{
			Timezone:      "UTC",
			WeekdayStart:  "09:00",
			WeekdayEnd:    "17:00",
			WeekendAllDay: true,
		},
	}, nil)

	// Park the task inside the grace window so it enters deferredRespawns.
	currentDateTime.SetNow(libtimetest.ParseDateTime(insideGrace))
	task := buildGraceTask(domain.TaskPhaseExecution, 0, 3)
	Expect(h.ConsumeMessage(ctx, buildMsg(task))).To(Succeed())
	Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(0))

	// The vault-api read now fails (dial failure).
	reader.forAssigneeErr = errors.New(ctx, "dial tcp: connection refused")
	before := testutil.ToFloat64(
		metrics.TaskEventsTotal.WithLabelValues("token_ceiling_read_error"),
	)

	// Without the fail-open this returns an error and exits the loop.
	currentDateTime.SetNow(libtimetest.ParseDateTime(graceExpiredR))
	Expect(h.EvalDeferredRespawns(ctx)).To(Succeed())

	// Fail open: the spawn proceeds despite the unreadable spend.
	Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
	Expect(
		testutil.ToFloat64(
			metrics.TaskEventsTotal.WithLabelValues("token_ceiling_read_error"),
		) - before,
	).To(Equal(float64(1)))
})
```

   Notes for this test:
   - `insideGrace` and `graceExpiredR` are the existing variables in that Describe block; `graceAnchor` is a Sunday, and `WeekendAllDay: true` keeps the run window open at that instant so the gate reaches `tokenSpendSince`.
   - `h` is the outer `handler.TaskEventHandler` variable; reassigning it with `handler.WithTokenSpendReader(reader)` is the supported way to attach the reader (see `pkg/handler/task_event_handler_options.go`).
   - The task is deferred by the grace window on the first `ConsumeMessage` (it carries `current_job` and `job_started_at`), so it is present in `deferredRespawns` when `EvalDeferredRespawns` runs.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass; do not weaken or delete them.
- The fail-open contract is load-bearing: a transient vault-api read failure must never terminate the executor process. Do not turn the read error into a fatal error anywhere on the spawn path.
- Follow `github.com/bborbe/errors` wrapping (`errors.Wrapf(ctx, err, ...)`); do not use `fmt.Errorf`.
- Mirror the existing `spawnJobError` / `triggerRefusalWriteError` carve-outs in `evalDeferredRespawns`; do not restructure that function.
- Do not add configuration knobs, opt-out flags, tunable thresholds, or new Prometheus metric families. Both new labels ride the existing `TaskEventsTotal{result}` counter.
- Keep the change minimal and scoped to this bug; no unrelated refactors.
- Anchor edits by function name, not line number.
</constraints>

<verification>
- `go test ./pkg/handler/... ./pkg/metrics/...` — must pass.
- `go test -run 'TestDeferIfOverTokenCeiling|TestTokenSpendSince' ./pkg/handler/` — must pass.
- `make precommit` — must pass (runs format, generate, test, lint).
</verification>
