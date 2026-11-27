---
status: completed
spec: [bug-nuke-dev-h1]
summary: Made the token-ceiling admission gate fail open on vault-api read errors, with a warning log, a new plain counter, unit and ConsumeMessage regression tests, and a changelog entry.
execution_id: ae9f1889-7d9f-544a-9902-2b5d870087a9-exec-070-spec-bug-nuke-dev-h1-token-ceiling-fail-open
dark-factory-version: dev
created: "2026-11-27T09:00:00Z"
queued: "2026-11-27T08:41:26Z"
started: "2026-11-27T08:41:27Z"
completed: "2026-11-27T09:04:27Z"
---

# fix: fail open when the token-ceiling spend read fails

<summary>
- A failed token-budget read no longer restarts the executor for every assignee.
- When the budget read fails, the pending run is admitted instead of being blocked, and the reconcile loop re-checks it on a later pass.
- The failure is logged as a warning and counted in a new metric, so it is visible rather than silent.
- The metric is a plain counter with no labels.
- The healthy path is unchanged: a run already at its ceiling is still deferred exactly as before.
- Both failure points are covered: listing the assignee's tasks, and reading one task's document.
- A regression test drives the real message path and asserts it returns success while admitting exactly one run.
- Unit tests pin the fail-open decision and prove the counter advances only on failure, not on the healthy path.
- A changelog entry records the fix.
</summary>

<objective>
Make the token-ceiling admission gate fail open. Today a vault-api read error on the spawn path is returned unmodified through `deferIfOverTokenCeiling`, `deferIfNotAdmitted`, and `spawnIfNeededClaimed`, so `ConsumeMessage` treats an advisory budget read as fatal and the consumer restarts the executor for every assignee (Sentry `NUKE-DEV-H1`). An unreadable spend must instead admit the spawn, count the failure, and let the existing reconcile loop re-evaluate.
</objective>

<context>
Read `CLAUDE.md` if present, and `README.md`, for project conventions.

Read these files in full before changing anything:

- `pkg/handler/token_ceiling.go` — the whole file. The fix point is the `if err != nil { return false, err }` immediately after `spent, err := h.tokenSpendSince(ctx, config.Assignee, since)` inside `deferIfOverTokenCeiling` (this is the site the spec names as `token_ceiling.go:87`). `tokenSpendSince` is the only function that produces this error, and it returns it from exactly two reads: `h.tokenSpendReader.ReadForAssignee(ctx, assignee)` and `h.tokenSpendReader.ReadDocument(ctx, item.ID)`. `glog` and `pkg/metrics` are already imported here.
- `pkg/handler/concurrency_admission.go` — `deferIfNotAdmitted`, which calls `deferIfOverTokenCeiling` and returns `(over, err)`. It propagates the error only because `deferIfOverTokenCeiling` returns one; a fail-open `(false, nil)` flows through it unchanged.
- `pkg/handler/task_event_handler.go` — `spawnIfNeededClaimed`. The `capped, err := h.deferIfNotAdmitted(...)` block near the `deferIfOverTokenCeiling` call site (the spec calls this `task_event_handler.go:981-988`) returns `spawnOutcomeSuppressed, err` on error; `ConsumeMessage` then returns that error and the consumer restarts. Do NOT change this block — the fail-open prevents the error from ever reaching it.
- `pkg/handler/job_lookup_denied.go` — the closest precedent for "an advisory/read failure must not exit `ConsumeMessage`". Read its doc comment on `jobLookupDeniedError`; it names the same failure class (Sentry `NUKE-PROD-CG`). This prompt mirrors that intent (fail open, suppress the error) but does not reuse that error type or its throttling.
- `pkg/metrics/metrics.go` — the metric definitions and the `init()` seeding block. `TaskContentTooLargeTotal` and `TaskViewSkippedTotal` are the plain-counter precedent (no labels, no seeding).
- `pkg/handler/token_ceiling_internal_test.go` — the `tokenSpendStubReader` test double and the existing `tokenSpendSince` specs. This file is plain `testing` in `package handler`; keep it that way.
- `pkg/handler/task_event_handler_deferred_test.go` and `pkg/handler/task_event_handler_test.go` — the external `handler_test` Ginkgo harness: `NewTaskEventHandler(...)` call shape, the `FakeJobSpawner` / `FakeConfigResolver` / `FakeResultPublisher` / `FakeTaskView` setup that produces a successful spawn, `testTaskVault`, and `TestHandler`/`RunSpecs`. Model the new regression spec on the `Describe("ConsumeMessage")` setup there.
- `pkg/claim/vault_api_index_client.go` — `TokenSpendReader` (`ReadForAssignee`, `ReadDocument`). It has no counterfeiter mock on purpose (its doc comment explains why), so a test double must be hand-written.
- `docs/executor-metrics-contract.md` — read only to confirm the new counter is NOT one of the two frozen contracts (`executor_config_accepted`, `executor_spawn_claim_total`). Do not touch those.

Read these coding guides from the coding plugin at `/home/node/.claude/plugins/marketplaces/coding/docs/`:
- `go-testing-guide.md` — Ginkgo v2 / Gomega and the external-`handler_test`-package conventions.
- `go-glog-guide.md` — the `event=` key=value style and warning level.
- `go-error-wrapping-guide.md` — `github.com/bborbe/errors`; this prompt adds no new error wrapping, but the guide is the baseline.
- `changelog-guide.md` — the `## Unreleased` bullet shape and the `fix:` prefix.

Verified facts (do not re-derive):
- `deferIfOverTokenCeiling` signature: `func (h *taskEventHandler) deferIfOverTokenCeiling(ctx context.Context, task lib.Task, config *pkg.AgentConfiguration) (bool, error)`. The `(bool, error)` return is `(overCeiling, err)`; the fail-open is `(false, nil)`.
- `tokenSpendSince` signature: `func (h *taskEventHandler) tokenSpendSince(ctx context.Context, assignee string, since time.Time) (int64, error)`. Keep this signature and keep it returning the read error — the fail-open belongs in the caller, not here.
- `pkg.AgentConfiguration` fields used: `TokenCeiling *int64`, `RunWindow *agentv1.RunWindow` (import `agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"`), `Assignee string`.
- `agentv1.RunWindow` fields: `Timezone string`, `WeekdayStart string`, `WeekdayEnd string`, `WeekendAllDay bool`.
- `libtime.NewCurrentDateTime()` returns `libtime.CurrentDateTime` (a setter+getter); `.SetNow(libtime.DateTime)` pins the clock; `libtimetest.ParseDateTime("2026-11-18T12:00:00Z")` returns a `libtime.DateTime`. 2026-11-18 is a Wednesday, so a `WeekdayStart:"00:00"`/`WeekdayEnd:"23:59"` window is open at 12:00 UTC (timezone empty = UTC).
- `handler.NewTaskEventHandler(...)` returns `handler.ClaimLifecycleEventHandler`, which embeds `TaskEventHandler` and therefore has `ConsumeMessage(ctx, *sarama.ConsumerMessage) error`. It takes `opts ...handler.HandlerOption`; `handler.WithTokenSpendReader(claim.TokenSpendReader)` attaches the reader. Positional args match the call in `task_event_handler_deferred_test.go`'s `BeforeEach`.
- `tokenSpendStubReader` (in `token_ceiling_internal_test.go`) currently returns `r.items, nil` and `r.docs[id], nil` with no error knob; adding fields is backward-compatible with the existing `tokenSpendStubReader{}` / `tokenSpendStubReader{items, docs}` literals.
- `metrics.TokenCeilingReadFailedTotal` will be a plain `prometheus.Counter`; assert it with `testutil.ToFloat64(...)` deltas (`github.com/prometheus/client_golang/prometheus/testutil`), because it is a process-global.
- `make precommit` runs `ensure format generate test check addlicense`; `generate` does `rm -rf mocks avro; mkdir -p mocks; go generate -mod=mod ./...`, so mocks are regenerated (this change touches no counterfeiter interface, so they regenerate identically).

<!-- OPEN QUESTION for the human reviewer: the spec says "Fail open in tokenSpendSince/deferIfOverTokenCeiling". This prompt puts the fail-open in deferIfOverTokenCeiling and leaves tokenSpendSince returning the error, because tokenSpendSince's error is exactly the read failure and keeping it observable there is testable. If you want the swallow inside tokenSpendSince instead, say so at audit time. -->

<!-- OPEN QUESTION for the human reviewer: the new counter is a plain, unlabelled counter (no per-assignee label), matching TaskContentTooLargeTotal. If the fleet needs to know WHICH assignee's read failed from the metric alone, an `assignee` label would be a follow-up; the log line already carries it. -->

<!-- OPEN QUESTION for the human reviewer: the warning is not throttled. A sustained vault-api outage logs one warning per spawn attempt (spawns are bounded by the concurrency cap, unlike the per-event denial path that job_lookup_denied throttles). Throttling was deliberately omitted because the spec does not request it; add a throttle only if production noise proves it necessary. -->
</context>

<requirements>
1. **Add the failure counter to `pkg/metrics/metrics.go`.** Place it beside `TaskViewSkippedTotal` (a plain-counter neighbour). It is a plain counter — no labels, no `init()` seeding (a plain counter registers at 0 by itself):

   ```go
   // TokenCeilingReadFailedTotal counts window-token reads that failed and were
   // failed open: the ceiling could not be enforced for that spawn, so the spawn
   // was admitted and the reconcile loop re-evaluates it. Without this counter
   // the fail-open is silent, and a persistently unreadable vault-api looks like
   // a fleet that simply never reaches its ceiling.
   var TokenCeilingReadFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
       Name: "agent_executor_token_ceiling_read_failed_total",
       Help: "Total number of token-ceiling spend reads that failed and were failed open.",
   })
   ```

   `promauto` and `prometheus` are already imported in that file. Do not add a label, do not seed it in `init()`, and do not touch the two frozen contracts documented in `docs/executor-metrics-contract.md`.

2. **Fail open in `deferIfOverTokenCeiling` (`pkg/handler/token_ceiling.go`).** Replace exactly this block:

   ```go
   	spent, err := h.tokenSpendSince(ctx, config.Assignee, since)
   	if err != nil {
   		return false, err
   	}
   ```

   with:

   ```go
   	spent, err := h.tokenSpendSince(ctx, config.Assignee, since)
   	if err != nil {
   		// The window budget is advisory: an unreadable spend must not stop the
   		// spawn, or a vault-api dial failure would exit ConsumeMessage and
   		// restart the executor for every assignee at once (Sentry NUKE-DEV-H1).
   		// Fail open, admit the spawn, and let the reconcile loop re-evaluate
   		// once the read recovers. Logged and counted so the failure is not
   		// silent.
   		glog.Warningf(
   			"event=token_ceiling_read_failed task=%s assignee=%s err=%v: failing open, spawn admitted",
   			task.TaskIdentifier, config.Assignee, err,
   		)
   		metrics.TokenCeilingReadFailedTotal.Inc()
   		return false, nil
   	}
   ```

   Do not change `tokenSpendSince`, `deferIfNotAdmitted`, or the `spawnIfNeededClaimed` error block. The `err != nil` branch above is the only fail-open point, and it covers both `ReadForAssignee` and `ReadDocument` because `tokenSpendSince` returns both through this one error.

3. **Extend the test double and add unit tests in `pkg/handler/token_ceiling_internal_test.go`** (plain `testing`, `package handler`; do not convert to Ginkgo; do not import `mocks` — it imports `pkg/handler`).

   Extend `tokenSpendStubReader` with two error knobs, leaving existing literals valid:

   ```go
   type tokenSpendStubReader struct {
       items   []claim.TaskIndexItem
       docs    map[string]*claim.TaskDocumentItem
       readErr error // returned by ReadForAssignee when set
       docErr  error // returned by ReadDocument when set
   }

   func (r tokenSpendStubReader) ReadForAssignee(
       context.Context,
       string,
   ) ([]claim.TaskIndexItem, error) {
       if r.readErr != nil {
           return nil, r.readErr
       }
       return r.items, nil
   }

   func (r tokenSpendStubReader) ReadDocument(
       _ context.Context,
       id string,
   ) (*claim.TaskDocumentItem, error) {
       if r.docErr != nil {
           return nil, r.docErr
       }
       return r.docs[id], nil
   }
   ```

   Add three tests (add the needed imports: `stderrors "errors"`, `lib "github.com/bborbe/agent"`, `libtime "github.com/bborbe/time"`, `libtimetest "github.com/bborbe/time/test"`, `"github.com/prometheus/client_golang/prometheus/testutil"`, `agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"`, `pkg "github.com/bborbe/agent-task-executor/pkg"`, `"github.com/bborbe/agent-task-executor/pkg/metrics"`):

   - `TestDeferIfOverTokenCeiling_FailsOpenOnAssigneeReadError`: clock at `2026-11-18T12:00:00Z`, config `{Assignee:"proto-agent", TokenCeiling:&ceiling(1), RunWindow:&agentv1.RunWindow{WeekdayStart:"00:00", WeekdayEnd:"23:59"}}`, handler `&taskEventHandler{tokenSpendReader: tokenSpendStubReader{readErr: stderrors.New("dial tcp: connection refused")}, currentDateTime: clock}`. Capture `before := testutil.ToFloat64(metrics.TokenCeilingReadFailedTotal)`, call `deferIfOverTokenCeiling(ctx, lib.Task{TaskIdentifier:"tid-ceiling-read-fail"}, config)`, assert `err == nil`, `over == false`, and `testutil.ToFloat64(metrics.TokenCeilingReadFailedTotal)-before == 1`.
   - `TestDeferIfOverTokenCeiling_FailsOpenOnDocumentReadError`: same config and clock; reader `tokenSpendStubReader{items: []claim.TaskIndexItem{{ID:"id-a"}}, docErr: stderrors.New("read vault-api task document: 503")}`. Assert `err == nil`, `over == false`, metric delta `1`.
   - `TestDeferIfOverTokenCeiling_DefersWhenSpendMeetsCeiling` (control, proves the healthy path is unchanged): same config and clock but `ceiling := int64(150)`; reader `tokenSpendStubReader{items: []claim.TaskIndexItem{{ID:"id-a"}}, docs: map[string]*claim.TaskDocumentItem{"id-a": {ID:"id-a", Frontmatter: map[string]any{"metrics_runs": []any{map[string]any{"completed_at":"2026-11-18T13:00:00Z", "input_tokens": int64(150)}}}}}}`; handler also sets `deferredRespawns: make(map[lib.TaskIdentifier]deferredEntry)` (the over-ceiling branch writes to it). Assert `err == nil`, `over == true`, and the `token_ceiling_read_failed` delta is `0`.

4. **Add the `ConsumeMessage` regression spec** in a new file `pkg/handler/token_ceiling_failopen_test.go` (`package handler_test`, BSD license header matching the other files, Ginkgo v2/Gomega). Do NOT add another `RunSpecs` — `TestHandler` in `task_event_handler_test.go` already runs the suite, and this file is in the same package so its `var _ = Describe(...)` is discovered. The spec drives the real consumer entry point and is the assertion the spec explicitly asks for: `ConsumeMessage` returns nil when `ReadForAssignee` fails.

   Define a hand-written double (there is no counterfeiter mock for `claim.TokenSpendReader`):

   ```go
   type failingTokenSpendReader struct{ err error }

   func (r failingTokenSpendReader) ReadForAssignee(
       context.Context,
       string,
   ) ([]claim.TaskIndexItem, error) {
       return nil, r.err
   }

   func (r failingTokenSpendReader) ReadDocument(
       context.Context,
       string,
   ) (*claim.TaskDocumentItem, error) {
       return nil, r.err
   }
   ```

   The spec (mirror the `Describe("ConsumeMessage")` setup in `task_event_handler_deferred_test.go`):

   ```go
   var _ = Describe("ConsumeMessage token ceiling read failure", func() {
       It("admits the spawn and returns nil when ReadForAssignee fails", func() {
           ctx := context.Background()

           fakeSpawner := new(mocks.FakeJobSpawner)
           fakeResolver := &mocks.FakeConfigResolver{}
           fakeResultPublisher := &mocks.FakeResultPublisher{}
           fakeView := &mocks.FakeTaskView{}
           fakeView.ReadyReturns(true)
           fakeView.SnapshotReturns(nil)
           fakeView.GetReturns(lib.Task{}, false)

           clock := libtime.NewCurrentDateTime()
           clock.SetNow(libtimetest.ParseDateTime("2026-11-18T12:00:00Z"))

           ceiling := int64(1)
           fakeResolver.ResolveReturns(pkg.AgentConfiguration{
               Assignee:     "claude",
               Image:        "my-image:latest",
               TokenCeiling: &ceiling,
               RunWindow:    &agentv1.RunWindow{WeekdayStart: "00:00", WeekdayEnd: "23:59"},
           }, nil)
           fakeSpawner.IsJobActiveReturns(false, nil)
           fakeSpawner.SpawnJobReturns("job-1", nil)

           h := handler.NewTaskEventHandler(
               fakeSpawner,
               "dev",
               base.Branch("prod"),
               "",
               true,
               true,
               fakeResolver,
               fakeResultPublisher,
               pkg.NewTaskStore(),
               clock,
               fakeView,
               testTaskVault,
               handler.DefaultReconcilePassTimeout,
               handler.DefaultReconcileInterval,
               pkg.ClaimModeOff,
               nil,
               handler.WithTokenSpendReader(
                   failingTokenSpendReader{err: stderrors.New("dial tcp: connection refused")},
               ),
           )

           task := lib.Task{
               TaskIdentifier: lib.TaskIdentifier("tid-token-ceiling-read-fail"),
               Vault:          testTaskVault,
               Frontmatter: lib.TaskFrontmatter{
                   "status":   "in_progress",
                   "phase":    string(domain.TaskPhaseExecution),
                   "assignee": "claude",
                   "stage":    "prod",
               },
           }
           value, err := json.Marshal(task)
           Expect(err).To(BeNil())

           before := testutil.ToFloat64(metrics.TokenCeilingReadFailedTotal)
           err = h.ConsumeMessage(ctx, &sarama.ConsumerMessage{Value: value})
           Expect(err).To(BeNil())
           Expect(fakeSpawner.SpawnJobCallCount()).To(Equal(1))
           Expect(
               testutil.ToFloat64(metrics.TokenCeilingReadFailedTotal) - before,
           ).To(Equal(float64(1)))
       })
   })
   ```

   Required imports for this file: `context`, `encoding/json`, `stderrors "errors"`, `github.com/IBM/sarama`, `lib "github.com/bborbe/agent"`, `github.com/bborbe/cqrs/base`, `libtime "github.com/bborbe/time"`, `libtimetest "github.com/bborbe/time/test"`, `github.com/bborbe/vault-cli/pkg/domain`, `. "github.com/onsi/ginkgo/v2"`, `. "github.com/onsi/gomega"`, `"github.com/prometheus/client_golang/prometheus/testutil"`, `agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"`, `"github.com/bborbe/agent-task-executor/mocks"`, `pkg "github.com/bborbe/agent-task-executor/pkg"`, `"github.com/bborbe/agent-task-executor/pkg/claim"`, `"github.com/bborbe/agent-task-executor/pkg/handler"`, `"github.com/bborbe/agent-task-executor/pkg/metrics"`. `testTaskVault` is already declared in `task_event_handler_test.go` in this package — reuse it, do not redeclare it.

5. **Add a changelog entry.** `CHANGELOG.md` currently has no `## Unreleased` section; its first heading is `## v0.71.2`. Insert a new `## Unreleased` section directly below the frozen preamble (the `# Changelog` title and the "All notable changes…" line) and above `## v0.71.2`, with exactly one bullet:

   ```
   - fix: **a failed token-ceiling spend read no longer exits the executor; the spawn is failed open and the read failure is counted.** `deferIfOverTokenCeiling` returned the `ReadForAssignee`/`ReadDocument` error from `tokenSpendSince` unchanged, so a vault-api dial failure exited `ConsumeMessage` and restarted the executor for every assignee (Sentry NUKE-DEV-H1). The ceiling is advisory, so the error now logs `event=token_ceiling_read_failed` at warn, increments `agent_executor_token_ceiling_read_failed_total`, and returns `(false, nil)`: the spawn is admitted and the reconcile loop re-evaluates once the read recovers. The healthy path (spend meets the ceiling) still defers. Change set: `pkg/handler/token_ceiling.go`, `pkg/metrics/metrics.go`, `pkg/handler/token_ceiling_internal_test.go`, `pkg/handler/token_ceiling_failopen_test.go`, `CHANGELOG.md`.
   ```

   Do not create a second `## Unreleased`, do not touch any released heading, and keep the preamble above it unchanged (see `changelog-guide.md`, rule `changelog/preamble-frozen`).

6. **Self-check before finishing.** Re-run every command in `<verification>` and confirm each passes. Walk requirements 1-5 against the diff and confirm each is satisfied. Confirm no production file other than `pkg/handler/token_ceiling.go` and `pkg/metrics/metrics.go` changed.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT change `tokenSpendSince`'s signature or make it swallow the error. The fail-open lives in `deferIfOverTokenCeiling` only.
- Do NOT change the `deferIfNotAdmitted` error return or the `spawnIfNeededClaimed` `deferIfNotAdmitted` error block. The fail-open means no error reaches them; touching them would widen the change.
- Do NOT change the healthy over-ceiling behaviour: `spent >= *config.TokenCeiling` must still defer exactly as today (the control unit test pins this).
- No new Config field, no new CRD knob, no env var, no flag, no opt-out. The only new observability is the log line `event=token_ceiling_read_failed` and the counter `agent_executor_token_ceiling_read_failed_total`.
- The new counter is a plain, unlabelled counter and is NOT seeded in `init()`. Do not modify the two frozen metric contracts in `docs/executor-metrics-contract.md`.
- Do not throttle the warning. The spec asks only for a warn log; a throttle is out of scope.
- `pkg/handler/token_ceiling_internal_test.go` is plain `testing` in `package handler`; keep it that way and do not import `mocks` there (import cycle).
- The new external test file must be Ginkgo in `package handler_test` and must not add another `RunSpecs`; the suite entry point is `TestHandler`.
- Do NOT hand-edit anything under `mocks/`. `make precommit`'s `generate` target rewrites it; this change touches no counterfeiter interface, so it regenerates unchanged.
- Wrap errors with `github.com/bborbe/errors` if you add any; this change adds none. Respect the 100-character line limit and the repo's lint limits.
- Use `-mod=mod` as the Makefile does. Do not run `go mod vendor`, do not create `vendor/`, and never use `-mod=vendor`.
- Existing tests must still pass.
</constraints>

<verification>
Run from the repository root:

- `make precommit` — must exit 0. This runs `ensure format generate test check addlicense` over the whole module.
- `go test -mod=mod -count=1 ./pkg/handler/ -run 'TestDeferIfOverTokenCeiling' -v` — must exit 0 and show the three `TestDeferIfOverTokenCeiling_*` tests passing (fail-open on assignee read error, fail-open on document read error, defers when spend meets ceiling).
- `go test -mod=mod -count=1 ./pkg/handler/ -run TestHandler -v -args -ginkgo.focus='token ceiling read failure' > /tmp/tc.out 2>&1; grep -q 'SUCCESS!' /tmp/tc.out && grep -q 'Ran 1 of' /tmp/tc.out && echo TOKEN_CEILING_REGRESSION_OK` — must print `TOKEN_CEILING_REGRESSION_OK`.
- `grep -q 'TokenCeilingReadFailedTotal' pkg/metrics/metrics.go && echo METRIC_OK` — must print `METRIC_OK`.
- `grep -q 'event=token_ceiling_read_failed' pkg/handler/token_ceiling.go && echo FAILOPEN_LOG_OK` — must print `FAILOPEN_LOG_OK`.
- `grep -q 'return false, nil' pkg/handler/token_ceiling.go && echo FAILOPEN_RETURN_OK` — must print `FAILOPEN_RETURN_OK`.
- `test -f pkg/handler/token_ceiling_failopen_test.go && echo REGRESSION_FILE_OK` — must print `REGRESSION_FILE_OK`.
- `grep -c '^## Unreleased$' CHANGELOG.md` — must print `1`.
- `awk '/^## /{sec=$0} /token-ceiling spend read no longer exits/{print sec; exit}' CHANGELOG.md` — must print `## Unreleased`.
</verification>
