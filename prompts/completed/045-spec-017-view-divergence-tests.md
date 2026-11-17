---
status: completed
spec: [017-view-divergence-detector]
summary: Added pkg/handler/view_divergence_test.go proving the reconcile-pass divergence detector end to end through ReconcileOnce against a real httptest index, covering divergence, agreement, absence, empty index, read failure, no-client, request bounds and early-return passes; precommit passes.
execution_id: agent-task-executor-view-divergence-exec-045-spec-017-view-divergence-tests
dark-factory-version: v0.198.7
created: "2026-10-27T00:00:00Z"
queued: "2026-10-26T23:15:33Z"
started: "2026-10-27T19:59:28Z"
completed: "2026-10-27T20:07:13Z"
---

# Prove the detector end to end through the reconcile pass

<summary>
- A three-entry table proves the check fires on a status-only divergence and a phase-only divergence, and stays silent when both agree
- The fixture serves a fixed three-id owned set with exactly one divergent id in every entry
- An owned id missing from the index is reported as absent, not as a divergence
- Exactly one index request is made per pass that reaches the check, and none at all for a pass that returns early
- A pass whose vault-api port is closed still drives its candidates and logs the read failure
- A deployment with no index client emits no line and moves no series
- The summary line reports the coverage, the index age and the warnings count
- The tests use a real HTTP server and the real client, so the auth and envelope paths are exercised
</summary>

<objective>
Lock the detector's contract with integration tests that run through `ReconcileOnce` against an `httptest` index. The negative cases — agreement, absence, read failure, no client — are the ones that make the check discriminating, so they are tested at the same level as the positive case.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for execution rules.

Then read these, in this order:

- `pkg/handler/task_reconcile_test.go` — the `Describe("TaskEventHandler reconcile loop")` fixture: `fakeSpawner`, `fakeResolver`, `fakeResultPublisher`, `fakeView`, `taskStore`, `currentDateTime`, `eligibleTask`, and the `NewTaskEventHandler(...)` construction. Reuse its shapes; do not duplicate its existing specs.
- `pkg/handler/task_view_test.go` — `captureStderr(fn func()) string`, which swaps `os.Stderr` and flushes glog. It is in `package handler_test`, so a new file in the same package can call it.
- `pkg/handler/task_event_handler_test.go` — `const testTaskVault = "openclaw"` and the `RunSpecs` bootstrap.
- `pkg/handler/view_divergence.go` — the implementation under test: the log event names and the summary fields.
- `pkg/claim/vault_api_index_client.go` — `claim.NewVaultAPIIndexClient(baseURL, secret, initiator)`.
- `pkg/metrics/metrics.go` — `ViewDivergenceTotal`.
- `pkg/handler/task_event_handler.go` — `ReconcileOnce`'s early returns and the drive loop, so the "pass continues on a read error" assertion is aimed at the right line.

Coding docs:
`/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`.
</context>

<requirements>
1. **Add `pkg/handler/view_divergence_test.go`** in `package handler_test`, Ginkgo/Gomega, discovered by the existing `RunSpecs` in `task_event_handler_test.go`. Use `mocks.FakeTaskView` and `mocks.FakeJobSpawner`; use the real `claim.NewVaultAPIIndexClient` against an `httptest.Server`. Do not add a second test style to the package.

2. **Helpers** (file-local):
   - `newIndexServer(body string) (*httptest.Server, *int32, *http.Header)` — serves `200` with `body`, increments the counter on every request with `atomic.AddInt32`, and records the last request's header clone under a `sync.Mutex`. The test reads the header after `ReconcileOnce` returns, so guard it rather than relying on the response's happens-before. Close the server with `DeferCleanup`.
   - `indexBody(items []map[string]any, age, warnings int) string` — `json.Marshal(map[string]any{"items": items, "total": len(items), "warnings": warnings, "index_age_seconds": age})`. Use `[]map[string]any{}` for an empty list so the JSON is `[]`, not `null`.
   - `ownedTask(id, status, phase string) lib.Task` — `TaskIdentifier: lib.TaskIdentifier(id)`, `Vault: testTaskVault`, `Frontmatter: lib.TaskFrontmatter{"status": status, "phase": phase}`. `TaskIdentifier` is a named type, so the conversion is required.
   - `newDivergenceHandler(indexClient claim.VaultAPIIndexClient, view handler.TaskView) handler.ClaimLifecycleEventHandler` — mirror `task_reconcile_test.go`'s construction with `pkg.ClaimModeOff`, `handler.DefaultReconcilePassTimeout`, `handler.DefaultReconcileInterval`, and `handler.WithVaultAPIIndexClient(indexClient)` as the last option. Callers set the view up themselves: `fakeView.ReadyReturns(true)` and `fakeView.SnapshotReturns(...)` are both required, because `ReconcileOnce` returns at `view_not_ready` before the check otherwise.
   - Use terminal tasks (`status: completed`, `phase: done`) in the comparison table so the drive loop spawns nothing and the test reads only the check. Use one eligible task (`status: in_progress`, `phase: execution`, `assignee: claude`, `stage: prod`, `Vault: testTaskVault`) only in the read-failure test, with `fakeSpawner.IsJobActiveReturns(false, nil)` and a resolver returning `pkg.AgentConfiguration{Assignee: "claude", Image: "my-image:latest"}`.

3. **`DescribeTable` "reports a divergence in either field and stays silent on agreement".** Fixed owned set in every entry: `tid-a`, `tid-b`, `tid-c`, all `completed`/`done`. Index contains all three; `tid-b` and `tid-c` always match (`completed`/`done`). Vary `tid-a`:
   - entry "status differs": vault `status: aborted`, `phase: done`.
   - entry "phase differs": vault `status: completed`, `phase: human_review`.
   - entry "both agree": vault `status: completed`, `phase: done`.
   Use `index_age_seconds: 12` in every entry. The "status differs" entry uses `warnings: 3` and must show `warnings=3` in its summary; the other two entries use `warnings: 0`. A non-zero value is the only thing that proves the field is carried through from the index rather than hardcoded. Capture the pass with `captureStderr(func() { Expect(h.ReconcileOnce(ctx)).To(Succeed()) })` and assert:
   - entries "status differs" and "phase differs": the output contains `event=view_divergence task=tid-a` and, on the same line, `view_status=`, `vault_status=`, `view_phase=`, `vault_phase=`. Assert the specific values for the changed field and the matching value for the unchanged one. The summary contains `event=view_divergence_summary compared=3 divergent=1` and `index_age_seconds=12`; the "status differs" summary carries `warnings=3` and the "phase differs" summary carries `warnings=0`.
   - entry "both agree": no `event=view_divergence task=tid-a`; summary contains `event=view_divergence_summary compared=3 divergent=0` and `warnings=0`.
   - in every entry: no `event=view_divergence task=tid-b` and no `event=view_divergence task=tid-c`.
   - the server saw exactly one request; its method is `GET`, its path is `/api/v1/tasks`, its `RawQuery` is empty, `X-Gateway-Secret == "secret"` and `X-Gateway-Initator == "agent-task-executor"`. Build the client for the table with `claim.NewVaultAPIIndexClient(server.URL, "secret", "agent-task-executor")` — the two strings must match the assertions exactly. This is the boundary assertion that the unfiltered, authenticated read is what the pass actually sends.
   - in the "status differs" entry: give the divergent owned task a distinctive frontmatter `title` and `assignee`, and assert the captured output does not contain either string. The line carries ids, statuses and phases only.

4. **`It` "reports an owned id missing from the index as absent".** Owned `tid-a`, `tid-b`, `tid-c` (all terminal); index omits `tid-b`. Assert the output contains `event=view_divergence_absent task=tid-b`, does not contain `event=view_divergence task=tid-b`, and contains `event=view_divergence_summary compared=2 divergent=0`. Assert the `absent` child series moved by exactly 1 and `mismatch` did not move. A second `DescribeTable`, "an empty index reports every owned id as absent", has one entry serving `indexBody([]map[string]any{}, 12, 0)` — `{"items": [], "total": 0, "warnings": 0, "index_age_seconds": 12}` — over the same three-id owned set: assert the summary contains `event=view_divergence_summary compared=0 divergent=0` and `strings.Count(out, "event=view_divergence_absent") == 3`. This proves the check is driven by the owned set, not by the index's contents.

5. **`It` "makes one index request per pass regardless of task count".** Build 50 owned terminal tasks (`tid-0` … `tid-49`), index all 50. Assert the server saw exactly one request and the summary reads `event=view_divergence_summary compared=50 divergent=0`.

6. **`It` "a failed index read does not stop the pass".** Build one eligible owned task and a resolver that returns a Config for it. Create an `httptest.NewServer`, capture its `URL`, then `Close()` it so the port is closed. Build `claim.NewVaultAPIIndexClient(url, "", "")` and the handler with it. Assert:
   - `ReconcileOnce` returns `nil`;
   - the output contains `event=view_divergence_read_error`;
   - the output contains `event=reconcile evaluated=`;
   - `fakeSpawner.SpawnJobCallCount() == 1`;
   - the `read_error` child series moved by 1 and `ok` did not move.
   This is the spec's closed-port evidence: a read failure is a finding, not a pass failure.

7. **`It` "a deployment with no index client emits nothing".** Build the handler without `WithVaultAPIIndexClient` (nil client). Run one pass over a ready view. Assert the output contains no `event=view_divergence` substring and all four child series are unchanged. This is the no-URL deployment's shape.

8. **`It` "a healthy pass counts one summary and one evaluated line".** One successful pass with three owned terminal tasks and one divergent id. Assert `strings.Count(out, "event=view_divergence_summary") == 1`, `strings.Count(out, "event=reconcile evaluated=") == 1`, and the `ok` child series moved by exactly 1. This is the healthy-window equality the spec's AC 4 states.

9. **`DescribeTable` "a pass that returns before the check makes no index request".** Fixed owned set in every entry (all `completed`/`done`, `Vault: testTaskVault` unless the entry says otherwise); the index serves all three ids with `index_age_seconds: 12`, `warnings: 0`. Vary only the pass's precondition, and in every entry assert the server saw zero requests and the output contains no `event=view_divergence` substring:
    - entry "view not ready": `fakeView.ReadyReturns(false)`; the output contains `event=reconcile_view_not_ready`.
    - entry "task vault unstamped": one owned task with `Vault: ""`; the output contains `event=task_vault_unstamped`.
    - entry "task vault mismatch": one owned task with `Vault: "other-vault"`; the output contains `event=task_vault_mismatch`.
    - entry "context already expired": call `h.ReconcileOnce(expiredCtx)` where `expiredCtx, cancel := context.WithCancel(ctx)` is cancelled before the call; the output contains `event=reconcile_aborted`.
    This is the other half of the spec's AC 4: the one-call bound is a floor of zero for a pass that never reaches the check, and the `passDone` guard is the mechanism that enforces it.

10. **Do not add operator-only checks.** No `kubectl`, no `docker`, no `dark-factory`, no `gh`, no host paths. The only network is `httptest` on loopback.
</requirements>

<constraints>
- Do NOT modify the implementation to make a test pass. If the contract and the code disagree, fix the code in the file prompt 2 or 3 created, and say so.
- Do NOT weaken or delete an existing reconcile spec.
- Assert counter deltas, never absolutes: these counters are process-global and never reset.
- Read the child series with `testutil.ToFloat64(metrics.ViewDivergenceTotal.WithLabelValues(...))`; the vec is pre-initialised, so the bare collector panics.
- Use `DeferCleanup` for every `httptest.Server`.
- Do NOT import `mocks` into an internal test file; this file is external (`package handler_test`), so `mocks` is allowed here.
- Add the standard license header to every new Go file.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
test -f pkg/handler/view_divergence_test.go
ROOTDIR=/workspace make precommit
go test -mod=mod -count=1 ./pkg/handler/ ./pkg/metrics/ ./pkg/claim/
grep -n 'event=view_divergence' pkg/handler/*.go
grep -n 'view_divergence' pkg/metrics/metrics.go
</verification>
