---
status: completed
spec: [025-bug-rearm-clobbered-by-stale-park-reemission]
summary: Made the reconcile terminal_status gate consult the pass-cached vault index, evaluating a terminal view over a non-terminal vault and logging event=terminal_gate_divergence with decision_source=vault, with six focused specs and CHANGELOG updated.
execution_id: 5f9bfd78-31e8-5f03-be25-0d5a3c166713-exec-070-spec-025-vault-terminal-gate
dark-factory-version: dev
created: "2026-11-22T09:30:00Z"
queued: "2026-11-22T09:47:51Z"
started: "2026-11-22T10:02:41Z"
completed: "2026-11-22T10:07:26Z"
branch: dark-factory/bug-rearm-clobbered-by-stale-park-reemission
---

# Vault-authoritative terminal gate plus divergence log line

<summary>
- A task whose view reports a terminal status but whose vault index reports the same task non-terminal is now evaluated, not refused.
- When the two disagree, the vault decides and one log line names the view status, the vault status, and `decision_source=vault`.
- When the vault index agrees the task is terminal, it is still refused exactly as today, and no divergence line is emitted.
- When the index has no entry for the task, or the read fails, the executor falls back to today's view-only decision.
- The gate reuses the reconcile pass's single, already-performed index read, so a pass still costs one request.
- The `index_terminal` and `terminal_status` gates stay distinct; only `terminal_status` gains the override.
- No new metric; the existing counters and the new log line carry the signal.
</summary>

<objective>
Make the reconcile's `terminal_status` gate consult the vault index it already reads in the same pass. When the view says terminal and the vault index says non-terminal, evaluate the task instead of refusing it, and log `event=terminal_gate_divergence` naming both values and `decision_source=vault`. Keep the view-only refusal when the index agrees, is absent, or cannot be read.
</objective>

<context>
Read `CLAUDE.md` if present, and `README.md`, for project conventions.

Read these files in full before changing anything:

- `pkg/handler/task_event_handler.go` — `reconcileTask` (around the `IsTerminalStatus(task.Frontmatter.Status())` gate that calls `refuseReconcile(task, metrics.ReconcileRefusedGateTerminalStatus)`), `gatherCandidates` / `gatherCandidate` (which call `reconcileTask` with the pass context), and `ReconcileOnce` (which installs `withVaultIndexCache(passCtx)` beside the other pass caches). This file is 1988 lines against revive's `file-length-limit: 2000`; keep the call-site edit to the existing `if` block only and put all new code in a new file.
- `pkg/handler/vault_index_cache.go` — `vaultIndexFor(ctx) (*claim.TaskIndex, map[string]struct{}, error)` and `indexTerminalIDs`. `vaultIndexFor` returns `(nil, nil, nil)` when `h.vaultIndexClient == nil`, and shares one read per pass through the context cache. You must read the cached index's **status value** — the derived terminal set cannot supply `vault_status=` for the log line.
- `pkg/handler/view_divergence.go` — `compareViewToIndex`'s duplicate-id tie-break (a duplicate id keeps the last item) and the `event=view_divergence` line. Do not change it.
- `pkg/handler/index_terminal_suppression.go` — the sibling `index_terminal` gate. It suppresses on index evidence and fails open. Do not merge it with `terminal_status`.
- `pkg/handler/task_event_handler_reconcile_refusal.go` — `refuseReconcile` logs `event=reconcile_refused … gate=<gate>` only when a task's gate changes. Do not route the new divergence line through it.
- `pkg/claim/vault_api_index_client.go` — `TaskIndex{Items []TaskIndexItem, ...}`, `TaskIndexItem{ID, Status, Phase}`.
- `pkg/handler/task_event_handler_phases.go` — `IsTerminal`, the fixed terminal phase set.
- `pkg/handler/task_event_handler.go` — `IsTerminalStatus` and the `terminalStatuses` set.
- `pkg/metrics/metrics.go` — `ReconcileRefusedGateTerminalStatus = "terminal_status"` and `ReconcileRefusedGateIndexTerminal = "index_terminal"`.
- `pkg/handler/index_terminal_reconcile_test.go` — the reconcile harness this suite mirrors: `new(mocks.FakeJobSpawner)` with `IsJobActiveReturns(false, nil)` / `SpawnJobReturns("job-1", nil)`, `mocks.FakeConfigResolver`, `mocks.FakeResultPublisher`, `mocks.FakeTaskView` with `ReadyReturns(true)`, `libtime.NewCurrentDateTime()`, `handler.NewTaskEventHandler(...)`, `handler.WithVaultAPIIndexClient(client)`, `testTaskVault`, and a counting index stub.
- `pkg/handler/index_terminal_deferred_test.go` — `deferredGateCaptureStderr(fn func()) string`, which sets `v=2` and captures stderr. Use it for the `glog.V(2)` lines.
- `docs/vault-api-task-index.md` § *Divergence check* and § *Refusal gate: `index_terminal`* — the behaviour a later prompt documents; do not edit the doc here.

Read these coding guides from the coding plugin at `/home/node/.claude/plugins/marketplaces/coding/docs/`:
- `go-testing-guide.md` — Ginkgo v2/Gomega conventions
- `go-glog-guide.md` — `event=` key=value style and V-levels
- `go-error-wrapping-guide.md` — `github.com/bborbe/errors`

Verified facts (do not re-derive):
- `task.Frontmatter.Status()` returns `domain.TaskStatus` (a string type); `%s` formats it directly.
- `vaultIndexFor` returns the shared `*claim.TaskIndex` even when the cache is used, so `index.Items` is available to read a status value.
- `reconcileTask` is called from `gatherCandidate(passCtx, ...)`, so it receives the pass context that carries the vault-index cache; calling `vaultIndexFor` there reuses the pass's single read.
- The Kafka event path (`parseAndFilter`, `skipped_terminal_status`) is deliberately **out of scope**: the event carries the vault's own write, which is already authoritative.
</context>

<requirements>
1. Create `pkg/handler/terminal_gate_divergence.go` (`package handler`, BSD license header). Do **not** add fields to `taskEventHandler` and do **not** add a metric. Imports: `context`, `lib "github.com/bborbe/agent"`, `"github.com/bborbe/vault-cli/pkg/domain"`, `"github.com/golang/glog"`, `"github.com/bborbe/agent-task-executor/pkg/claim"`.

   - `func vaultStatusFor(index *claim.TaskIndex, id string) (string, bool)` — iterate `index.Items` in order; for a duplicate id keep the **last** item (assign each match so the last wins), matching `indexTerminalIDs` / `compareViewToIndex`. Return the item's `Status` and `true`; return `("", false)` when no item matches or `index == nil`.
   - `func (h *taskEventHandler) terminalGateAllowsEvaluation(ctx context.Context, task lib.Task) bool`:
     - if `h.vaultIndexClient == nil`, return `false` (view-only decision; no call, no log — the no-URL deployment's shape);
     - `index, _, err := h.vaultIndexFor(ctx)`; if `err != nil || index == nil`, log `glog.V(2).Infof("event=terminal_gate_index_unavailable gate=terminal_status task=%s", task.TaskIdentifier)` and return `false` (fall back to today's view-only decision);
     - `vaultStatus, ok := vaultStatusFor(index, string(task.TaskIdentifier))`; if `!ok`, return `false` (no vault evidence of a non-terminal state; refuse as today, no log);
     - if `IsTerminalStatus(domain.TaskStatus(vaultStatus))`, return `false` (both terminal; refuse as today, no divergence line);
     - otherwise log exactly `glog.V(2).Infof("event=terminal_gate_divergence task=%s view_status=%s vault_status=%s decision_source=vault", task.TaskIdentifier, task.Frontmatter.Status(), vaultStatus)` and return `true` (the vault decides: evaluate).
   - `func (h *taskEventHandler) terminalStatusRefuses(ctx context.Context, task lib.Task) bool` — `if !IsTerminalStatus(task.Frontmatter.Status()) { return false }; return !h.terminalGateAllowsEvaluation(ctx, task)`. This wrapper keeps `reconcileTask`'s cognitive complexity unchanged (it replaces the existing `if` condition, adding no branch).

2. In `pkg/handler/task_event_handler.go`, inside `reconcileTask`, replace the terminal-status gate block:
   - old:
     ```go
     	if IsTerminalStatus(task.Frontmatter.Status()) {
     		return h.refuseReconcile(task, metrics.ReconcileRefusedGateTerminalStatus)
     	}
     ```
     with
     ```go
     	// A terminal VIEW status is refused unless the vault index reports the same
     	// task non-terminal: the vault is the authority (the view is a projection),
     	// so a re-armed task whose view has not replayed yet must be evaluated, not
     	// refused. See terminal_gate_divergence.go.
     	if h.terminalStatusRefuses(ctx, task) {
     		return h.refuseReconcile(task, metrics.ReconcileRefusedGateTerminalStatus)
     	}
     ```
   Do not change any other gate, its order, or `refuseReconcile`. Do not touch `parseAndFilter` (the Kafka path).

3. Create `pkg/handler/terminal_gate_divergence_test.go` (`package handler_test`, Ginkgo v2/Gomega, BSD license header). Make it **self-contained**: declare its own index stub type (prefix it `terminalGateIndexStub` so it cannot collide with `reconcileGateIndexStub` in `index_terminal_reconcile_test.go`), its own `terminalGateBuild`, `terminalGateTask`, `terminalGateConfig`, and `terminalGateSpawnedIDs` helpers, mirroring `index_terminal_reconcile_test.go`. Reuse the existing `deferredGateCaptureStderr` helper (same package) and `testTaskVault`. The stub implements `claim.VaultAPIIndexClient` with `items []claim.TaskIndexItem`, `err error`, and `reads int`.

   Build tasks with `status`, `phase`, `assignee: claude`, `stage: prod`, `Vault: testTaskVault`. Build the Config with `Trigger.Statuses` containing `in_progress`, `completed`, `aborted` and `Trigger.Phases` containing the phases used. Cover:
   - **Terminal view over non-terminal vault is evaluated (AC 6)** — view task `status: completed`, `phase: execution`; index item `{ID, Status: "in_progress", Phase: "execution"}`. Run `ReconcileOnce` inside `deferredGateCaptureStderr`. Assert: `strings.Count(out, "event=reconcile_refused") == 0`; the task spawned (`fakeSpawner.SpawnJobCallCount() == 1`, or a `skipped_*` line — assert the Job reference); and `strings.Count(out, "event=terminal_gate_divergence") == 1` with the captured line containing `view_status=completed`, `vault_status=in_progress`, and `decision_source=vault`.
   - **Terminal view over terminal vault is still refused (AC 7)** — view `status: completed`, `phase: execution`; index item `{ID, Status: "completed", Phase: "execution"}`. Assert `strings.Count(out, "event=reconcile_refused") == 1` and it contains `gate=terminal_status`; `strings.Count(out, "event=terminal_gate_divergence") == 0`; `fakeSpawner.SpawnJobCallCount() == 0`.
   - **Absent index entry falls back to view-only** — view `status: completed`, `phase: execution`; index stub with no matching item (or an empty `items`). Assert one `event=reconcile_refused` with `gate=terminal_status`, no divergence line, and no spawn.
   - **Failed index read falls back to view-only (failure mode)** — index stub `err` set. Assert one `event=reconcile_refused … gate=terminal_status`, no divergence line, one `event=terminal_gate_index_unavailable gate=terminal_status`, and no spawn.
   - **No index client is silent** — build without `WithVaultAPIIndexClient`; view `status: completed`, `phase: execution`. Assert one `event=reconcile_refused … gate=terminal_status`, no divergence line, and no `event=terminal_gate_index_unavailable` line.
   - **The read is shared, not doubled** — one task that the terminal gate evaluates (view `completed` / `execution`, index `in_progress` / `execution`) in a pass where the divergence check and the `index_terminal` gather gate also run; assert the stub's `reads == 1`.

4. Before finishing, re-run every command in `<verification>`. Walk the spec's Acceptance Criteria 6-7 and the relevant failure-mode row and confirm each is asserted above. Do not attempt the Post-Deploy (Rung-2) criteria; they are operator-executed after merge.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass, including `pkg/handler/index_terminal_reconcile_test.go`, `pkg/handler/view_divergence_test.go`, and `pkg/handler/task_event_handler_terminal_status_test.go`.
- The gate only decides refuse-vs-evaluate; it never spawns and never writes to the vault.
- The `terminal_status` and `index_terminal` gates stay distinct. `index_terminal` still suppresses on index evidence and fails open; `terminal_status` still refuses on view evidence. Do not conflate them, and do not change `indexGateSuppresses`.
- Only a **non-terminal** vault status overrides a terminal view. A terminal vault status, an absent entry, or a failed read keeps today's refusal.
- A failed read must not flip either gate's direction, and the new gate must reuse the pass-scoped read rather than adding a second request.
- The gate reads the cached index's **status value** (for `vault_status=`), not only the derived terminal set.
- Do not repair, converge or back-fill the view; `event=view_divergence` keeps reporting the divergence after this fix.
- Do not change the Kafka event path, the vault write-back path, the controller apply cycle, or the `TaskUpdated` contract.
- No new metric, config knob, flag, or opt-out. The only new observability is `event=terminal_gate_divergence` and `event=terminal_gate_index_unavailable`.
- Do not exceed revive's `file-length-limit` on `pkg/handler/task_event_handler.go` (2000 lines; it is at 1988). Put all new code in the new file; the call-site edit replaces the existing three-line block.
- Wrap errors with `github.com/bborbe/errors` if you introduce one (this gate returns no error).
- Use Ginkgo v2/Gomega; use Counterfeiter fakes in `package handler_test`; use `deferredGateCaptureStderr` for `V(2)` assertions.
</constraints>

<verification>
- `make precommit` — must exit 0.
- `go test ./pkg/handler/ -count=1` — must exit 0.
- `grep -rn 'decision_source=vault' pkg/handler/terminal_gate_divergence.go && echo DIVERGENCE_OK` — must print a matching line and `DIVERGENCE_OK` (the line is in production source, not only a test).
- `grep -q 'event=terminal_gate_index_unavailable' pkg/handler/terminal_gate_divergence.go && echo UNAVAILABLE_OK` — must print `UNAVAILABLE_OK`.
- `grep -q 'terminalStatusRefuses' pkg/handler/task_event_handler.go && echo WIRED_OK` — must print `WIRED_OK`.
- `go test ./pkg/handler/ -v -args -ginkgo.focus='terminal gate divergence' > /tmp/tgate.out 2>&1; grep -q 'SUCCESS!' /tmp/tgate.out && echo FOCUS_OK` — must print `FOCUS_OK`.
</verification>
