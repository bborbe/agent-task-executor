---
status: completed
spec: [017-view-divergence-detector]
summary: Added an advisory per-pass reconcile divergence check joining owned view tasks to vault-api's task index on status and phase, with separate absent events, a coverage summary, option-based wiring, docs and table tests.
execution_id: agent-task-executor-view-divergence-exec-043-spec-017-view-divergence-comparison
dark-factory-version: v0.198.7
created: "2026-10-27T00:00:00Z"
queued: "2026-10-26T23:15:33Z"
started: "2026-10-27T19:43:07Z"
completed: "2026-10-27T19:53:39Z"
---

# Compare the reconcile snapshot to the vault index and report divergence

<summary>
- A reconcile pass compares the tasks it owns against the vault's task index
- Divergence is reported per task id with both sides of status and phase
- A difference in either field is a divergence; a match on both fields is silent
- An owned id missing from the index is reported as absent, separately from a divergence
- One summary line per pass states how many ids were compared, how many diverged, the index age and the index warnings
- A failed index read is logged and the pass still drives every candidate
- No index read happens before the pass's early returns or after its budget is spent
- A deployment without a vault-api client makes no call and emits nothing
- The check never changes eligibility, admission, capping or spawning
</summary>

<objective>
Join the reconcile pass's owned view snapshot to vault-api's task index, compare both `status` and `phase`, and report per-id divergence plus one coverage summary per pass. The check is advisory: it feeds nothing back into the pass and never writes to the vault.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for execution rules. This repo has no project-level `CLAUDE.md`; `README.md` covers routing and admission only.

Then read these, in this order:

- `pkg/handler/task_event_handler.go` — `ReconcileOnce` in full, especially the early returns (`view_not_ready`, `task_vault_unstamped`, `task_vault_mismatch`), `ownedTasks`, `gatherCandidates`, `sortCandidatesByAdmissionPriority`, `passDone`, and the drive loop. The check goes after `sortCandidatesByAdmissionPriority(candidates)` and before `for _, candidate := range candidates`, guarded by `!h.passDone(passCtx)`. The file is 1,985 lines against `revive`'s 2,000-line limit, and `skipComments: false` so comments count: the struct field plus its two comment lines add 3, and requirement 4's call site (two comment lines plus three code lines) adds 5, leaving 1,993. Do not add anything else to this file; a longer call-site comment or an extra blank line block trips the limit.
- `pkg/handler/task_event_handler_redrive.go` — `taskPhaseString`. Reuse it; do not add a second phase helper.
- `pkg/handler/task_view.go` — `TaskView.Snapshot()` returns `[]lib.Task`; `lib.Task` carries `TaskIdentifier`, `Vault` and `Frontmatter` (`lib.TaskFrontmatter`, a `map[string]any`). `Frontmatter.Status()` returns `domain.TaskStatus`; `Frontmatter.Phase()` returns `*domain.TaskPhase`.
- `pkg/handler/task_event_handler_options.go` — the `HandlerOption` pattern. Adding an optional dependency here keeps every existing `NewTaskEventHandler` call site unchanged.
- `pkg/claim/vault_api_index_client.go` — the client added by prompt 1: `VaultAPIIndexClient.Read(ctx) (*TaskIndex, error)`, `TaskIndex.Items []TaskIndexItem{ID, Status, Phase}`, `IndexAgeSeconds`, `Warnings`.
- `pkg/factory/factory.go` — `CreateConsumer` and its call to `NewTaskEventHandler`.
- `main.go` — the `if claimMode.TakesClaim()` block that reads `vaultAPIGatewaySecretValue` and builds `claimClientFactory`, and the `factory.CreateConsumer(...)` call.
- `pkg/handler/task_reconcile_test.go` — the fixture style for the internal unit test's neighbours.
- `pkg/handler/redrive_streak_cache_internal_test.go` — the header comment explaining why internal test files use plain `testing`, not Ginkgo.

Coding docs:
`/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`,
`/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`,
`/home/node/.claude/plugins/marketplaces/coding/docs/go-doc-best-practices.md`,
`/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md`,
`/home/node/.claude/plugins/marketplaces/coding/docs/go-functional-options-pattern.md`.
</context>

<requirements>
1. **Add `pkg/handler/view_divergence.go`** (package `handler`) with the pure comparison and the pass-level check.

   Types:
   ```go
   type viewDivergenceFinding struct {
       ID          string
       Absent      bool
       ViewStatus  string
       ViewPhase   string
       VaultStatus string
       VaultPhase  string
   }

   type viewDivergenceReport struct {
       Compared  int
       Divergent int
       Absent    int
       Findings  []viewDivergenceFinding
   }
   ```

   `compareViewToIndex(owned []lib.Task, index *claim.TaskIndex) viewDivergenceReport`:
   - Index `index.Items` by `item.ID`. A duplicate id keeps the last item; that is an arbitrary tie-break for a map build, not a contract — do not add an error path or a test for it.
   - For each owned task, `id := string(task.TaskIdentifier)`.
   - Not in the map → append a finding with `Absent: true`, `ViewStatus: string(task.Frontmatter.Status())`, `ViewPhase: taskPhaseString(task)`; increment `Absent`; do **not** increment `Compared`.
   - In the map → increment `Compared`; compute `viewStatus := string(task.Frontmatter.Status())` and `viewPhase := taskPhaseString(task)`. If `viewStatus == item.Status && viewPhase == item.Phase`, append nothing. Otherwise increment `Divergent` and append a finding carrying `ViewStatus`, `VaultStatus: item.Status`, `ViewPhase`, `VaultPhase: item.Phase`.
   - Sort `Findings` by `ID` ascending so the emitted log order is deterministic.
   - Compare **canonical** values on both sides. `task.Frontmatter.Status()` and `taskPhaseString` normalize legacy aliases (`todo`→`next`, `done`→`completed`, `deferred`→`hold`, and the phase equivalents), so the index side must be canonical too. Vault-api reads the same `vault-cli` domain values, so this holds — but it is an assumption, not a contract. Record the assumption in requirement 6's `## Divergence check` section, and add one row in requirement 7's table whose view carries a legacy `status: todo` and whose index carries `next`, asserting `Divergent == 0` and `Compared == 1`.
   - A difference in **either** field is a divergence. Do not short-circuit on the first matching field; a status match with a phase mismatch is still a divergence, and the finding carries both sides of both fields so a reader can see which moved.

2. **`checkViewDivergence`** (method on `*taskEventHandler`):
   ```go
   func (h *taskEventHandler) checkViewDivergence(ctx context.Context, owned []lib.Task)
   ```
   - `h.vaultIndexClient == nil` → return immediately. No log, no metric. This is the no-URL deployment's shape.
   - `index, err := h.vaultIndexClient.Read(ctx)`; on `err != nil || index == nil`, `glog.Warningf("event=view_divergence_read_error error=%v", err)` and return. A nil index is treated as a read failure, not as an empty index: a nil deref inside `ReconcileOnce` is the one outcome the spec forbids. Do **not** return the error to `ReconcileOnce`: a read failure is a finding, not a pass failure, and `ReconcileOnce` must keep returning `nil` so `service.Run` cannot tear the executor down.
   - `report := compareViewToIndex(owned, index)`.
   - For each finding, log at warning level:
     - absent: `event=view_divergence_absent task=%s view_status=%s view_phase=%s`
     - divergent: `event=view_divergence task=%s view_status=%s vault_status=%s view_phase=%s vault_phase=%s`
   - After the findings, one summary at info level: `event=view_divergence_summary compared=%d divergent=%d index_age_seconds=%d warnings=%d` using `report.Compared`, `report.Divergent`, `index.IndexAgeSeconds`, `index.Warnings`.
   - Log only task identifiers, statuses and phases. Never a title, assignee or frontmatter body.
   - The `event=view_divergence_absent` name is deliberate: AC 1 greps `event=view_divergence task=<id>`, which must not match an absent id. Do not collapse absent into the divergence line.

3. **Handler field and option.**
   - Add `vaultIndexClient claim.VaultAPIIndexClient` to `taskEventHandler`, with a doc comment saying nil means the deployment has no vault-api URL and the check is skipped.
   - Add to `pkg/handler/task_event_handler_options.go`:
     ```go
     // WithVaultAPIIndexClient supplies the vault-api task-index reader used by
     // the reconcile pass's view/vault divergence check. A nil client (the
     // default) skips the check, which is the no-URL deployment's shape.
     func WithVaultAPIIndexClient(client claim.VaultAPIIndexClient) HandlerOption {
         return func(h *taskEventHandler) {
             h.vaultIndexClient = client
         }
     }
     ```
   - Do not add a positional parameter to `NewTaskEventHandler`; every existing call site must stay untouched.

4. **Call site in `ReconcileOnce`.** Insert exactly this between `sortCandidatesByAdmissionPriority(candidates)` and the drive loop:
   ```go
   // One vault-api index read per pass that reaches the drive loop. Advisory:
   // a read failure is logged and the pass drives its candidates unchanged.
   if !h.passDone(passCtx) {
       h.checkViewDivergence(passCtx, owned)
   }
   ```
   Do not move it above the vault-classification early returns: `view_not_ready`, `task_vault_unstamped` and `task_vault_mismatch` must make no call. Do not place it after the drive loop: a busy pass would then skip it. The `passDone` guard is the drive loop's own, so a pass that has already spent its budget makes no call.

5. **Wiring.**
   - `pkg/factory/factory.go`: add a final parameter `vaultIndexClient claim.VaultAPIIndexClient` to `CreateConsumer`, after `claimClientFactory`, and pass `handler.WithVaultAPIIndexClient(vaultIndexClient)` in the `NewTaskEventHandler` option list.
   - `main.go`: declare `var vaultAPIIndexClient claim.VaultAPIIndexClient` beside `var claimClientFactory pkg.ClaimClientFactory`. Inside the existing `if claimMode.TakesClaim()` block, after `vaultAPIGatewaySecretValue` is read and `claimClientFactory` is built, add:
     ```go
     vaultAPIIndexClient = claim.NewVaultAPIIndexClient(
         a.VaultAPIURL,
         vaultAPIGatewaySecretValue,
         "agent-task-executor",
     )
     ```
     Pass `vaultAPIIndexClient` as the last argument to `factory.CreateConsumer(...)`. Add the `claim` import.
   - The block already errors when `a.VaultAPIURL == ""` in a claim-taking mode, so the client is never built without a URL. A non-claim deployment leaves the variable nil, and `checkViewDivergence` emits nothing. This is the "no URL, no client" behaviour; do not build the client outside the claim block.

6. **Document the semantics** in `docs/vault-api-task-index.md` (created by prompt 1). Add a `## Divergence check` section:
   - the join key: owned view tasks' `task_identifier` to `items[].id`;
   - both `status` and `phase` are compared, and a difference in either is a divergence;
   - the per-id events (`event=view_divergence`, `event=view_divergence_absent`) and the exact fields they carry;
   - the summary event and its four fields;
   - `compared` counts ids actually joined, not owned ids, so a failed or empty index reads as `compared=0`, never as agreement;
   - one read per pass, no read before the early returns, no read after the budget expires;
   - a read failure is logged and the pass continues. Do not restate the envelope section.

7. **Unit tests in `pkg/handler/view_divergence_internal_test.go`** (`package handler`, **plain `testing`, not Ginkgo** — specs declared in an internal test package of this repository are not discovered by the package-external suite, see `redrive_streak_cache_internal_test.go`). Cover `compareViewToIndex` with a table. `checkViewDivergence`'s own boundary — the `Read` call, the nil-client skip and the three log events — is covered end to end by prompt 4's `httptest` suite; do not duplicate it here.
   - status-only difference on one of three owned ids → `Compared == 3`, `Divergent == 1`, one finding carrying both statuses and equal phases.
   - phase-only difference → `Divergent == 1`, both phases, equal statuses.
   - both match → `Divergent == 0`, `Findings` empty.
   - a legacy view alias against a canonical index value → view `status: todo`, index `status: next` → `Divergent == 0`, `Compared == 1`. This pins the canonical-comparison assumption from requirement 1: `Status()` normalizes `todo` to `next`, so the index side must normalize identically or the check reports a permanent false divergence.
   - both fields differ → exactly one finding, `Divergent == 1`, all four sides present.
   - one owned id missing from the index → `Absent == 1`, `Compared == 2`, finding `Absent == true`.
   - empty index → `Compared == 0`, `Absent == 3`.
   - findings are ordered by id.
   Build `lib.Task` values directly (`lib.Task{TaskIdentifier: ..., Frontmatter: lib.TaskFrontmatter{"status": ..., "phase": ...}}`). Do not import `mocks`: it imports `pkg/handler`, so an internal test cannot use it.
</requirements>

<constraints>
- Do NOT change eligibility, admission order, capping, spawning, or the candidate set. The check feeds nothing back.
- Do NOT write to the vault, re-publish an event, or mutate the view.
- Do NOT filter the index request.
- Do NOT extend the pass budget; the call runs under `passCtx`.
- Do NOT return an index read error from `ReconcileOnce`. Log it and continue.
- Do NOT add the metric in this prompt. Prompt 3 adds `executor_view_divergence_total` and its increments.
- Do NOT emit a per-id line for an id whose status and phase both agree.
- Do NOT log titles, assignees, or frontmatter bodies.
- Do NOT add a positional parameter to `NewTaskEventHandler`; use the `HandlerOption`.
- Keep `pkg/handler/task_event_handler.go` under the 2,000-line `revive` limit; the implementation belongs in the new file.
- Add the standard license header to every new Go file.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
ROOTDIR=/workspace make precommit
go test -mod=mod -count=1 ./pkg/handler/
grep -n 'event=view_divergence' pkg/handler/view_divergence.go
</verification>
