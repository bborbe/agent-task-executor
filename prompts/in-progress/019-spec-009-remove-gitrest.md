---
status: failed
spec: [009-kafka-task-view-replaces-gitrest-reconcile]
created: "2026-10-21T18:40:07Z"
queued: "2026-10-21T19:39:58Z"
completed: "2026-10-21T20:24:18Z"
branch: dark-factory/kafka-task-view-replaces-gitrest-reconcile
lastFailReason: 'setup workflow: git merge origin default branch: merge origin/master: : git merge origin/master failed: exit status 1'
---

# Remove the git-rest reader, its configuration, and its documentation

<summary>
- The executor no longer has any code path that reads the vault over git-rest
- The git-rest URL, gateway-secret name, and task-glob settings are deleted from the binary
- The git-rest client package, its mock, and the gateway-secret reader are deleted
- The startup refusal on an empty task glob is deleted with the setting
- The README documents the new task-vault setting and no longer mentions git-rest or the task glob
- The changelog records the task view, the new ownership rule, and the removal
- The executor now reads task state only through Kafka, matching the single-writer doctrine
</summary>

<objective>
Delete every remaining git-rest artifact now that reconcile and ownership run on the in-memory task view: configuration fields, startup validation, the client package, the gateway-secret reader, the generated mock, and the documentation that describes the old contract.
</objective>

<context>
Read these before editing:

- `main.go` — the `application` struct fields `GitRestURL`, `GitRestGatewaySecret`, `TaskGlob`; the `ErrTaskGlobEmpty` var; the `stderrors` import (used only by that var); and `validate`.
- `pkg/factory/factory.go` — `CreateGitRestClient` and the `gitrestclient` import.
- `pkg/gitrestclient/` — `git_rest_client.go`, `git_rest_client_test.go`, `gitrestclient_suite_test.go`.
- `pkg/gitrest_gateway_secret.go`, `pkg/gitrest_gateway_secret_test.go`.
- `mocks/git_rest_client.go` (generated; `make precommit` regenerates the directory).
- `main_internal_test.go` — the `TaskGlob` field-tag spec.
- `README.md` — the "Reconcile loop configuration" section.
- `CHANGELOG.md` — the bullet style and the current top section.
- `pkg/handler/task_event_handler.go` and `pkg/factory/factory.go` — confirm no remaining reference to `gitrestclient`, `GitRestClient`, `TaskGlob`, `taskGlob`, `pathIDs`, `prioritizeUncachedPaths`, or `path=` in reconcile log lines (the previous prompt removed them).

Read these coding docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-factory-pattern.md`
</context>

<requirements>
1. **Delete the git-rest configuration from `main.go`.**
   - Remove the struct fields `GitRestURL`, `GitRestGatewaySecret`, and `TaskGlob` and their doc comments.
   - Remove `var ErrTaskGlobEmpty = stderrors.New("task-glob is empty")` and the `stderrors "errors"` import if nothing else uses it.
   - Remove any remaining reference to `a.TaskGlob` in `validate`.
   - Keep `vaultSlugRegexp`: it still validates `VAULT_NAME` and the effective task vault.
   - Reword the `ReconcilePassTimeout`/`ReconcileInterval` doc comment and the `RECONCILE_PASS_TIMEOUT` usage string so they no longer mention git-rest or vault task files (e.g. "Deadline for one reconcile pass over the in-memory task view").

2. **Delete the factory constructor.** Remove `CreateGitRestClient` from `pkg/factory/factory.go` and the now-unused `github.com/bborbe/agent-task-executor/pkg/gitrestclient` import. Do not remove `CreateTaskView`.

3. **Delete the files and packages:**
   - `pkg/gitrestclient/` (all three files)
   - `pkg/gitrest_gateway_secret.go`
   - `pkg/gitrest_gateway_secret_test.go`
   - `mocks/git_rest_client.go`
   Do not delete or edit any other mock.

4. **Update `main_internal_test.go`.** Delete the whole `Describe("empty TaskGlob", ...)` block, including its remaining spec that asserts the `TaskGlob` field tags (`env` = `TASK_GLOB`, `arg` = `task-glob`, empty `default`). The previous prompt already removed the validation spec from that block, so the container would otherwise be left empty. Keep the `VaultName`, `BuildGitVersion`, and reconcile-timing specs and the new `TASK_VAULT` specs. In the two reconcile-timing specs "rejects a non-positive pass timeout before the k8s client" and "rejects a non-positive interval before the k8s client", delete the `TaskGlob: "t/*.md",` line from each `&application{...}` literal (the field no longer exists); keep their `VaultName` and timing fields and their assertions unchanged.

5. **Update `README.md`.** Replace the "Reconcile loop configuration" section with a section that describes the Kafka task view. It must:
   - State that the reconcile floor reads an in-memory view rebuilt at startup from `{TOPIC_PREFIX}-agent-task-v1-event` and kept current from the same topic, and that the executor no longer reads git-rest.
   - Include a table with these rows (exact env names):
     | Env var | Default | Meaning |
     | `TASK_VAULT` | `VAULT_NAME` | Vault slug this executor owns tasks for. Must match `^[a-z][a-z0-9-]*$`. A task is owned when its stamped `vault` equals this value; a task with no stamped vault is never owned. A ready view that holds tasks but none for this vault logs `event=task_vault_mismatch` and drives nothing; a view in which every task is unstamped logs `event=task_vault_unstamped` and drives nothing. |
     | `RECONCILE_INTERVAL` | `60s` | Interval between reconcile passes. |
     | `RECONCILE_PASS_TIMEOUT` | `45s` | Deadline for one reconcile pass. |
   - Mention that the view becomes ready once every partition has replayed to the high-water mark captured at startup, and that a reader failure exits the process so the pod restart rebuilds the view.
   - Remove every mention of `GITREST_URL`, `GITREST_GATEWAY_SECRET`, and `TASK_GLOB`, including the ServiceAccount/Secret-grant sentence.

6. **Update `CHANGELOG.md`.** Add a `## Unreleased` section above the newest released section if one does not exist, and add one bullet per logical change (prefix required):
   - `feat:` the reconcile floor now reads an in-memory task view rebuilt from the compacted `{TOPIC_PREFIX}-agent-task-v1-event` topic instead of listing and fetching vault files over git-rest; a pass over ~15,000 tasks is an in-memory scan and no longer aborts at the pass timeout.
   - `feat:` task ownership is now `Task.Vault == TASK_VAULT` (defaults to `VAULT_NAME`) on both the live event path and the reconcile path, with loud `task_vault_mismatch` / `task_vault_unstamped` pass outcomes and `skipped_foreign_vault` on the event path.
   - `refactor:` remove the git-rest client, the gateway-secret reader, and the `GITREST_URL` / `GITREST_GATEWAY_SECRET` / `TASK_GLOB` settings; the executor now reads task state only through Kafka.
   Name the observable metrics and log lines. Do not copy `<verification>` commands into the changelog.

7. **Confirm the removal is complete.** Run `go build ./...`, `go vet ./...`, and the test suite. `make precommit` regenerates `mocks/` from the counterfeiter annotations, so the git-rest mock must not reappear.
</requirements>

<constraints>
- This is a pure removal plus documentation change. Do not change reconcile behaviour, ownership, metrics labels, log formats, the live consumer, or the task view.
- Do not delete `CreateTaskView`, `TaskView`, or any task-view test.
- Do not add a replacement git-rest client, a compatibility shim, or an opt-out flag.
- Do not change the Helm chart or deployment manifests; they live in `bborbe/agent` and are out of scope.
- Do not change `RECONCILE_INTERVAL` / `RECONCILE_PASS_TIMEOUT` defaults or validation.
- Use `github.com/bborbe/errors` conventions anywhere an error is wrapped; this prompt should need no new error path.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `cd /workspace && ROOTDIR=/workspace make precommit` — must pass.

Then confirm the negative evidence (do not use `git grep`; `.git` is masked in this container):

- `! grep -rqE 'gitrestclient|GITREST_URL|TASK_GLOB' --include='*.go' --include='*.md' --exclude=CHANGELOG.md --exclude-dir=specs --exclude-dir=prompts .` — must find nothing.
- `! grep -rqE 'GITREST_GATEWAY_SECRET|ErrTaskGlobEmpty|CreateGitRestClient|ReadGitRestGatewaySecret|prioritizeUncachedPaths|pathIDs|TaskGlob' --include='*.go' --include='*.md' --exclude=CHANGELOG.md --exclude-dir=specs --exclude-dir=prompts .` — must find nothing.
- `! test -e pkg/gitrestclient` — the package is gone.
- `! test -e pkg/gitrest_gateway_secret.go` — the secret reader is gone.
- `! test -e mocks/git_rest_client.go` — the generated mock is gone.

Then confirm the documentation:

- `grep -n 'TASK_VAULT' README.md` — prints at least one line naming the `VAULT_NAME` default.
- `! grep -qE 'GITREST_|TASK_GLOB' README.md` — no old setting remains.
- `grep -q '^## Unreleased' CHANGELOG.md` — the section exists.
- `awk '/^## /{if (sec=="Unreleased") exit} /^## Unreleased/{sec="Unreleased"; next} sec=="Unreleased" && /^- (feat|fix|refactor|test|docs|chore|perf):/' CHANGELOG.md | grep -q '^- '` — the Unreleased section has at least one prefixed bullet.

Then confirm the code still builds and the new contract is intact:

- `go test -mod=mod ./...` — exits 0.
- `grep -q 'event=reconcile evaluated=%d re_driven=%d deferred=%d skipped=%d capped=%d task_vault=%q' pkg/handler/task_event_handler.go` — the summary line survived the removal.
- `! grep -qE 'event=(reconcile_redrive|redrive_capped|reconcile_skip)[^"]*path=' pkg/handler/task_event_handler.go` — no log line carries a path.

Do NOT run `docker`, `make build`, `kubectl`, `dark-factory`, `gh`, or `scripts/*.sh` in this prompt — those are operator-executable.
- `! grep -niE 'git-rest|gitrest' main.go pkg/factory/factory.go pkg/handler/task_event_handler.go` — no git-rest wording remains in live code.
</verification>
