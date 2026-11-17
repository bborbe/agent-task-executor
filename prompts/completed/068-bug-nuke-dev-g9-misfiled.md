---
status: completed
spec: [bug-nuke-dev-g9]
summary: Confirmed exitErrorForStatus is absent from this repository (grep and git -S show only the spec-filing commit 30840c2); the spec is misfiled because the function lives in bborbe/github-dark-factory-agent at main.go:235 (called from finishRun at main.go:218) and its fix already exists on origin/sentry-fix/nuke-dev-g9 at af18439, so no changes were made here and the spec should be re-filed or merged in the correct repository.
execution_id: e6e20354-39e8-56a4-b3b2-f74b2b9f2628-exec-068-bug-nuke-dev-g9-misfiled
dark-factory-version: dev
created: "2026-11-17T11:46:42Z"
queued: "2026-11-17T11:52:10Z"
started: "2026-11-17T11:52:11Z"
completed: "2026-11-17T12:01:22Z"
---

<!--
OPEN QUESTIONS FOR THE REVIEWER (non-interactive run; resolved best-effort from
the spec and the repository, not from the spec's assertion):

1. THE SPEC'S TARGET DOES NOT EXIST IN THIS REPOSITORY.
   - `grep -rn "exitErrorForStatus" --include='*.go' .` returns no matches in the
     working tree.
   - `git log --all -S exitErrorForStatus --oneline` returns exactly one commit,
     `30840c2 fix(sentry): file bug spec for NUKE-DEV-G9` — the spec-filing
     commit itself. No branch in this clone has ever contained the function.
   - `main.go:235-249` in this repository is the tail of `validateClaimTransport`
     (the `gitrest-url` host check and the `cluster` check), not an error builder
     for an agent Result.
   - No non-test file in this repository references `lib.Result` or
     `agentlib.Result`; the `github.com/bborbe/agent` library is imported for
     `Task`, `TaskFrontmatter`, `AgentStatus`, and similar, but the Result struct
     the spec describes is never handled here.
   - `main.go` has no status/phase error path at all. Its only `status` use is
     `http.StatusUnauthorized` in the pprof handler.

2. THE REAL LOCATION IS `bborbe/github-dark-factory-agent`.
   - In that repository, `finishRun` calls
     `exitErrorForStatus(ctx, result.Status, phase)` at `main.go:218`.
   - `exitErrorForStatus` is defined at `main.go:235` and builds exactly the
     error the spec describes:
     `agent reported status %q for phase %q` with no `Result.Message`.
   - The execution-phase failure paths are that binary's planning/execution/
     ai_review paths, not anything in this executor.

3. THE FIX THE SPEC RECOMMENDS ALREADY EXISTS IN THAT REPOSITORY.
   - Branch `origin/sentry-fix/nuke-dev-g9`, commit
     `af18439 001-bug-nuke-dev-g9-sentry-phase-fingerprint`, branch tip
     `70ba4e7 dark-factory: complete spec(s)`.
   - The same-named branch in THIS repository points at `30840c2`, the
     spec-filing commit, and contains no code change.
   - That commit threads `result.Message` into the error, adds `phase`/`status`
     error data, and registers a per-phase Sentry fingerprint processor.

4. RECOMMENDATION: reject this spec in `bborbe/agent-task-executor` and
   re-file or merge it in `bborbe/github-dark-factory-agent`. The fix is already
   implemented and tested on that branch. This spec was auto-filed by the
   sentry-fix-agent's "candidate position 3" resolution rule, which chose this
   repository for a stack frame that belongs to the agent binary.

5. CONSEQUENCE FOR THIS PROMPT: it makes no production change. Any
   implementation here would have to invent `exitErrorForStatus`, its caller,
   the `Result` handling, and the Sentry wiring — exactly the invented-signature
   failure the auditor is meant to catch. There is no defensible executor-side
   change for this symptom.
-->

<summary>
- The function the spec names does not exist in this repository, in any branch or in the working tree.
- The cited `main.go:235-249` is a claim-transport validator, not an agent-result error builder.
- No non-test file here handles the agent `Result` type the spec describes.
- The real code lives in `bborbe/github-dark-factory-agent`, where `finishRun` calls `exitErrorForStatus`.
- The fix the spec recommends already exists on that repository's `sentry-fix/nuke-dev-g9` branch (`af18439`).
- This prompt changes no Go code, tests, changelog, or docs in this repository.
- It asks the executing agent to confirm the absence and report the misfiling instead of inventing the function.
- The reviewer should reject this spec here and re-file it against the correct repository.
</summary>

<objective>
Prevent a wrong-repository change for `NUKE-DEV-G9`. The spec was auto-filed into `bborbe/agent-task-executor` by the sentry-fix-agent's candidate-position rule, but the implicated function lives in `bborbe/github-dark-factory-agent`, where the fix is already implemented on `origin/sentry-fix/nuke-dev-g9`. This prompt makes no code change; it exists so the misfiling is caught at audit time instead of producing an invented implementation.
</objective>

<context>
Read `main.go` and confirm for yourself that `exitErrorForStatus` is absent. Lines 235-249 are inside `validateClaimTransport`, which validates `gitrest-url` and `cluster` for claim-taking modes; there is no agent-result error path anywhere in the file.

Read `pkg/result_publisher.go` (the `ResultPublisher` interface and its implementations). It publishes frontmatter commands for tasks; it never builds an error from an agent `Result`. The `github.com/bborbe/agent` import here is for `Task`, `TaskFrontmatter`, and `AgentStatus`, not for the `Result` struct the spec names.

For reference only, not for implementation in this repository: the spec's cited function is `exitErrorForStatus` in `bborbe/github-dark-factory-agent` (`main.go:235`), called from `finishRun` (`main.go:218`). The spec's recommended fix is commit `af18439` on branch `origin/sentry-fix/nuke-dev-g9` in that repository. Do not copy that code here.

Read `specs/in-progress/bug-nuke-dev-g9.md` to see the filing, including its `Reproduction` line: it cites `main.go:241` in `bborbe/agent-task-executor`, which is the comment above the `cluster` check in `validateClaimTransport`, not a failing call site.
</context>

<requirements>
1. Do not modify any file in this repository. Specifically: do not add an `exitErrorForStatus` function, do not edit `main.go`, do not edit `main_test.go` or `main_internal_test.go`, do not edit `CHANGELOG.md`, and do not add or edit anything under `docs/`.

2. Confirm the premise before concluding. Run both checks and record their output in your completion summary:
   - `grep -rn "exitErrorForStatus" --include='*.go' .` — must return no matches.
   - `git log --all -S exitErrorForStatus --oneline` — must return only the spec-filing commit (`30840c2`), not a code change.

3. Record in the completion summary that this spec is misfiled: `exitErrorForStatus` lives in `bborbe/github-dark-factory-agent` at `main.go:235` (called from `finishRun` at `main.go:218`), and the fix already exists on that repository's `origin/sentry-fix/nuke-dev-g9` branch at commit `af18439`. State that no change was made in this repository and that the spec should be re-filed or merged in the correct repository.

4. Do not create a substitute implementation, a placeholder function, a documentation note, or a changelog entry to make this prompt "produce a diff". An empty diff is the correct outcome here.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT invent `exitErrorForStatus`, its caller, the `Result` handling, or the Sentry fingerprint wiring in this repository. None of it exists here, and an invented implementation would not compile against any real call site.
- Do NOT copy code from `bborbe/github-dark-factory-agent` into this repository. The two repositories have different `main.go` files, different imports, and different responsibilities.
- Do NOT modify the spec file `specs/in-progress/bug-nuke-dev-g9.md`; its frontmatter is managed by dark-factory.
- Existing tests must still pass unchanged.
</constraints>

<verification>
- `go build ./...` — must pass, with no source changes.
- `test -z "$(grep -rn 'exitErrorForStatus' --include='*.go' . || true)"` — must exit 0, proving the function is absent from this repository.
</verification>
