---
status: completed
summary: Executor now refuses to start when TASK_GLOB is empty (sentinel ErrTaskGlobEmpty, checked above the K8s client) and the built-in '24 Tasks/*.md' default was removed from the flag, README, and doc comment
execution_id: agent-task-executor-taskglob-exec-011-fail-loud-on-empty-task-glob
dark-factory-version: v0.193.0
created: "2026-09-13T17:10:00Z"
queued: "2026-09-13T16:21:30Z"
started: "2026-09-13T16:22:18Z"
completed: "2026-09-13T16:27:33Z"
---

# Fail loudly on an empty TASK_GLOB instead of evaluating zero tasks

<summary>
- The executor refuses to start when no task glob is configured
- An unset or empty glob exits non-zero with an error naming the flag
- The binary no longer ships a default naming any vault directory
- Operators upgrading see a loud failure instead of a silently idle fleet
- Existing deployments that set the glob are unaffected
</summary>

<objective>
Make the executor fail loudly when `TASK_GLOB` is empty. A misconfigured executor currently looks like a healthy quiet fleet: when the glob points at a vault path that no longer exists the reconcile loop logs `evaluated=0` with no error, so a renamed vault folder is indistinguishable from a day with no work. On 2026-09-13 a vault renumber left every executor reading a dead path and dispatching nothing for ~75 minutes before anyone noticed. The executor is the only component that knows the glob is unset, so it is the component that must refuse.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for project conventions (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, glog `V(n)` gating, ≥80% coverage for new code). The coding plugin docs at `/home/node/.claude/plugins/marketplaces/coding/docs/` hold `go-error-wrapping-guide.md`, `go-testing-guide.md`, `go-k8s-binary-conventions.md` (application-struct tag semantics — `default:` is a fallback applied only when the env/arg is **absent**), and `changelog-guide.md` (`## Unreleased` goes directly above the highest `## vX.Y.Z`; conventional-prefix table).

`main.go` declares the executor's flags; `TaskGlob` is the `TASK_GLOB` env/arg with an inline `default:"24 Tasks/*.md"`. That default is a vault directory the binary cannot know, and it is being removed in the sibling chart change (`bborbe/agent`, prompt 216) — after which consumers supply the glob explicitly. This prompt removes the default here and adds the guard that makes an empty value loud.

`pkg/gitrestclient/git_rest_client.go` `List(ctx, glob)` sets the glob as a URL query parameter with no client-side validation. **git-rest treats an empty pattern as match-everything** — `GET /api/v1/files/?glob=` is routed to the list handler and returns every file in the repo (git-rest `pkg/git/git.go` `ListFiles`) — so an unset glob silently **widens** the reconcile loop to the whole vault rather than failing or no-opping. The guard belongs at startup, not in `List` — a glob that matches nothing is a legitimate result for a live glob (the loop logs `evaluated=0` and no-ops), so the client cannot distinguish "nothing matched this pass" from "nothing was configured".

The sibling `bborbe/agent` change is a separate dark-factory project with its own worktree. Do not edit it here.

**Sequencing — this release must land before the chart change is safe to inherit.** The flag's inline `default:` applies only when `TASK_GLOB` is **absent**, so a set-but-empty value is already an empty glob at runtime (verified: `env TASK_GLOB="" …` logs `Argument: TaskGlob ''`). The chart-side change renders `value: ""` — present-but-empty — so a fleet deployed from the new chart before this guard ships runs the reconcile loop with an empty pattern, which git-rest treats as **match-everything over the whole vault**. The chart-side prompt is completed but **not merged** — `bborbe/agent` master still carries the default. Whoever merges should sequence: release this guard first, then the chart.
</context>

<requirements>
1. In `main.go`, remove `default:"24 Tasks/*.md"` from the `TaskGlob` flag. The flag stays `required:"false"` on the wire — the binary must still start from an env var or arg, and this change must not alter the flag's name, env var, or arg spelling.

2. Make an empty `TaskGlob` **fail at startup**: the process exits non-zero and the message names the flag and the reason. Follow the repo's existing error convention — `errors.Wrapf` from `github.com/bborbe/errors`, returned up so `service.Main` handles it. For the shape, read the startup checks in `main.go` (`errors.Errorf` for the inline checks) and `ErrConfigNotFound` in `pkg/config_resolver.go` for the package-level sentinel pattern. A sentinel error plus an explanatory message is appropriate here; the message must let an operator reading only the log line know which setting to fix.

3. Validation runs **before** any Kafka consumer, K8s client, or HTTP listener is constructed, so a misconfigured pod fails fast rather than consuming a topic and then idling. Reference `factory.CreateConsumer(...)` in `main.go` (it currently receives `a.TaskGlob`) — the check must sit with the existing startup checks **above `rest.InClusterConfig()`**, i.e. before the K8s client and long before that call.

4. Update the `TASK_GLOB` row in `README.md` so its documented default is empty and it states the flag is now required. Keep the existing table shape and the one-line description style.

5. Update the `TaskGlob` doc comment in `main.go` so it no longer implies a vault directory. The current comment says the glob is "consumed by the reconcile loop in the follow-up prompt" — correct that if it is now stale, and do not restate the removed default anywhere.

6. Add a `## Unreleased` section to `CHANGELOG.md` directly above the top version heading, with a **`fix:`-prefixed** bullet covering the empty-glob guard and the removal of the built-in default (dark-factory derives the version bump from the prefix). Do **not** bump any version string.

7. Tests: use **Ginkgo/Gomega v2** (`github.com/onsi/ginkgo/v2`, `github.com/onsi/gomega`) — every `_test.go` in this repo uses them and there are no stdlib table tests. The test must live in **`package main`**, extending `main_internal_test.go`: `application` is unexported, so an external `main_test` suite cannot construct it. Calling `Run` with an empty `TaskGlob` proves the guard precedes the K8s client **only if the test asserts which error came back** — off-cluster `rest.InClusterConfig()` fails too, so a bare `HaveOccurred()` passes in both orderings. Assert the error names the glob setting (e.g. `MatchError(ContainSubstring("task-glob"))`). A test that only asserts the flag's string value does not satisfy this — the test must exercise the validation path itself. Coverage for new code: ≥80%.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git
- Existing tests must still pass
- Repo-relative paths for repo files; never host-absolute or home-relative paths (`/Users/...`, `~/...`). Container-local scratch under `/tmp` is fine (the build output in `<verification>`)
- Do not edit `bborbe/agent` — separate repo, separate worktree, separate prompt
- Do not change the flag's name, env var name, or arg spelling — only its default and its validation
- A non-empty glob must behave exactly as before; this change adds a guard, it does not alter glob handling
- Do not bump version strings
- Never run `go mod vendor`; use `-mod=mod` for any `go test` command that needs it
</constraints>

<verification>
Run `make precommit` -- must pass.

Then confirm the default is gone from the flag declaration and the guard exists:

- `! grep -rq 'default:"24 Tasks/\*\.md"' main.go` -- must succeed (the built-in default is removed)
- `! grep -rq '24 Tasks/' main.go README.md` -- must succeed (no vault directory named in either file)

Then confirm the guard actually fires, by building the binary and running it with the glob unset. Use `! ...` for the absence assertion — `grep -c` exits 1 on a zero count and would fail the step it was meant to pass:

- `go build -o /tmp/agent-task-executor .` -- must succeed
- `env SENTRY_DSN="http://public@example.com/1" LISTEN=":8080" KAFKA_BROKERS="localhost:9092" BRANCH="develop" NAMESPACE="default" VAULT_NAME="personal" TASK_GLOB="" /tmp/agent-task-executor; echo "exit=$?"` -- must print a **non-zero** `exit=` and a message naming the glob setting. This needs no cluster and no network: off-cluster the binary currently exits 1 at `get in-cluster k8s config`, which is exactly the slot the guard must precede.

**This run is the contract test for flag wiring *and* for guard ordering.** If the message is the k8s one rather than the glob one, the guard sits after `rest.InClusterConfig()` — fix the ordering, do not report a pass. The captured output must contain `task-glob` or `TASK_GLOB`, so this is a string contract rather than a free-text judgement.

Also confirm the change is recorded, not just absent:

- `grep -q 'TaskGlob' main.go` -- must succeed
- `! grep -q 'follow-up prompt' <(grep -A3 '// TaskGlob is' main.go)` -- must succeed (scoped deliberately: the same stale phrase legitimately remains on `GitRestURL` and `GitRestGatewaySecret`, which are out of scope)
- `grep -q 'TASK_GLOB' README.md` -- must succeed (without this, deleting the row entirely would pass)
- `grep -q '^## Unreleased' CHANGELOG.md` -- must succeed
- `grep -A5 '^## Unreleased' CHANGELOG.md | grep -qE '^- (feat|fix|refactor|test|docs|chore|perf):'` -- must succeed (scoped to the new section — the unscoped form matches pre-existing entries, so it would pass even when the new bullet has no prefix; the conventional prefix is what dark-factory parses to pick the bump)

Before finishing, re-run `<verification>` and confirm it passes; walk each acceptance criterion against the change.
</verification>
