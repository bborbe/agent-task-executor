---
status: completed
spec: [009-bug-parked-tasks-never-redrive]
summary: Added ResolveByTaskType to the ConfigResolver interface and configResolver, with ErrConfigAmbiguous for multi-match and regenerated counterfeiter mock plus 9 new specs.
execution_id: agent-task-executor-redrive-parked-exec-015-spec-009-resolve-config-by-task-type
dark-factory-version: v0.198.7
created: "2026-10-14T21:41:00Z"
queued: "2026-10-14T21:47:48Z"
started: "2026-10-14T21:47:49Z"
completed: "2026-10-14T21:52:43Z"
---

<summary>
- The executor's Config resolver gains a second lookup: resolve an agent Config by a task's `task_type`, not only by its `assignee`.
- This is the routing seam the recovery path needs — a task whose own agent cleared its `assignee` before parking it still carries a `task_type` that names exactly one agent.
- A `task_type` declared via the deprecated singular `taskType` field and one declared via the `taskTypes` list resolve identically; the lookup uses the same union the existing type filter already uses.
- When no Config declares the requested `task_type`, the lookup returns the existing not-found sentinel — no new error class for the ordinary miss.
- When more than one Config declares the same `task_type`, the lookup refuses to guess and returns a new ambiguous-config sentinel, so the caller can skip and log rather than pick arbitrarily.
- An empty `task_type` never matches anything, and a Config that declares no task type at all is never selected.
- The existing resolve-by-assignee path is untouched — every current caller behaves exactly as it does today.
- No Config CRD field, environment variable, CLI flag, or new Config struct field is added; the CRD contract in `docs/agent-crd-specification.md` stays as written.
- No user-visible behavior changes in this prompt: nothing yet calls the new lookup, so no task is written and no Job is spawned differently.
</summary>

<objective>
Add `ResolveByTaskType` to the `ConfigResolver` interface and implement it on `configResolver`, so a Config can be resolved from a task's `task_type` alone. This establishes the routing contract the reconcile floor consumes in the sibling prompt that follows: a parked task's `task_type` still identifies exactly one agent even though its `assignee` was cleared. Resolution is deliberately vault-agnostic across the executor's Config store, and an ambiguous `task_type` is reported as an error rather than resolved arbitrarily.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for project conventions (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping with `errors.Wrapf(ctx, err, ...)`, sentinel errors aliased as `stderrors`, counterfeiter mocks, coverage ≥80% for new code).

Read these files fully before changing anything:
- `/workspace/pkg/config_resolver.go` — the whole file. It is ~140 lines. Key anchors: `ErrConfigNotFound` (line 20), the `//counterfeiter:generate` annotation for `ConfigResolver` (line 22), the `ConfigResolver` interface (lines 26-28), `NewConfigResolver` (lines 38-44), the `configResolver` struct (lines 46-50), `Resolve` (lines 52-101), and `convert` (lines 103-127). Note that `stderrors "errors"` and `agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"` are already imported.
- `/workspace/pkg/task_type_filter.go` — the whole file (~38 lines). It already contains the two primitives this lookup must reuse: `func EffectiveTaskTypes(taskType string, taskTypes []string) []string` (lines 10-24, the ordered dedup union of the singular `taskType` and the `taskTypes` list) and `func TaskTypeInSet(taskType string, effectiveTypes []string) bool` (lines 28-38, which returns `false` for an empty `taskType`).
- `/workspace/pkg/config_resolver_test.go` — the whole file (~253 lines). The test harness to reuse: `fakeProvider` (lines 19-26, an in-memory `k8s.Provider[agentv1.Config]` with a settable `items` and `err`), and the `BeforeEach` at lines 35-39 which builds `resolver = pkg.NewConfigResolver(provider, "dev", "personal")`. Note the existing "resolves a Config CR by the composed assignee" spec (lines 174-189) — it shows the real production shape: `Spec.Assignee` is the composed `{assignee}-{vaultName}` value and `convert` copies it verbatim into `AgentConfiguration.Assignee`.
- `/workspace/pkg/agent_configuration.go` — the `AgentConfiguration` struct (lines 28-84) and `appendBranchTag` (lines 20-25). `convert` appends the branch tag to `Image`; specs must assert the tagged form (e.g. `foo/bar:dev`).

Verified library signatures (read from the module source at `/home/node/go/pkg/mod/github.com/bborbe/agent@v0.86.0/`, do not re-derive from memory):
- `func (f TaskFrontmatter) TaskType() TaskType` where `type TaskType string` — `agent_task-frontmatter.go` line 45 and `agent_task-type.go` line 19.
- `TaskType` has `func (t TaskType) String() string` — `agent_task-type.go` lines 21-23.
- `agentv1.ConfigSpec` carries `TaskType string` (json `taskType`) and `TaskTypes []string` (json `taskTypes,omitempty`) — `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/types.go` lines 114-117.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `bborbe/errors` API, sentinel errors with the `stderrors` alias, never `fmt.Errorf`.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md` — public interface + private struct + `New*` constructor, counterfeiter annotations.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega, external test packages (`package pkg_test`), counterfeiter mocks, coverage ≥80%.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-mocking-guide.md` — counterfeiter mock regeneration.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-doc-best-practices.md` — GoDoc comment style (start with the name, full sentences).
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new code.

Open question for the human reviewer (do NOT resolve it in code — implement exactly what the requirements below state): the task-type lookup scans every Config the executor's informer holds, which is every Config in the executor's namespace (`pkg/k8s_connector.go`, `Listen` — `agentinformers.WithNamespace(namespace.String())`, no vault filter). If a namespace holds Configs for more than one vault and two of them declare the same `task_type`, this lookup reports `ErrConfigAmbiguous` and the caller skips — the spec's Failure Modes table names that outcome as the intended degradation. If in production each `task_type` is unique across the namespace, no change is needed; if it is not, the reviewer may want a vault-scoped preference (prefer the Config whose `Spec.Assignee` carries the `-{vaultName}` suffix) as a follow-up. Do not add vault scoping here — the spec's Failure Modes table defines multi-match as ambiguity.
</context>

<requirements>
1. In `/workspace/pkg/config_resolver.go`, add a new exported sentinel error directly below the existing `ErrConfigNotFound` (line 20), matching its style. The file already imports `stderrors "errors"`:
   ```go
   // ErrConfigAmbiguous is returned by ConfigResolver.ResolveByTaskType when more
   // than one Config in the store declares the requested task_type. The executor
   // does not guess between them: the ambiguous task is left untouched and the
   // skip is logged, so an operator can narrow the Configs' taskTypes.
   var ErrConfigAmbiguous = stderrors.New("config ambiguous")
   ```

2. In the same file, extend the `ConfigResolver` interface (lines 26-28) with a second method. Keep `Resolve` exactly as it is and add the new method after it. Also widen the interface's GoDoc from "for an assignee" to cover both lookups:
   ```go
   // ConfigResolver looks up the AgentConfiguration for an assignee, and — on
   // the recovery path — for a task's task_type.
   type ConfigResolver interface {
   	Resolve(ctx context.Context, assignee string) (AgentConfiguration, error)
   	// ResolveByTaskType looks up the single AgentConfiguration whose effective
   	// task-type set ({Spec.TaskType} ∪ Spec.TaskTypes) contains taskType. It is
   	// the recovery path's routing lookup for a task whose assignee was cleared:
   	// the task's own task_type still identifies exactly one agent.
   	//
   	// Returns ErrConfigNotFound when no Config declares taskType, and
   	// ErrConfigAmbiguous when more than one does.
   	ResolveByTaskType(ctx context.Context, taskType string) (AgentConfiguration, error)
   }
   ```

3. Implement `ResolveByTaskType` on `*configResolver` in the same file, placed after `Resolve` (after line 101) and before `convert` (line 103). It must reuse the existing `EffectiveTaskTypes` and `TaskTypeInSet` helpers from `pkg/task_type_filter.go` — do NOT reimplement the set logic, and do NOT reuse `Resolve`'s composed/plain assignee matching (this lookup is keyed on the task type, not the assignee):
   ```go
   func (r *configResolver) ResolveByTaskType(
   	ctx context.Context,
   	taskType string,
   ) (AgentConfiguration, error) {
   	items, err := r.provider.Get(ctx)
   	if err != nil {
   		return AgentConfiguration{}, errors.Wrapf(ctx, err, "list agent configs")
   	}
   	var matches []agentv1.Config
   	for _, it := range items {
   		select {
   		case <-ctx.Done():
   			return AgentConfiguration{}, errors.Wrapf(
   				ctx,
   				ctx.Err(),
   				"resolve config by task_type %q cancelled",
   				taskType,
   			)
   		default:
   		}
   		if TaskTypeInSet(taskType, EffectiveTaskTypes(it.Spec.TaskType, it.Spec.TaskTypes)) {
   			matches = append(matches, it)
   		}
   	}
   	switch len(matches) {
   	case 0:
   		return AgentConfiguration{}, errors.Wrapf(
   			ctx,
   			ErrConfigNotFound,
   			"find config for task_type %q",
   			taskType,
   		)
   	case 1:
   		return convert(ctx, matches[0], r.branch.String())
   	default:
   		return AgentConfiguration{}, errors.Wrapf(
   			ctx,
   			ErrConfigAmbiguous,
   			"task_type %q matches %d configs",
   			taskType,
   			len(matches),
   		)
   	}
   }
   ```
   Behavioral facts this relies on, both verified in `/workspace/pkg/task_type_filter.go`: `TaskTypeInSet("", anything)` returns `false`, so an empty `taskType` never matches (the `len(matches) == 0` branch returns `ErrConfigNotFound`); and `EffectiveTaskTypes("", nil)` returns `nil`, so a Config that declares neither `taskType` nor `taskTypes` can never be selected.

4. Regenerate the counterfeiter mock — do NOT hand-edit `/workspace/mocks/config_resolver.go`. The `//counterfeiter:generate -o ../mocks/config_resolver.go --fake-name FakeConfigResolver . ConfigResolver` annotation at `/workspace/pkg/config_resolver.go` line 22 already covers the interface, and `/workspace/pkg/k8s_connector_test.go` line 5 carries the `//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6@v6.12.2 -generate` directive for package `pkg`. Run:
   ```
   make generate
   ```
   (equivalently `go generate -mod=mod ./...`). This must produce `ResolveByTaskType`, `ResolveByTaskTypeReturns`, `ResolveByTaskTypeReturnsOnCall`, `ResolveByTaskTypeCallCount`, `ResolveByTaskTypeCalls`, and `ResolveByTaskTypeArgsForCall` on `mocks.FakeConfigResolver`. Run this BEFORE running `make test`, or the `pkg`, `pkg/handler` and `pkg/spawner` test packages will fail to compile because the fake no longer satisfies the interface. If `make generate` cannot reach the Go module proxy, report it in `## Improvements` and stop — do not hand-write generated code.

5. Add specs to `/workspace/pkg/config_resolver_test.go` (package `pkg_test`, Ginkgo/Gomega). Put them in a new `Describe("ResolveByTaskType", ...)` block inside the existing top-level `Describe("ConfigResolver", ...)` (line 28), reusing the file-level `provider`/`resolver`/`ctx` harness from the `BeforeEach` at lines 35-39. The file already imports `agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"`, `pkg "github.com/bborbe/agent-task-executor/pkg"`, and `"github.com/bborbe/errors"`. Cover all of the following:
   a. Resolves via the singular `Spec.TaskType`, and returns the branch-tagged image. Given a Config `{Assignee: "update-go-agent-personal", Image: "foo/bar", TaskType: "github-update-go"}`, `ResolveByTaskType(ctx, "github-update-go")` must return no error, `config.Assignee == "update-go-agent-personal"`, and `config.Image == "foo/bar:dev"`.
   b. Resolves via a `Spec.TaskTypes` list element. Given `{Assignee: "pr-reviewer-agent-personal", Image: "img", TaskTypes: []string{"pr-review", "oauth-probe"}}`, both `ResolveByTaskType(ctx, "pr-review")` and `ResolveByTaskType(ctx, "oauth-probe")` must succeed and return that Config.
   c. Resolves every member of the effective union when one Config declares both the singular field and the list. Given `{Assignee: "a-personal", Image: "img", TaskType: "github-update-go", TaskTypes: []string{"go-vuln"}}`, both `"github-update-go"` and `"go-vuln"` must resolve to it.
   d. Returns `ErrConfigNotFound` when no Config declares the type. Assert `errors.Is(err, pkg.ErrConfigNotFound)` is `true` and `errors.Is(err, pkg.ErrConfigAmbiguous)` is `false`. Cover both a non-empty store with a non-matching Config and an empty store (`provider.items = []agentv1.Config{}`).
   e. Returns `ErrConfigAmbiguous` when two Configs declare the same type. Given two Configs both with `TaskTypes: []string{"shared-type"}`, assert `errors.Is(err, pkg.ErrConfigAmbiguous)` is `true`, `errors.Is(err, pkg.ErrConfigNotFound)` is `false`, and `err.Error()` contains `shared-type`.
   f. Never matches an empty `taskType`. With a store holding `{Assignee: "a-personal", Image: "img", TaskType: "github-update-go"}`, `ResolveByTaskType(ctx, "")` must return `ErrConfigNotFound` (`errors.Is(err, pkg.ErrConfigNotFound)` is `true`).
   g. Never selects a Config whose effective type set is empty. Given `{Assignee: "service-agent-personal", Image: "img"}` (neither `TaskType` nor `TaskTypes`), `ResolveByTaskType(ctx, "anything")` must return `ErrConfigNotFound`.
   h. Returns a wrapped error that is neither sentinel when `provider.Get` fails — set `provider.err = errors.Errorf(ctx, "storage unavailable")` and assert `errors.Is(err, pkg.ErrConfigNotFound)` is `false` and `errors.Is(err, pkg.ErrConfigAmbiguous)` is `false` (mirrors the existing "returns a wrapped error when provider.Get fails" spec at lines 131-136).
   i. Leave every existing spec in the file unchanged — the resolve-by-assignee path must still behave identically. Do not modify `Resolve` or `convert`.

6. Do NOT add a CHANGELOG entry in this prompt. `CHANGELOG.md` is owned by the sibling prompt that follows (the same split spec 008 used: its first prompt states "Do NOT touch `CHANGELOG.md` … They are handled by the sibling prompt that follows"). No user-visible behavior changes here, so no `## Unreleased` bullet is written yet.

7. Do NOT wire the new lookup into any caller. `pkg/handler/task_event_handler.go`, `pkg/factory/factory.go`, and `main.go` must be byte-identical after this prompt. In particular, do NOT touch `resolveConfig`, `parseAndFilter`, `reconcileTask`, or `reconcileOne` — those changes belong to the sibling prompt.

8. Before finishing, prove the change: re-run every bullet in `<verification>` and confirm each passes, then walk the spec's ACs 2 and 3 against the new specs — the resolved assignee comes from the Config rather than from `previous_assignee`, and a `task_type` matching no Config yields `ErrConfigNotFound`. Name each AC and the spec covering it in `## Improvements` if any AC has no covering spec.
</requirements>

<constraints>
- Repo conventions are frozen: Ginkgo/Gomega v2 specs in external test packages (`package pkg_test`), `github.com/bborbe/errors` wrapping (never `fmt.Errorf`, never `context.Background()` inside `pkg/`), sentinel errors aliased as `stderrors`, counterfeiter mocks (never hand-written mocks).
- Reuse the existing `EffectiveTaskTypes` and `TaskTypeInSet` helpers from `/workspace/pkg/task_type_filter.go`. Do not duplicate the union or membership logic, and do not change those two functions.
- `Resolve` (the assignee path) must keep its exact current semantics: composed `{assignee}-{vaultName}` first, plain-assignee fallback second, `ErrConfigNotFound` when neither matches. Do not refactor it, and do not route `Resolve` through the new lookup.
- No new Config field, environment variable, CLI flag, or CRD field. No change to `/workspace/docs/agent-crd-specification.md` — its `spec.taskTypes` row ("Filtering on this list is enforced by a follow-up spec.") stays as written.
- No new metric and no new log line in this prompt — it is a pure resolver addition with no caller.
- Adding a method to an exported interface means the counterfeiter mock MUST be regenerated (`make generate`) before `make test` can compile. Do not commit a hand-edited `mocks/config_resolver.go`.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `make test` iteratively after each meaningful change (fast feedback loop), then `make precommit` ONCE at the very end. If `make precommit` fails with a `git rev-parse`/ROOTDIR error (this container's `/workspace/.git` is masked), re-run as `make ROOTDIR=/workspace precommit`.

Do NOT run `git`, `docker`, `make build`, `kubectl`, `dark-factory`, or `gh` commands — this container has no such tooling.

- `make generate` — exits 0 and regenerates `mocks/config_resolver.go`.
- `grep -n 'ResolveByTaskType' mocks/config_resolver.go` — returns at least one line (the fake carries the new method).
- `grep -n 'ResolveByTaskType' pkg/config_resolver.go` — returns at least 2 lines (the interface declaration and the implementation).
- `grep -n 'ErrConfigAmbiguous' pkg/config_resolver.go` — returns at least 3 lines (the `var`, and the two `errors.Wrapf`/`errors.Is`-relevant references in the implementation).
- `make test` — exits 0; the new `ResolveByTaskType` specs and all pre-existing `ConfigResolver` specs are green.
- `make precommit` — exits 0.
- `go test -mod=mod -coverprofile=/tmp/cover.out ./pkg/... && go tool cover -func=/tmp/cover.out | grep config_resolver.go` — confirm `ResolveByTaskType` is at ≥80% statement coverage (the provider-error branch and the 0/1/many match arms; the `ctx.Done()` branch mirrors `Resolve`'s existing untested shape).
- `! grep -q 'ResolveByTaskType' pkg/handler/task_event_handler.go` — exits 0 (no caller was wired in this prompt; the handler is untouched).
- `! grep -q '^## Unreleased' CHANGELOG.md` — exits 0 (this prompt adds no changelog entry; the sibling prompt creates it).
</verification>
