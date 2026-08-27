---
status: completed
spec: [003-vault-scoped-config-resolver]
summary: 'Scoped Config CR resolution per executor vault: added required VAULT_NAME startup setting, threaded it through main.go/factory into the resolver, matched Resolve against the composed {assignee}-{vaultName} key with no plain fallback, updated and extended the Ginkgo suite (4 new cases), added a reflect tag guard, and updated CHANGELOG.'
execution_id: agent-task-executor-vaultslot-exec-003-spec-003-vault-scoped-resolver
dark-factory-version: dev
created: "2026-08-26T20:24:00Z"
queued: "2026-08-26T20:33:07Z"
started: "2026-08-26T20:33:42Z"
completed: "2026-08-26T20:42:32Z"
---

<summary>
- The executor declares the Obsidian vault it serves as a new required startup setting — it refuses to run without knowing its vault.
- The vault name travels unchanged from process startup through the wiring layer into the config resolver.
- Config lookup now matches a Config CR whose assignee equals the task's plain assignee plus the executor's vault as a suffix.
- A Config CR whose assignee is just the plain assignee (no vault suffix) never matches; there is no fallback to plain matching.
- When no Config matches, the existing not-found error contract is preserved, so the task handler's unknown-assignee skip path behaves exactly as before.
- The failed-lookup error message includes the composed assignee that was attempted, so operators can see which vault-scoped name missed.
- The public resolver interface and its generated mock are unchanged.
- Tests cover the composed match, no-match, plain-assignee-never-matches, and other-vault isolation cases, plus a guard on the required startup setting.
</summary>

<objective>
Make every executor resolve Config CRs scoped to its own vault: the executor requires `VAULT_NAME` at startup, threads it through the wiring into the resolver, and matches each task's plain assignee against the composed `{assignee}-{vaultName}` key so per-vault `maxConcurrentJobs` slots take effect — while preserving the existing no-match contract (`ErrConfigNotFound`) for unknown assignees. This is prompt 1 of 2 (code + tests); the CRD doc and CHANGELOG land in prompt 2.
</objective>

<context>
Read CLAUDE.md for project conventions.

Read these files fully before changing anything:
- `/workspace/main.go` — the `application` struct (struct-tag config fields, e.g. `SentryDSN`/`Namespace` with `required:"true" arg:"..." env:"..." usage:"..."`) and its `Run` method; currently calls `factory.CreateConfigResolver(eventHandlerConfig, a.Branch)` on the resolver-wiring line.
- `/workspace/pkg/factory/factory.go` — `CreateConfigResolver`, the zero-logic single-return pass-through factory function.
- `/workspace/pkg/config_resolver.go` — `NewConfigResolver`, the `configResolver` struct, `Resolve`, the `ErrConfigNotFound` sentinel and its `errors.Wrapf` wrapping shape.
- `/workspace/pkg/config_resolver_test.go` — the Ginkgo suite (package `pkg_test`) with the `fakeProvider` fixture; every fixture `Assignee` and every `config.Assignee` assertion changes here.
- `/workspace/main_internal_test.go` — the reflect-based struct-tag guard test pattern (`TestApplicationBuildGitVersionFieldExists`).
- `/workspace/pkg/event_handler_config.go` — the `EventHandlerConfig` store type passed to the resolver as its provider.

Verified current signatures (do not paraphrase):
- `func NewConfigResolver(provider k8s.Provider[agentv1.Config], branch base.Branch) ConfigResolver`
- `func CreateConfigResolver(handler pkg.EventHandlerConfig, branch base.Branch) pkg.ConfigResolver`
- `type ConfigResolver interface { Resolve(ctx context.Context, assignee string) (AgentConfiguration, error) }` — unchanged by this prompt
- `var ErrConfigNotFound = stderrors.New("config not found")` — preserved exactly
- `MaxConcurrentJobs int` is a `ConfigSpec` field (the per-vault slot that must flow through `convert`).

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-factory-pattern.md` — factory is zero-logic, `Create*` prefix, single-return pass-through.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `errors.Wrapf`, never `fmt.Errorf`, never bare `return err`.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega conventions.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-kubernetes-crd-controller-guide.md` — keep the linear-scan Config store lookup (no indexing or caching).
</context>

<requirements>
1. In `/workspace/main.go`, add a `VaultName` field to the `application` struct immediately after the `Namespace` field, using the same tagged-struct `required:"true"` mechanism as the other required settings (SENTRY_DSN, LISTEN, BRANCH, NAMESPACE). `service.Main`'s argument parsing treats `required:"true"` as "must be set and non-empty" — the startup abort on unset/empty `VAULT_NAME` happens automatically. Do NOT add any custom validation logic.
   ```go
   VaultName string `required:"true" arg:"vault-name" env:"VAULT_NAME" usage:"Obsidian vault name this executor instance serves; Config CRs are resolved by the composed {assignee}-{vaultName} assignee"`
   ```
   Keep the struct-tag alignment consistent with the surrounding fields (gofmt/golines normalize it).

2. In `/workspace/main.go`, in `(a *application) Run(...)`, change the resolver-wiring call to pass the vault name as a third argument:
   ```go
   resolver := factory.CreateConfigResolver(eventHandlerConfig, a.Branch, a.VaultName)
   ```
   This line must contain both `CreateConfigResolver` and `VaultName` (acceptance grep).

3. In `/workspace/pkg/factory/factory.go`, extend `CreateConfigResolver` with a third parameter that MUST be literally named `vaultName` with type `string` (acceptance grep `vaultName string`), and forward it unchanged to `NewConfigResolver` so the call reads `NewConfigResolver(handler, branch, vaultName)`:
   ```go
   func CreateConfigResolver(
   	handler pkg.EventHandlerConfig,
   	branch base.Branch,
   	vaultName string,
   ) pkg.ConfigResolver {
   	return pkg.NewConfigResolver(handler, branch, vaultName)
   }
   ```
   The factory stays logic-free: single-return pass-through, no conditionals, no validation, no transformation of the vault name.

4. In `/workspace/pkg/config_resolver.go`:
   - Extend `NewConfigResolver` with a `vaultName string` third parameter and store it on the returned resolver:
     ```go
     func NewConfigResolver(
     	provider k8s.Provider[agentv1.Config],
     	branch base.Branch,
     	vaultName string,
     ) ConfigResolver {
     	return &configResolver{provider: provider, branch: branch, vaultName: vaultName}
     }
     ```
   - Add a `vaultName string` field to the `configResolver` struct.
   - In `Resolve`, compute the composed lookup key once before the loop: `composed := assignee + "-" + r.vaultName`, and match items with `it.Spec.Assignee == composed`. A Config CR whose `Spec.Assignee` equals the plain assignee without the suffix must never match — the composed key is ALWAYS used, with NO fallback to plain-assignee matching (spec Non-goal: a fallback would silently reintroduce cross-vault slot sharing).
   - On no match, keep returning `errors.Wrapf(ctx, ErrConfigNotFound, ...)` — same sentinel, same wrapping shape (no `fmt.Errorf`, no bare `return err`). The message MUST include the composed key that was attempted so an operator can see which `{assignee}-{vaultName}` missed (exact wording is yours to choose).
   - Update the `NewConfigResolver` GoDoc comment to mention the vault scoping.
   - Do NOT change the `ConfigResolver` interface (`Resolve(ctx, assignee)` keeps its shape), `convert()`, `appendBranchTag`, `copyEnv`, or the branch-tagging behavior. Do NOT touch `/workspace/mocks/config_resolver.go` — the interface is unchanged, so the generated mock output stays identical (precommit's `generate` target regenerates it idempotently).

5. In `/workspace/pkg/config_resolver_test.go`, update the suite for the new constructor argument and the composed match semantics:
   - In `BeforeEach`, construct the resolver with the vault: `resolver = pkg.NewConfigResolver(provider, "dev", "personal")`. Every fixture in the existing tests then gains the `-personal` suffix so the pre-change matching semantics still hold (the composed key for a resolve of `"claude"` is now `"claude-personal"`).
   - Update every fixture `Assignee` and the corresponding `config.Assignee` assertion:
     - "returns converted AgentConfiguration with image tag appended": fixture `Assignee: "claude"` → `"claude-personal"`; assertion `Expect(config.Assignee).To(Equal("claude-personal"))`.
     - "leaves Resources nil when Spec.Resources is nil": `Assignee: "claude"` → `"claude-personal"`.
     - "returns ErrConfigNotFound when no item matches": `Assignee: "other-agent"` → `"other-agent-personal"` (resolve `"claude"` still misses; the `Expect(errors.Is(err, pkg.ErrConfigNotFound)).To(BeTrue())` assertion is retained verbatim).
     - "defensively copies env map — mutation after Resolve does not affect returned config": `Assignee: "claude"` → `"claude-personal"`.
     - "branch tagging: given branch=dev and Image=foo/bar, result has Image==foo/bar:dev": `Assignee: "claude"` → `"claude-personal"` (Image assertion unchanged).
     - "returns ErrConfigNotFound when store is empty" and "returns a wrapped error when provider.Get fails" need no fixture change.

6. Append these new `It` cases inside the same `Describe("ConfigResolver")` block (after the branch-tagging test). The spec-text strings below are EXACT — the acceptance greps match them verbatim:
   - `It("resolves a Config CR by the composed assignee", ...)`: store one Config with `Assignee: "github-update-go-agent-personal"`, `Image: "foo/bar"`, `Heartbeat: "1m"`, `MaxConcurrentJobs: 1`; call `resolver.Resolve(ctx, "github-update-go-agent")`; assert `err` is nil, `config.Assignee` equals `"github-update-go-agent-personal"` (NOT merely that a match occurred), and `config.MaxConcurrentJobs` equals 1 — proving the per-vault slot flows through.
   - `It("no Config matches the composed assignee", ...)`: empty store; `resolver.Resolve(ctx, "github-update-go-agent")`; assert `errors.Is(err, pkg.ErrConfigNotFound)` is true AND `err.Error()` contains `"github-update-go-agent-personal"` (the composed key that was attempted).
   - `It("a plain assignee without a vault suffix never matches", ...)`: store one Config with `Assignee: "github-update-go-agent"` (plain, no suffix); `resolver.Resolve(ctx, "github-update-go-agent")`; assert `errors.Is(err, pkg.ErrConfigNotFound)` is true.
   - `It("ignores Config CRs belonging to other vaults", ...)`: store one Config with `Assignee: "github-update-go-agent-openclaw"` (another vault's suffix); `resolver.Resolve(ctx, "github-update-go-agent")`; assert `errors.Is(err, pkg.ErrConfigNotFound)` is true — each executor matches only its own `{assignee}-{vaultName}`.

7. In `/workspace/main_internal_test.go`, add a reflect-based tag guard mirroring `TestApplicationBuildGitVersionFieldExists`, asserting the `VaultName` field exists, is a `string`, has `env` tag `VAULT_NAME` and `required` tag `true`:
   ```go
   func TestApplicationVaultNameFieldExists(t *testing.T) {
   	typ := reflect.TypeOf(application{})
   	f, ok := typ.FieldByName("VaultName")
   	if !ok {
   		t.Fatalf("application struct is missing VaultName field")
   	}
   	if f.Type.Kind() != reflect.String {
   		t.Fatalf("VaultName must be string, got %s", f.Type.Kind())
   	}
   	if got, want := f.Tag.Get("env"), "VAULT_NAME"; got != want {
   		t.Errorf("VaultName env tag = %q, want %q", got, want)
   	}
   	if got, want := f.Tag.Get("required"), "true"; got != want {
   		t.Errorf("VaultName required tag = %q, want %q", got, want)
   	}
   }
   ```
   This guards the startup-abort failure mode (`VAULT_NAME` unset/empty → required-field error) against a tag typo that a grep would not catch.

8. Do NOT change: the `ConfigResolver` interface, `/workspace/mocks/config_resolver.go`, `/workspace/pkg/handler/task_event_handler.go` (the `stderrors.Is(err, pkg.ErrConfigNotFound)` consumer and its unknown-assignee skip path), the Config CR fields (`/workspace/k8s/apis/agent.benjamin-borbe.de/v1/types.go`), `convert()`, branch tagging, or the Kafka result path. `VAULT_NAME` is consumed only inside the resolver constructor chain — no other component reads it.
</requirements>

<constraints>
- The `ConfigResolver` interface signature is unchanged — `Resolve(ctx, assignee)` keeps its shape; `mocks/config_resolver.go` is NOT changed (the interface is untouched, so regenerated output is identical).
- The `ErrConfigNotFound` sentinel and its `errors.Wrapf` wrapping shape are preserved exactly — `pkg/handler/task_event_handler.go` (the `stderrors.Is` consumer) is untouched.
- Error construction follows the go-error-wrapping-guide shape: `errors.Wrapf`, no `fmt.Errorf`, no bare `return err`.
- The factory stays logic-free (single-return pass-through), per the go-factory-pattern guide.
- The linear-scan Config store lookup stays — no indexing or caching is added (go-kubernetes-crd-controller-guide).
- NO fallback to plain-assignee matching when the composed key misses — the composed key is always used (spec Non-goal: a fallback would silently reintroduce cross-vault slot sharing).
- Config CR fields, `convert()`, branch tagging, and the Kafka result path are unchanged.
- Existing `pkg/config_resolver_test.go` cases are updated for the new constructor argument, and their fixture Config CRs' `Assignee` values gain the vault suffix matching the test's vault so the pre-change matching semantics still hold; the `errors.Is(err, pkg.ErrConfigNotFound)` assertions are retained verbatim.
- `VAULT_NAME` is consumed only inside the resolver constructor chain — no other component reads it.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run in `/workspace`:
```
make precommit
go test ./pkg -v 2>&1 | grep -c 'resolves a Config CR by the composed assignee'
go test ./pkg -v 2>&1 | grep -c 'no Config matches the composed assignee'
go test ./pkg -v 2>&1 | grep -c 'a plain assignee without a vault suffix never matches'
grep -n 'env:"VAULT_NAME"' main.go | grep -c 'required:"true"'
grep -n 'CreateConfigResolver' main.go | grep -c 'VaultName'
grep -n 'vaultName string' pkg/factory/factory.go
grep -c 'NewConfigResolver(handler, branch,' pkg/factory/factory.go
grep -n 'pkg.ErrConfigNotFound' pkg/config_resolver_test.go
```
Expected:
- `make precommit` exits 0 — all Ginkgo specs green (including the updated fixtures and the four new cases), lint/vet/errcheck pass, `generate` regenerates the mock unchanged.
- Each of the three `go test ./pkg -v` description greps returns ≥ 1 (the new `It` descriptions appear in `-v` output).
- `env:"VAULT_NAME"` + `required:"true"` grep returns ≥ 1.
- `CreateConfigResolver` + `VaultName` grep returns ≥ 1 (the wiring line passes the vault).
- `vaultName string` in `pkg/factory/factory.go` returns ≥ 1 line.
- `NewConfigResolver(handler, branch,` in `pkg/factory/factory.go` returns ≥ 1 (the three-arg call).
- `pkg.ErrConfigNotFound` is present in `pkg/config_resolver_test.go` (presence check only — the existing assertions are retained verbatim).

If `make precommit` exits non-zero, the prompt is NOT done — fix and re-run the failing target until it passes, then run `make precommit` once more.

Before you finish: re-run the verification block and confirm every expected line holds; walk each acceptance criterion against the change.
</verification>

## Definition of Done

- [ ] `application` struct declares `VaultName` with `env:"VAULT_NAME"` and `required:"true"` (startup aborts on unset/empty)
- [ ] `main.go` wires `factory.CreateConfigResolver(eventHandlerConfig, a.Branch, a.VaultName)`
- [ ] `factory.CreateConfigResolver` accepts `vaultName string` and forwards it unchanged (zero-logic)
- [ ] `configResolver.Resolve` matches `it.Spec.Assignee == assignee + "-" + vaultName` with no plain fallback; no-match returns `errors.Wrapf(ctx, ErrConfigNotFound, ...)` including the composed key
- [ ] Existing `pkg/config_resolver_test.go` fixtures gain the `-personal` suffix; assertions updated; `errors.Is(err, pkg.ErrConfigNotFound)` assertions retained
- [ ] Four new `It` cases present with the exact spec texts (composed match, no match, plain never matches, other vault ignored); composed-match asserts `Assignee` equals the suffixed fixture and `MaxConcurrentJobs` flows through
- [ ] Reflect guard `TestApplicationVaultNameFieldExists` verifies the `VAULT_NAME`/`required:"true"` tags
- [ ] `ConfigResolver` interface, mock, and `task_event_handler.go` untouched
- [ ] `make precommit` exits 0; all verification greps match
