---
status: prompted
tags:
    - dark-factory
    - spec
approved: "2026-08-26T20:17:05Z"
generating: "2026-08-26T20:33:07Z"
prompted: "2026-08-26T20:33:07Z"
branch: dark-factory/vault-scoped-config-resolver
---

## Summary

- The executor will run one instance per Obsidian vault; each instance must resolve agent Config CRs scoped to its own vault.
- Config CRs are named `{assignee}-{vault}` (e.g. `github-update-go-agent-openclaw`, `github-update-go-agent-personal`), each with its own `maxConcurrentJobs` slot.
- Tasks keep their plain assignee (e.g. `github-update-go-agent`); the executor composes the lookup key as `{assignee}-{vaultName}`.
- The executor learns its own vault from a required `VAULT_NAME` environment variable; startup fails if it is unset.
- This is the executor-side code slice of the "Isolate per-vault Kafka topic-executor for agent tasks" design (vault note, 2026-08-26). Chart work (per-vault Deployments, per-vault Config CRs, env injection) is a separate repo.

## Problem

One executor currently serves all vaults from a shared topic and shared Config CRs, so a single `github-update-go-agent` slot (`maxConcurrentJobs:1`) is monopolized by OpenClaw's 232-task backlog while Personal's 12 tasks wait behind it. The design splits the executor per vault and gives each vault its own Config CR with an independent slot. But a task carries only its plain assignee, while the per-vault Config CR is named `{assignee}-{vault}`. Without a vault-scoped lookup, a per-vault executor cannot find its Config, every task resolves to "config not found", and the whole split ships dead on arrival.

## Goal

After this work, a per-vault executor, given a task with plain assignee `X` and its own vault name `V`, resolves the Config CR named `X-V` and applies that CR's independent concurrency slot. The executor refuses to run without knowing its vault, and the no-match contract for unknown assignees behaves exactly as it does today.

## Non-goals

- Creating the per-vault Config CRs (`{assignee}-{vault}`), their PriorityClass/ResourceQuota, or the chart `executors:` list — that is `nuke/agent` chart work in a separate repo
- Injecting `VAULT_NAME` into the executor Deployment — chart work
- Deriving the vault from the Kafka topic prefix instead of `VAULT_NAME` — `VAULT_NAME` is the source of truth
- Changing the `ConfigResolver` interface, its mock, or the task-event-handler unknown-assignee skip path
- Changing topic prefixes, publishers, or the controller — topic isolation is chart-side
- Migrating or renaming existing plain-named Config CRs
- Do NOT add an opt-out flag that falls back to plain-assignee matching when the composed key misses — a fallback would silently reintroduce cross-vault slot sharing and defeat the goal. Invariant: the composed key is always used. If a future consumer demands plain matching, that is a separate spec.

## Acceptance Criteria

- [ ] `make precommit` exits 0 — evidence: exit code
- [ ] The application declares its vault identity via `VAULT_NAME` and requires it — `grep -n 'env:"VAULT_NAME"' main.go | grep -c 'required:"true"'` returns 1 — evidence: file content
- [ ] The application wiring passes the vault into the resolver — `grep -n 'CreateConfigResolver' main.go | grep -c 'VaultName'` returns ≥1 — evidence: file content
- [ ] The factory forwards the vault name unmodified — `grep -n 'vaultName string' pkg/factory/factory.go` returns ≥1 line (param present) **and** `grep -c 'NewConfigResolver(handler, branch,' pkg/factory/factory.go` returns ≥1 (three-arg call). The local variable name is NOT frozen — any name carrying the vault through is fine — evidence: file content
- [ ] Vault-scoped match is covered by a unit test — `go test ./pkg -v` exits 0 and stdout contains the spec description `resolves a Config CR by the composed assignee`; that `It` must assert the resolved `AgentConfiguration.Assignee` equals the suffixed fixture (e.g. `github-update-go-agent-personal` for `VAULT_NAME=personal`), not merely that a match occurred — evidence: exit code + stdout match + the assertion requirement above
- [ ] The no-match sentinel contract is preserved and covered — `go test ./pkg -v` stdout contains the spec descriptions `no Config matches the composed assignee` and `a plain assignee without a vault suffix never matches`; the former must assert the returned error satisfies `errors.Is(err, pkg.ErrConfigNotFound)`. (The pre-existing `pkg.ErrConfigNotFound` grep in the test file is a presence check only, not new evidence.) — evidence: stdout match
- [ ] The CRD doc documents the composed match rule, not just the field name — `grep -n 'vaultName' docs/agent-crd-specification.md | grep -c 'assignee'` returns ≥1 — evidence: file content
- [ ] CHANGELOG records the change under a fresh `## Unreleased` section — `grep -n '^## Unreleased' CHANGELOG.md` returns ≥1 line **and** `sed -n '/^## Unreleased/,/^## /p' CHANGELOG.md | grep -c 'vault'` returns ≥1 — evidence: file content

No new scenario. This is a single-function behavior fully reachable by unit tests in the implementation prompt; the real cluster proof (per-vault slot independence) belongs to the chart work that ships the per-vault CRs and env injection.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

```bash
make precommit
go test ./pkg -v
grep -n 'env:"VAULT_NAME"' main.go | grep -c 'required:"true"'
grep -n 'CreateConfigResolver' main.go | grep -c 'VaultName'
grep -n 'vaultName string' pkg/factory/factory.go
grep -c 'NewConfigResolver(handler, branch,' pkg/factory/factory.go
grep -n 'pkg.ErrConfigNotFound' pkg/config_resolver_test.go   # presence check only
grep -n 'vaultName' docs/agent-crd-specification.md | grep -c 'assignee'
grep -n '^## Unreleased' CHANGELOG.md
sed -n '/^## Unreleased/,/^## /p' CHANGELOG.md | grep -c 'vault'
```

All ACs are container-executable. No deployed system is observed by this spec — the runtime proof of per-vault slot independence lives in the `nuke/agent` chart work that ships the `executors:` list, the per-vault Config CRs, and the `VAULT_NAME` env injection.

## Desired Behavior

1. The executor binds its vault identity from the `VAULT_NAME` environment variable using the same tagged-struct `required:"true"` mechanism as every other required setting (SENTRY_DSN, LISTEN, BRANCH, NAMESPACE). Startup aborts with a required-field error when `VAULT_NAME` is unset or empty, so an instance never runs without knowing which vault it serves.
2. The vault name flows from the application wiring into the resolver constructor through the factory as a pure pass-through — the factory adds no validation, transformation, or composition. The resolver stores the vault name and uses it at lookup time.
3. Config lookup composes the match key as `{assignee}-{vaultName}`: a task with plain assignee `github-update-go-agent` on an executor with `VAULT_NAME=personal` matches the Config CR whose `spec.assignee` is `github-update-go-agent-personal`. A Config CR whose `spec.assignee` equals the plain assignee without a vault suffix never matches.
4. When no Config CR matches the composed key, the resolver returns the existing `ErrConfigNotFound` sentinel wrapped in the same `errors.Wrapf` shape, so the task handler's unknown-assignee skip path (`stderrors.Is(err, pkg.ErrConfigNotFound)`) behaves exactly as before. The no-match error message includes the composed key that was attempted (exact message wording — agent decides at impl time), so an operator can see which `{assignee}-{vaultName}` missed.
5. The CRD specification document states that with a per-vault executor, `spec.assignee` matches the composed `{assignee}-{vaultName}` value, and the CHANGELOG records the change under a fresh `## Unreleased` bullet. Release stays manual (`autoRelease:false` in `.dark-factory.yaml`).

## Constraints

- The `ConfigResolver` interface signature is unchanged — `Resolve(ctx, assignee)` keeps its shape; `mocks/config_resolver.go` is NOT regenerated
- The `ErrConfigNotFound` sentinel and its `errors.Wrapf` wrapping shape are preserved exactly — `pkg/handler/task_event_handler.go` (the `stderrors.Is` consumer) is untouched by this spec
- Error construction follows the go-error-wrapping-guide shape: `errors.Wrapf`, no `fmt.Errorf`, no bare `return err`
- The factory stays logic-free (single-return pass-through), per the go-factory-pattern guide
- The linear-scan Config store lookup stays — no indexing or caching is added (go-kubernetes-crd-controller-guide)
- Config CR fields, `convert()`, branch tagging, and the Kafka result path are unchanged
- Existing `pkg/config_resolver_test.go` cases are updated for the new constructor argument, and their fixture Config CRs' `Assignee` values gain the vault suffix matching the test's vault so the pre-change matching semantics still hold; the `errors.Is(err, pkg.ErrConfigNotFound)` assertions are retained verbatim
- `VAULT_NAME` is consumed only inside the resolver constructor chain — no other component reads it
- CHANGELOG edits touch only the new `## Unreleased` section; no released section is rewritten

## Failure Modes

| Trigger | Expected behavior | Detection | Reversibility | Recovery |
|---|---|---|---|---|
| `VAULT_NAME` unset or empty at startup | Startup aborts with a required-field error, non-zero exit | Process never becomes Ready | Reversible (no state written) | Set `VAULT_NAME` in the Deployment env (chart work) |
| `VAULT_NAME` value matches no Config CR suffix (typo, e.g. `personall`) | Every task for that vault resolves to `ErrConfigNotFound`; handler skips as unknown assignee | WARNING log `skip task ...: unknown assignee`; metric `TaskEventsTotal{label="skipped_unknown_assignee"}` increments | Reversible — skip is log/metric-only; tasks stay in the vault and re-trigger once fixed | Correct `VAULT_NAME` to match the CR suffix |
| Plain-named Config CRs still deployed (not yet renamed to `{assignee}-{vault}`) | Same silent skip — no CR matches the composed key; executor appears healthy but spawns nothing | WARNING log + `skipped_unknown_assignee` metric | Reversible (no Jobs spawned, nothing mutated) | Rename/apply the per-vault CRs before rolling out the vault-scoped executor; the executor is inert without them |
| Two executors (openclaw + personal) share one namespace and Config store | Each matches only its own `{assignee}-{vault}` suffix; slots stay independent | Independent `skipped_unknown_assignee` and slot counters per vault | Reversible — resolution is read-only | None needed; verify per-vault `maxConcurrentJobs` via the per-vault PriorityClass/ResourceQuota |
| A Config CR's `Assignee` coincidentally equals another vault's composed name | That vault's executor resolves it (cross-vault slot claim) | Operator review of CR names at apply time | Reversible (edit CR name) | Configs are operator-authored and version-controlled in the chart; fix the CR name |
| Executor restarts mid-rollout with some CRs renamed | Stateless per-resolve lookup — each call re-reads the store; no partial-progress state | None (no persisted state) | Reversible | None needed |

External unavailability, rate limiting, and clock skew do not apply: the resolver performs an in-memory read of the Config store with no new I/O, retries, or time dependence.

## Security / Abuse Cases

- **What can an attacker control?** The task assignee (task-frontmatter input, already untrusted) and `VAULT_NAME` (operator-set env). Both feed a plain string concatenation used only in equality comparisons against Config CR names — no shell, no regex, no paths, no injection surface.
- **Cross-vault interference:** a Config CR author could name a CR to collide with another vault's composed assignee and claim that vault's tasks/slots. This is operator-authored, version-controlled chart content in the same trust domain as today; accepted residual risk. A per-vault CR name convention (`{assignee}-{vault}`) and review at apply time mitigate it.
- **Wrong `VAULT_NAME` cannot redirect tasks to another vault's CR:** the composed key always includes the executor's own vault, so a mismatch produces no match (skip), never a foreign match — except when the wrong value coincidentally equals the other vault's suffix, an operator error caught by CR-name review.
- **Nothing hangs or retries forever:** unmatched tasks take the existing deferred-respawn path unchanged; no new retry loop is introduced.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Vault-scoped resolver code + tests — `VAULT_NAME` field on the application, factory pass-through, `NewConfigResolver` param, composed match key, test updates | 1, 2, 3, 4 | 1, 2, 3, 4, 5, 6 | — |
| 2 | Docs + CHANGELOG — CRD doc match rule + `## Unreleased` bullet | 5 | 7, 8 | 1 (the doc describes the settled match semantics) |

Rationale: the code change is one connected chain (application wiring → factory → resolver → tests) with a single seam; the doc row describes the settled semantics and lands last. The two prompts could be merged into one without loss.

## Do-Nothing Option

A per-vault executor cannot resolve its per-vault Config CRs — every task would be skipped as unknown assignee, and the per-vault slot isolation design (independent `maxConcurrentJobs` per vault, ending queue starvation) cannot ship. The status quo remains: OpenClaw's 232-task backlog keeps starving Personal's 12 behind a single shared slot (measured 2026-08-26). Doing nothing is acceptable only if slot isolation is abandoned.
