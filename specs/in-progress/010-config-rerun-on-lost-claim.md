---
status: verifying
approved: "2026-10-21T18:47:55Z"
generating: "2026-10-21T19:08:12Z"
prompted: "2026-10-21T20:00:54Z"
verifying: "2026-10-21T20:16:51Z"
branch: dark-factory/config-rerun-on-lost-claim
---

## Summary

- Each agent gets a new yes/no setting that decides whether a task runs again when the cluster running it drops out and another cluster takes the task over.
- Yes is the default, so every existing agent keeps that behaviour without any change.
- No means the task stops and waits for a person to look at it, for agents where running twice could do harm or cost a lot.
- This work adds the setting and the rule that turns it into "run again" or "wait for a person". Nothing uses the rule yet; a later change connects it to how tasks are started.
- The setting is added everywhere it has to be for Kubernetes to keep it, and it is documented.

## Problem

Under pull-based claims (claim library spec `specs/claim-lease-client-library.md`), a cluster that stops renewing fences its own Job, and after the lease plus grace another cluster takes the claim over. The taker then has to decide whether to run the task again. For most agents a re-run is the right answer, but for some (a Job with external side effects, a long expensive analysis) a silent second run is worse than asking a person. Today there is no place to declare that per agent. The existing attempt budgets cannot express it: `retries: 0` and `max_triggers` limit how many times a task may be started, but they cannot tell a lost claim apart from a failed run, so they cannot say "do not re-run after a lost claim" without also changing what happens after a failure. The repo has also already shipped a Config field once that the API server silently pruned because it was missing from the installed CRD schema (`maxConcurrentJobs`, v0.5.0), so a new field is only real once it is in the schema too.

## Goal

A Config can declare `spec.rerunOnLostClaim: false`, the value survives the executor's own CRD install and a Config round-trip, and a pure decision function returns "park at `human_review`" for it and for an unset field, and "re-dispatch" for an explicit `true`. Existing Configs, which do not set the field, therefore park: unset is the safe default. The executor's runtime behaviour is unchanged by *this* spec; the wiring change that consumes the decision is what acts on it.

## Non-goals

- `CLAIM_MODE` and dispatch wiring (calling the decision function after a takeover, writing the phase) → later wiring spec.
- No carrying of the field through the agent-configuration resolver — the wiring spec does that.
- Not `max_triggers`, not `spec.retries`; the field is independent of both and changes neither.
- Helm chart CRD in `bborbe/agent` is another repo → follow-up named "sync rerunOnLostClaim into bborbe/agent helm/crds/config-crd.yaml".
- No admission validation beyond the schema type (`boolean`).
- Do NOT add a third value or a per-task override — invariant two-way choice; if a future consumer demands more, that is a separate spec.

## Assumptions

- The executor writes the Config CRD schema itself at startup; a field missing from that schema is pruned from every Config by the API server (the `configSpecSchema` comment and the existing schema guard tests in the repo).
- The field must distinguish "unset" from `true`, so it is an optional pointer-typed boolean whose unset value reads as `false`.
- `human_review` is an existing task phase value (`docs/agent-crd-specification.md`, `spec.trigger.phases`).

## Acceptance Criteria

All container-executable. Each AC names how a broken implementation is caught ("Discriminates").

- [ ] AC1 — **decision, both sides (SC8 of the vault task Executors Claim Agent Tasks With Leased Git Refs).** Table test of the decision function in the Config type's package. Evidence: field unset → `ParkAfterLostClaim`; `false` → `ParkAfterLostClaim`; `true` → `RedispatchAfterLostClaim`. Discriminates: a function ignoring the field returns the same outcome for all three rows and fails the `true` row (or both `false`/unset rows).
- [ ] AC2 — **JSON round-trip and accessor.** Evidence: decoding Config spec JSON `{}` → the rerun accessor returns `true` and the field is nil; `{"rerunOnLostClaim":false}` → `false`; `{"rerunOnLostClaim":true}` → `true`; encoding a spec with the field nil produces JSON with no `rerunOnLostClaim` key (`omitempty`). Discriminates: a plain non-pointer boolean decodes `{}` as `false` and fails the first row.
- [ ] AC3 — **schema-pruning guard.** A test over the CRD the executor installs, in the style of the existing `maxConcurrentJobs` guard, finds property `rerunOnLostClaim` of type `boolean` in the Config spec properties. Evidence: the test passes under `make test`; `grep -n 'rerunOnLostClaim' pkg/k8s_connector.go` returns ≥1 line. Discriminates: a field added to the type but not to the schema fails this test — the v0.5.0 failure.
- [ ] AC4 — **equality.** Evidence: `Equal` returns false for two specs that differ only in `rerunOnLostClaim` (rows: nil vs `false`, `true` vs `false`) and true for nil vs nil and `false` vs `false`. Discriminates: an `Equal` that skips the field returns true for nil vs `false`, so a Config edit would not be noticed.
- [ ] AC5 — **deep-copy.** Evidence: `grep -n 'RerunOnLostClaim' k8s/apis/agent.benjamin-borbe.de/v1/zz_generated.deepcopy.go` returns ≥1 line; a test deep-copies a spec with the field set to `false`, sets the copy's value to `true`, and the original still reads `false`. Discriminates: a shallow pointer copy shares the value and fails.
- [ ] AC6 — **docs.** Evidence: `grep -n 'spec.rerunOnLostClaim' docs/agent-crd-specification.md` returns a Fields-table row containing `true` and `human_review`.
- [ ] AC7 — **changelog.** Evidence: `grep -c '^## Unreleased' CHANGELOG.md` returns 1, and `awk '/^## /{sec=$0} /^- feat:/ && /rerunOnLostClaim/{print sec; exit}' CHANGELOG.md` prints `## Unreleased`.
- [ ] AC8 — **no live wiring.** Evidence: `grep -rnE 'RerunOnLostClaim|RedispatchAfterLostClaim|ParkAfterLostClaim' --include='*.go' pkg | grep -v '_test.go'` returns 0 lines — no executor code under `pkg/` reads the field or calls the decision (the decision function's own file lives with the Config type, outside `pkg/`; the schema property in the CRD install code is the lower-case JSON name and does not match).
- [ ] AC9 — `make precommit` exits 0. Evidence: exit code.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

```
make precommit
go test -count=1 ./k8s/... ./pkg/...
grep -n 'rerunOnLostClaim' pkg/k8s_connector.go                                           # >= 1
grep -rnE 'RerunOnLostClaim|RedispatchAfterLostClaim|ParkAfterLostClaim' --include='*.go' pkg | grep -v '_test.go'   # 0 lines
grep -n 'RerunOnLostClaim' k8s/apis/agent.benjamin-borbe.de/v1/zz_generated.deepcopy.go  # >= 1
grep -n 'spec.rerunOnLostClaim' docs/agent-crd-specification.md                           # row with true + human_review
grep -c '^## Unreleased' CHANGELOG.md                                                     # 1
awk '/^## /{sec=$0} /^- feat:/ && /rerunOnLostClaim/{print sec; exit}' CHANGELOG.md       # ## Unreleased
```

The operator-executable rung is the wiring change that consumes the decision (`pkg/handler/claim_wiring.go` § `acquireSpawnClaim`): it parks or re-dispatches after a takeover. That rung is out of scope for this spec and is proven by the vault task "Park a Task at human_review When Its Claim Is Lost".

## Desired Behavior

1. The Config spec type gains optional `rerunOnLostClaim` (pointer-typed boolean, JSON `omitempty`) with a doc comment stating: after another executor takes over this agent's expired claim, `false`/unset parks the task at phase `human_review` and `true` re-dispatches it. An accessor returns `false` when the field is unset.
2. The CRD schema the executor installs declares `rerunOnLostClaim` as `boolean` in the Config spec properties, so the API server keeps the value.
3. `Equal` compares the field by value (nil and `false` differ), and the generated deep-copy copies the pointed-to value.
4. A pure decision function, in the same package as the Config type, maps the field to a named two-value type `LostClaimAction` with values `RedispatchAfterLostClaim` and `ParkAfterLostClaim` (park at phase `human_review`), with unset treated as `false`.
5. `docs/agent-crd-specification.md` lists `spec.rerunOnLostClaim` in the Fields table (default `true`, `false` parks at `human_review`, independent of `max_triggers` and `spec.retries`); `CHANGELOG.md` gains a `## Unreleased` `feat:` bullet naming `rerunOnLostClaim`.

## Constraints

- Existing Configs that do not set the field decode, compare and render exactly as before; all existing tests pass.
- Generated k8s code is regenerated with the repo's codegen target rather than hand-edited (`make generatek8s`; if the container cannot run it, the deep-copy addition follows the generator's exact output shape — agent decides at impl time).
- Existing schema properties and validation rules are unchanged; the new property is additive.
- `## Unreleased` in `CHANGELOG.md` appears exactly once, shared with any other pending spec's bullets.

## Failure Modes

| Trigger | Expected behavior | Recovery (observable) |
|---|---|---|
| Field added to the type but not the installed schema | API server prunes it; every Config reads as unset (`false`) | Prevented by AC3; would show as `kubectl get config <name> -o jsonpath='{.spec.rerunOnLostClaim}'` empty after an executor restart |
| Operator sets a non-boolean value | Rejected by the API server's schema validation at apply time | `kubectl apply` error naming the field |
| Helm chart CRD in `bborbe/agent` lacks the field | The executor's own install overwrites the stored schema on start, so the field survives; a chart-only install would prune it | Follow-up "sync rerunOnLostClaim into bborbe/agent helm/crds/config-crd.yaml"; done when `grep -n rerunOnLostClaim helm/crds/config-crd.yaml` in `bborbe/agent` returns ≥ 1 line |
| Decision called with a nil field | Treated as `false` → park | AC1 unset row |

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Type field + accessor, generated deep-copy, `Equal`, installed schema property, decision function, CRD doc row, CHANGELOG bullet, `make precommit` | 1, 2, 3, 4, 5 | 1–9 | — |

Rationale: the changes are small and must land together, or the field is pruned or compared wrongly; one prompt keeps the type and schema in step.

## Do-Nothing Option

Every taken-over task would be re-dispatched unconditionally once claims are wired. That is acceptable for most agents, but an agent whose second run has side effects would repeat them silently with no per-agent way to ask for a human decision instead — and adding the field later means touching the wiring code a second time.
