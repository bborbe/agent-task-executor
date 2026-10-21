---
status: approved
spec: [010-config-rerun-on-lost-claim]
created: "2026-10-21T19:20:00Z"
queued: "2026-10-21T19:14:22Z"
---

<summary>
- Every agent Config can now declare a yes/no setting that decides what happens to its task when another cluster takes over the agent's expired claim.
- The default is "run it again", so every Config that does not set the setting behaves exactly as before.
- Setting it to "no" makes the task stop and wait at the `human_review` phase for a person instead of running a second time.
- The setting is stored as an optional value so Kubernetes can tell "not set" (means yes) apart from "explicitly no".
- The executor's own Kubernetes schema declares the field, so the API server no longer silently removes it — the v0.5.0 `maxConcurrentJobs` failure cannot repeat.
- A small pure rule turns the setting into one of two named outcomes: "re-dispatch" or "park at human_review".
- Two Configs that differ only in this setting are now correctly detected as different, and a deep copy no longer shares the underlying value.
- The operator-facing CRD documentation gets a row explaining the default and the `human_review` behaviour.
- The changelog gets one feature bullet.
- Nothing reads the setting at runtime yet: no executor code under `pkg/` changes, so existing behaviour is untouched.
</summary>

<objective>
Add the per-agent `spec.rerunOnLostClaim` setting to the Config CRD type, make it survive the executor-installed CRD schema and a JSON round-trip, and add a pure decision function in the Config type's package that maps the setting to `RedispatchAfterLostClaim` (true or unset) or `ParkAfterLostClaim` (false, park at phase `human_review`). This prompt ships the field, the schema property, equality, deep-copy, the decision rule, the documentation row and the changelog entry together, because a field missing from the installed schema is silently pruned by the API server. No runtime wiring is part of this prompt.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for the container agent's execution rules (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, external test packages, coverage ≥80% for new code, license headers on every Go file). This repo has no project-level `CLAUDE.md`.

Read these files fully before changing anything:
- `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/types.go` — `ConfigSpec` (the struct whose fields all carry a GoDoc comment and a JSON tag), `ConfigSpec.Equal` (the chained `==` / `reflect.DeepEqual` expression), and the existing pointer-typed fields `ZombieSweeperIntervalSeconds *int32` / `ZombieJobTimeoutSeconds *int32` whose comment explicitly explains the "pointer-typed so unset is distinguishable" rationale. Mirror that shape.
- `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/types_test.go` — the existing `Describe("Equal - maxConcurrentJobs", ...)` block and the `Describe("JSON round-trip for priorityClassName", ...)` block; the new tests mirror their Ginkgo style. The file is `package v1_test`.
- `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/v1_suite_test.go` — the package's Ginkgo entry point (`RunSpecs(t, "V1 Suite")`). Do not add a second suite.
- `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/zz_generated.deepcopy.go` — `ConfigSpec.DeepCopyInto`. Read the two existing pointer blocks (`ZombieSweeperIntervalSeconds`, `ZombieJobTimeoutSeconds`) and the `// NOTE: hand-added 2026-09-07 ... make generatek8s is broken on master` comment.
- `/workspace/pkg/k8s_connector.go` — `configSpecSchema` and its long doc comment about the v0.5.0 `maxConcurrentJobs` pruning defect, and `configSpecProperties()` (the map that is the authoritative Config.spec schema written into the cluster on every executor start).
- `/workspace/pkg/k8s_connector_test.go` — the `Describe("desiredCRDSpec (via SetupCustomResourceDefinition)", ...)` block, especially the `It("declares every field AgentConfiguration reads from spec", ...)` guard at lines ~210-221 (the `maxConcurrentJobs` guard this prompt's guard mirrors) and the `getCRDFromCreateAction` helper.
- `/workspace/docs/agent-crd-specification.md` — the `## Fields` markdown table (rows `| Field | Required | Description |`).
- `/workspace/CHANGELOG.md` — the top of the file: the preamble, then `## v0.23.4`. A `## Unreleased` section may already exist (spec 009 adds one).

Library source — read only to confirm the phase name and the `omitempty` behaviour, do NOT modify:
- `domain.TaskPhaseHumanReview` is the existing constant for `human_review`; it is already used in this repo (`grep -rn 'domain.TaskPhaseHumanReview' /workspace --include='*.go'`). This prompt does not need to import `domain` in production code — the phase name appears only in doc comments and documentation.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-doc-best-practices.md` — GoDoc comments start with the name and describe behaviour, not implementation.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2, external `_test` package, coverage ≥80% for new code.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` placement, `feat:` prefix, one bullet per logical change.
- `/home/node/.claude/plugins/marketplaces/coding/docs/definition-of-done.md` — coverage rules for new code.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-precommit.md` — golines 100-char limit, funlen, license headers.
</context>

<requirements>
1. Add the field to `ConfigSpec` in `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/types.go`. Place it after `MaxConcurrentJobs` and before `Trigger`, with exactly this doc comment and tag (the pointer type and `omitempty` are load-bearing: a plain `bool` decodes `{}` as `false` and would break the default-true contract):
   ```go
   // RerunOnLostClaim decides what happens to this agent's task after another
   // executor takes over the claim this cluster stopped renewing. True or unset
   // re-dispatches the task; false parks it at phase human_review for a person
   // to decide. Pointer-typed so "unset" is distinguishable from false, and
   // unset reads as true so every existing Config keeps re-dispatching.
   // Independent of max_triggers and spec.retries.
   RerunOnLostClaim *bool `json:"rerunOnLostClaim,omitempty"`
   ```

2. Extend `ConfigSpec.Equal` in the same file so the field is compared by value. Add this term to the existing chained expression, immediately after the `s.MaxConcurrentJobs == o.MaxConcurrentJobs &&` line:
   ```go
   reflect.DeepEqual(s.RerunOnLostClaim, o.RerunOnLostClaim) &&
   ```
   `reflect` is already imported. `reflect.DeepEqual` on two `*bool` distinguishes nil from a pointer to false, and compares pointed-to values otherwise — exactly the four rows Acceptance Criterion 4 requires. Do NOT compare `s.RerunOnLostClaimEnabled()` instead: that would treat nil and `true` as equal and hide a real Config edit.

3. Create `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/lost_claim.go` (new file, `package v1`) with the BSD license header copied from the other files in the package (`// Copyright (c) 2026 Benjamin Borbe All rights reserved.` / `// Use of this source code is governed by a BSD-style` / `// license that can be found in the LICENSE file.`), then exactly this content:
   ```go
   // LostClaimAction is what the executor should do with a task after another
   // cluster has taken over this agent's expired claim.
   type LostClaimAction string

   const (
       // RedispatchAfterLostClaim runs the task again under the new owner. It is
       // the outcome for true and for an unset rerunOnLostClaim.
       RedispatchAfterLostClaim LostClaimAction = "redispatch"
       // ParkAfterLostClaim stops the task and parks it at phase human_review
       // for a person to decide, instead of running it a second time.
       ParkAfterLostClaim LostClaimAction = "park"
   )

   // RerunOnLostClaimEnabled reports whether a task should run again after
   // another executor takes over this agent's expired claim. An unset field
   // means true, so every Config that predates the field keeps re-dispatching.
   func (s ConfigSpec) RerunOnLostClaimEnabled() bool {
       return s.RerunOnLostClaim == nil || *s.RerunOnLostClaim
   }

   // LostClaimAction maps rerunOnLostClaim to the action to take after a lost
   // claim: RedispatchAfterLostClaim for true or unset, ParkAfterLostClaim for
   // false. It is pure and is not called by any executor code yet.
   func (s ConfigSpec) LostClaimAction() LostClaimAction {
       if s.RerunOnLostClaimEnabled() {
           return RedispatchAfterLostClaim
       }
       return ParkAfterLostClaim
   }
   ```
      Do NOT add a `Validate` method for the type, a third value, or a per-task override — the spec's Non-goals forbid all three.

4. Add the schema property in `/workspace/pkg/k8s_connector.go`. In `configSpecProperties()`, insert directly after the `"maxConcurrentJobs"` entry:
   ```go
   // Optional; unset means true. false parks a taken-over task at human_review.
   "rerunOnLostClaim": {Type: "boolean"},
   ```
   This is the authoritative schema `SetupCustomResourceDefinition` writes on every executor start; omitting it is the v0.5.0 failure the surrounding comment describes. Do not add a default, an enum, or a CEL rule — the spec's Non-goals limit admission validation to the `boolean` type.

5. Add the deep-copy block in `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/zz_generated.deepcopy.go`. In `func (in *ConfigSpec) DeepCopyInto`, after the `ZombieJobTimeoutSeconds` block and before the final `return`, add exactly the generator's shape for a pointer-to-bool:
   ```go
   if in.RerunOnLostClaim != nil {
       in, out := &in.RerunOnLostClaim, &out.RerunOnLostClaim
       *out = new(bool)
       **out = **in
   }
   ```
   Do NOT run `make generatek8s`: `hack/update-codegen.sh` pins the wrong module path (`github.com/bborbe/agent/task/executor`) and regenerating rewrites the whole `k8s/client` tree and breaks the build — the file itself carries a hand-added-block NOTE for this reason. Hand-add the block in the generator's exact output shape.

6. Create `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/lost_claim_test.go` (`package v1_test`, same license header) covering Acceptance Criteria 1, 2, 4 and 5. Imports: `encoding/json`, Ginkgo/Gomega v2 dot-imports, and `agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"`. Include a file-local helper `func boolPtr(b bool) *bool { return &b }`. Add exactly these blocks:

   a. AC1 — the decision, all three rows (Ginkgo `DescribeTable` is the repo's table-test idiom):
   ```go
   var _ = Describe("LostClaimAction", func() {
       DescribeTable("maps rerunOnLostClaim to the takeover action",
           func(field *bool, expected agentv1.LostClaimAction) {
               spec := agentv1.ConfigSpec{RerunOnLostClaim: field}
               Expect(spec.LostClaimAction()).To(Equal(expected))
           },
           Entry("unset re-dispatches", nil, agentv1.RedispatchAfterLostClaim),
           Entry("true re-dispatches", boolPtr(true), agentv1.RedispatchAfterLostClaim),
           Entry("false parks at human_review", boolPtr(false), agentv1.ParkAfterLostClaim),
       )
   })
   ```

   b. AC2 — JSON round-trip and accessor, four cases:
   ```go
   var _ = Describe("rerunOnLostClaim JSON", func() {
       It("treats an absent field as nil and true", func() {
           var spec agentv1.ConfigSpec
           Expect(json.Unmarshal([]byte(`{}`), &spec)).To(Succeed())
           Expect(spec.RerunOnLostClaim).To(BeNil())
           Expect(spec.RerunOnLostClaimEnabled()).To(BeTrue())
       })

       It("decodes an explicit false", func() {
           var spec agentv1.ConfigSpec
           Expect(json.Unmarshal([]byte(`{"rerunOnLostClaim":false}`), &spec)).To(Succeed())
           Expect(spec.RerunOnLostClaim).ToNot(BeNil())
           Expect(*spec.RerunOnLostClaim).To(BeFalse())
           Expect(spec.RerunOnLostClaimEnabled()).To(BeFalse())
       })

       It("decodes an explicit true", func() {
           var spec agentv1.ConfigSpec
           Expect(json.Unmarshal([]byte(`{"rerunOnLostClaim":true}`), &spec)).To(Succeed())
           Expect(spec.RerunOnLostClaim).ToNot(BeNil())
           Expect(*spec.RerunOnLostClaim).To(BeTrue())
           Expect(spec.RerunOnLostClaimEnabled()).To(BeTrue())
       })

       It("omits the field when unset", func() {
           spec := agentv1.ConfigSpec{
               Assignee:  "claude",
               Image:     "registry/agent-claude",
               Heartbeat: "30m",
           }
           data, err := json.Marshal(spec)
           Expect(err).ToNot(HaveOccurred())
           Expect(string(data)).NotTo(ContainSubstring("rerunOnLostClaim"))
       })
   })
   ```

   c. AC4 — equality, four rows:
   ```go
   var _ = Describe("Equal - rerunOnLostClaim", func() {
       DescribeTable("compares the field by value",
           func(a, b *bool, expected bool) {
               specA := agentv1.ConfigSpec{
                   Assignee:         "claude",
                   Image:            "registry/agent-claude",
                   Heartbeat:        "30m",
                   RerunOnLostClaim: a,
               }
               specB := agentv1.ConfigSpec{
                   Assignee:         "claude",
                   Image:            "registry/agent-claude",
                   Heartbeat:        "30m",
                   RerunOnLostClaim: b,
               }
               Expect(specA.Equal(specB)).To(Equal(expected))
           },
           Entry("nil vs false differs", nil, boolPtr(false), false),
           Entry("true vs false differs", boolPtr(true), boolPtr(false), false),
           Entry("nil vs nil is equal", nil, nil, true),
           Entry("false vs false is equal", boolPtr(false), boolPtr(false), true),
       )
   })
   ```

   d. AC5 — deep copy does not share the pointed-to value, plus the unset case:
   ```go
   var _ = Describe("rerunOnLostClaim deep copy", func() {
       It("copies the pointed-to value", func() {
           spec := agentv1.ConfigSpec{
               Assignee:         "claude",
               Image:            "registry/agent-claude",
               Heartbeat:        "30m",
               RerunOnLostClaim: boolPtr(false),
           }
           copied := spec.DeepCopy()
           Expect(copied.RerunOnLostClaim).ToNot(BeNil())
           *copied.RerunOnLostClaim = true
           Expect(*spec.RerunOnLostClaim).To(BeFalse())
           Expect(*copied.RerunOnLostClaim).To(BeTrue())
       })

       It("keeps an unset field nil", func() {
           spec := agentv1.ConfigSpec{}
           copied := spec.DeepCopy()
           Expect(copied.RerunOnLostClaim).To(BeNil())
       })
   })
   ```

7. Add the schema-pruning guard to `/workspace/pkg/k8s_connector_test.go` (AC3). Inside the existing `Describe("desiredCRDSpec (via SetupCustomResourceDefinition)", ...)` block, immediately after the `It("declares every field AgentConfiguration reads from spec", ...)` block, add:
   ```go
   It("declares rerunOnLostClaim as an optional boolean", func() {
       // Same defect class as maxConcurrentJobs: a field added to ConfigSpec but
       // not to this schema is pruned from every Config on the next executor
       // start, and the failure reads as "the operator never set it".
       crd := getCRDFromCreateAction(cs.Actions())
       specProps := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]
       Expect(specProps.Properties).To(HaveKey("rerunOnLostClaim"))
       Expect(specProps.Properties["rerunOnLostClaim"].Type).To(Equal("boolean"))
   })
   ```
   `apiextensionsv1` is already imported in that file; the new `It` needs no new import.

8. Add the documentation row (AC6). In `/workspace/docs/agent-crd-specification.md`, in the `## Fields` table, insert after the `spec.trigger.statuses` row:
   ```
   | `spec.rerunOnLostClaim` | no | Whether to run the task again after another executor takes over this agent's expired claim. Default `true`: re-dispatch the task. `false` parks it at phase `human_review` for a person to decide — use it for agents whose second run has external side effects or is expensive. Independent of `max_triggers` and `spec.retries`; unset means `true`. |
   ```

9. Add the changelog entry (AC7). In `/workspace/CHANGELOG.md`, if a `## Unreleased` section already exists (an earlier prompt may have added one), add the bullet under it; otherwise insert a new `## Unreleased` section between the preamble (`All notable changes...`) and `## v0.23.4` — never above the `# Changelog` title:
   ```
   ## Unreleased

   - feat: add `spec.rerunOnLostClaim` to the Config CRD — a per-agent optional boolean (default `true`) deciding whether a task is re-dispatched or parked at `human_review` after another executor takes over this agent's expired claim; the property is declared in the executor-installed CRD schema so the API server does not prune it, and a pure `ConfigSpec.LostClaimAction` maps it to `RedispatchAfterLostClaim`/`ParkAfterLostClaim` (no runtime wiring yet)
   ```

10. Do NOT modify anything under `/workspace/pkg/` other than `k8s_connector.go` and `k8s_connector_test.go`. In particular, do NOT touch `pkg/config_resolver.go`, `pkg/agent_configuration.go`, `pkg/handler/`, or the spawner — carrying the field into the agent configuration and calling the decision after a takeover are explicitly a later wiring spec (spec Non-goals). No non-test Go file under `pkg/` may contain the identifiers `RerunOnLostClaim`, `RedispatchAfterLostClaim`, or `ParkAfterLostClaim` (the schema key is the lower-case JSON name `rerunOnLostClaim` and does not match that grep).
</requirements>

<constraints>
- Existing Configs that do not set the field decode, compare and render exactly as before; all existing tests pass.
- The field is an optional pointer-typed boolean with `json:"rerunOnLostClaim,omitempty"` and unset reads as `true` — a plain `bool` is forbidden because it cannot distinguish unset from `false` (spec Assumptions, AC2).
- The installed CRD schema must declare the property as `boolean`; a field added only to the Go type is pruned by the API server (the v0.5.0 `maxConcurrentJobs` failure).
- The decision is a two-way choice only: `RedispatchAfterLostClaim` or `ParkAfterLostClaim`. Do NOT add a third value, a per-task override, a validation rule beyond `boolean`, or a configurable default (spec Non-goals — invariant).
- Do NOT wire the decision into runtime behaviour. No executor code under `pkg/` reads the field or calls the decision function in this prompt (AC8). The decision lives in `k8s/apis/agent.benjamin-borbe.de/v1/`, outside `pkg/`.
- Do NOT carry the field through the agent-configuration resolver; the wiring spec does that.
- Do NOT change `max_triggers`, `spec.retries`, `maxConcurrentJobs`, or their semantics. The new field is independent of all three.
- Do NOT run `make generatek8s` / `hack/update-codegen.sh` — the script pins the wrong module path and regenerating rewrites `k8s/client` and breaks the build. Hand-add the deep-copy block in the generator's exact output shape.
- Do NOT modify the Helm chart CRD (it lives in another repo) — that is the named follow-up in the spec.
- Generated-file rule: only `zz_generated.deepcopy.go` is hand-touched, and only with the block shown above.
- Every new Go file carries the repo's BSD license header; `make precommit` runs `addlicense`.
- `## Unreleased` in `CHANGELOG.md` appears exactly once and stays shared with any other pending spec's bullets.
- Repo conventions are frozen: Ginkgo/Gomega v2 tests in `package v1_test`, `github.com/bborbe/errors` wrapping (never `fmt.Errorf`), golines 100-char line limit.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `make test` iteratively after each meaningful change (fast feedback loop), then `make precommit` ONCE at the very end. If `make precommit` fails with a `git rev-parse`/ROOTDIR error (the container's `.git` may be masked), re-run as `make ROOTDIR=/workspace precommit`.

Acceptance-criterion evidence (all container-executable):
- `go test -count=1 ./k8s/... ./pkg/...` — exits 0, with the new `LostClaimAction`, `rerunOnLostClaim JSON`, `Equal - rerunOnLostClaim`, `rerunOnLostClaim deep copy` and `declares rerunOnLostClaim as an optional boolean` specs green.
- `grep -n 'rerunOnLostClaim' pkg/k8s_connector.go` — at least 1 line (AC3).
- `grep -n 'RerunOnLostClaim' k8s/apis/agent.benjamin-borbe.de/v1/zz_generated.deepcopy.go` — at least 1 line (AC5).
- `grep -rnE 'RerunOnLostClaim|RedispatchAfterLostClaim|ParkAfterLostClaim' --include='*.go' pkg | grep -v '_test.go'` — 0 lines (AC8). The pipeline exits 1 when empty; that is the expected result, not a failure.
- `grep -n 'spec.rerunOnLostClaim' docs/agent-crd-specification.md` — a Fields-table row containing both `true` and `human_review` (AC6).
- `grep -c '^## Unreleased' CHANGELOG.md` — returns `1` (AC7).
- `awk '/^## /{sec=$0} /^- feat:/ && /rerunOnLostClaim/{print sec; exit}' CHANGELOG.md` — prints `## Unreleased` (AC7).
- `go test -mod=mod -coverprofile=/tmp/cover.out ./k8s/apis/agent.benjamin-borbe.de/v1/... ./pkg/... && go tool cover -func=/tmp/cover.out` — the new accessor, decision method and schema entry are exercised (≥80% statement coverage for new code).
- `make precommit` — exits 0 (AC9).

Do NOT run `docker`, `make build`, `make buca`, `kubectl`, `dark-factory`, or `gh` commands in this prompt — those are operator-executable and belong on the spec's verification ladder.
</verification>
