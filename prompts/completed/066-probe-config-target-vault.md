---
status: completed
summary: Added Config.spec.targetVault, resolved per Config in the healthcheck runner so both published commands carry the Config's vault (falling back to the executor's own), with CRD schema, equality, docs, tests, and changelog.
execution_id: agent-task-executor-lane-vault-exec-066-probe-config-target-vault
dark-factory-version: v0.198.17
created: "2026-11-10T21:36:44Z"
queued: "2026-11-10T21:59:56Z"
started: "2026-11-10T22:05:53Z"
completed: "2026-11-10T22:16:33Z"
---

# A Config names the vault its healthcheck probe is written to

<summary>
- A lane's Config can now name the vault its liveness-probe record belongs in
- Whichever executor fires the tick, the probe is written to that one vault, so one logical probe yields one record instead of one per executor
- A Config that names no vault keeps today's behaviour exactly: the probe goes to the probing executor's own task vault
- The field is optional and inert until an operator sets it on a Config; no existing Config changes behaviour
- Both commands the tick publishes carry the resolved vault, so the create and the paired update can never land in different vaults
- Nothing in the controller or vault-api changes
- Tests prove the override wins over the executor's own vault, and that an empty field still falls back to it
- The CRD field table documents the new field, and the changelog records the fix
</summary>

<objective>
A Config can name the vault its healthcheck probe record belongs in, and the probe prefers that vault over the probing executor's own — so one logical probe yields one record instead of one per executor, each copy in a different vault and a different phase. A Config that names no vault keeps today's behaviour exactly.
</objective>

<context>
Read CLAUDE.md if present for project conventions (this worktree has none; follow the surrounding code).

Read these before changing anything — they carry the house style this change must adopt:

- `pkg/probe/probe.go` — the whole file. `healthcheckRunner.Run` loops over every Config from `configProvider.Get`, picks a phase with `probePhase(config.Spec.Trigger)`, then publishes a `taskcmd.CreateCommand` and a `taskcmd.UpdateFrontmatterCommand` per Config. `TargetVault: r.targetVault` is stamped at two sites: the create command's struct literal (~line 176) and the update command's (~line 191). `NewHealthcheckRunner`'s GoDoc explains why each of the two needs the stamp, and the update site carries a second comment repeating the load-bearing half; both reasonings are correct and must survive. `probePhase` is the exemplar for a small unexported per-Config helper: placed just above `Run`, GoDoc explaining what it returns and why.
- `pkg/probe/probe_test.go` — Ginkgo/Gomega with Counterfeiter fakes `mocks.FakeConfigProvider` / `mocks.FakeCommandPublisher`. The "probe phase follows the Config trigger" table asserts a frontmatter key on both published commands by type-asserting `publisher.PublishArgsForCall(n)`'s payload; the "both published commands stamp the executor's configured vault (regression)" table is the nearest sibling to this change and explains why two distinct vault values are table-tested (a single value cannot tell a computed value from a literal that happens to equal it).
- `k8s/apis/agent.benjamin-borbe.de/v1/types.go` — `ConfigSpec` and its neighbours. `JobNamespace` and `ConfigMapName` show the doc-comment convention: what the field does, what empty means, and why the field exists. `Trigger *Trigger` is the field this one belongs beside, since both exist for the probe.
- `main.go` — `effectiveTaskVault()` (TASK_VAULT defaulting to VAULT_NAME) and the `factory.CreateHealthcheckRunner(..., a.effectiveTaskVault())` call site. That value is the runner's `targetVault` today; this change makes it the fallback rather than the only source. It also carries `vaultSlugRegexp` (`^[a-z][a-z0-9-]*$`, ~line 100) — the in-repo statement of the same rule the new `crdVaultSlug` must express; cite it in the GoDoc alongside the consumer's `validateCreateTargetVault`.
- `pkg/k8s_connector.go` — `configSpecProperties()` and `configSpecSchema()`. **Read the GoDoc on `configSpecSchema` before editing anything**: it states that any field added to the CRD must be added to that map too, or the API server prunes it from every Config, silently. `desiredCRDSpec` sets `PreserveUnknownFields: false`. `crdDNS1123Label` (lines 76-80) and the `jobNamespace` / `priorityClassName` properties (lines 408, 411) are the pattern precedent — but note `crdDNS1123Label` accepts a leading digit and the vault-slug rule does not.
- `pkg/k8s_connector_internal_test.go` — the existing `It` "declares jobNamespace so the API server cannot prune it" is the test to mirror.
- The coding plugin's `go-kubernetes-crd-controller-guide.md` § `SetupCustomResourceDefinition` — states the rule this change depends on: do not ship a separate CRD YAML manifest, the schema lives in Go code. (Read it if it is reachable in this container; the GoDoc in `pkg/k8s_connector.go` carries the same rule and is authoritative for this repo.)
- `docs/agent-crd-specification.md` § Fields — a table of every `spec.*` field with a "no"/"yes" required column and a prose cell; `spec.jobNamespace` and `spec.rerunOnLostClaim` are the shape to copy.

Measured baseline (do not re-derive, do not go looking for the records): every executor's `/healthcheck-trigger` probes every Config on the fleet, so three executors stamp three different vaults on the same logical probe. On 2026-11-10 `task_identifier be811b32` existed as `private-agent` (openclaw, done/aborted), `private-personal` (personal, human_review/in_progress) and `private-trading` (trading, done/completed). `ConfigSpec` has no `targetVault` field today.
</context>

<requirements>
1. In `k8s/apis/agent.benjamin-borbe.de/v1/types.go`, add to `ConfigSpec` a new optional field, placed adjacent to `Trigger`:

   ```go
   TargetVault string `json:"targetVault,omitempty"`
   ```

   Document it in the `JobNamespace` / `ConfigMapName` style — three things: it is the vault this Config's healthcheck probe records are written to; empty means the probing executor's own TASK_VAULT, so every Config that does not set it behaves exactly as before; and it exists because every executor probes every Config, so without it one logical probe record is written once per executor, each copy in a different vault and a different phase. Keep the comment to the field's contract and reason — no measured identifiers, no incident narrative. Also add `s.TargetVault == o.TargetVault` to `ConfigSpec.Equal` (it enumerates every other field, `JobNamespace` and `Trigger` included), and in `types_test.go` add a `Describe("Equal with targetVault")` mirroring `Describe("Equal with jobNamespace")` — without the Equal clause two Configs that differ only in `targetVault` compare equal, and any `libk8s.Type` equality consumer would skip the update.

1b. In `pkg/k8s_connector.go`, add the `targetVault` property to `configSpecProperties()`. **This is mandatory, not optional**: that map is the authoritative CRD schema the executor installs on every start (`SetupCustomResourceDefinition` → `desiredCRDSpec`), and `desiredCRDSpec` sets `PreserveUnknownFields: false`, so an unlisted `spec.targetVault` is pruned from every Config by the API server and the field always reads empty — this whole change would be inert in production while every test and every grep below passes. The function's own GoDoc records the precedent: *"v0.5.0 shipped maxConcurrentJobs … not here, so the field was pruned everywhere … the feature was inert in prod for weeks."*
   - Add a package-level constant beside `crdDNS1123Label` (`pkg/k8s_connector.go:76-80`), because the vault slug rule is **not** the DNS-1123 label rule — `crdDNS1123Label` (`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`) accepts a leading digit, while the consumer's validator (`bborbe/agent` `validateCreateTargetVault`) requires `^[a-z][a-z0-9-]*$`. Name it for what it is and document why it differs:
     ```go
     // crdVaultSlug is the shape a vault slug must have: it is what bborbe/agent's
     // validateCreateTargetVault accepts, so a value this pattern admits can never be
     // rejected later by the command validator. Deliberately not crdDNS1123Label, which
     // allows a leading digit.
     const crdVaultSlug = "^[a-z][a-z0-9-]*$"
     ```
   - Add to the properties map, beside the other probe/namespace-shaped entries:
     ```go
     "targetVault": {Type: "string", Pattern: crdVaultSlug},
     ```
   - In `pkg/k8s_connector_internal_test.go`, add an `It` mirroring *"declares jobNamespace so the API server cannot prune it"*: `props := configSpecProperties()`, then assert `props` has the `targetVault` key, its `Type` is `"string"`, and its `Pattern` is `crdVaultSlug`.
   - Also add a `DescribeTable` that **exercises** the pattern, because comparing `Pattern` to `crdVaultSlug` asserts what was typed, not what admission accepts. Compile `regexp.MustCompile(props["targetVault"].Pattern)` and assert it matches `openclaw`, `personal`, `private-agent`, `private-personal`, `private-trading`, and rejects `Private`, `1abc`, `private_agent`, `private agent`, `-private`. The hyphenated entries are the point: every vault this change's baseline names is hyphenated and no other test in this prompt uses one, so a pattern that lost `-` from its character class would pass every other assertion and then reject those vaults at Config admission — the same silent-inert failure this requirement exists to prevent.

2. In `pkg/probe/probe.go`, add an unexported helper beside `probePhase`, mirroring its placement and GoDoc style:

   ```go
   func probeTargetVault(config agentv1.Config, fallback string) string
   ```

   It returns `config.Spec.TargetVault` when that is non-empty, otherwise `fallback`. GoDoc states the preference and what the empty case preserves (the probing executor's own task vault, the pre-existing behaviour).

3. In `healthcheckRunner.Run`, resolve the vault once per Config with that helper (passing `r.targetVault` as the fallback) and use the resolved value at both stamp sites: the `CreateCommand`'s `TargetVault` and the `UpdateFrontmatterCommand`'s `TargetVault`. Neither site may read `r.targetVault` directly any more — the create and the paired update must carry the same vault for every Config. Nothing else in either command changes (`TaskIdentifier`, `Title`, `Frontmatter`, `Body`, `Updates` all stay as they are).

4. Extend the comments, do not replace them. `NewHealthcheckRunner`'s GoDoc (`pkg/probe/probe.go:88-102`) has **three** paragraphs. The opening one currently says `targetVault` is *"the vault this executor owns … stamped onto BOTH commands"* — that becomes false, so change it to describe it as the **fallback** vault this executor owns, overridden by each Config's `spec.targetVault` when set, with the resolved value stamped onto both commands. Keep the existing two reasoning paragraphs (create → legacy-default substitution and cross-vault skip; update → stamped miss is a failure, unstamped stays a skip) verbatim, and add that the value stamped is now resolved per Config: a Config's `targetVault` wins, empty falls back to this executor's vault, and the fallback is what every Config that does not set the field keeps. In the inline comment above the update command's `TargetVault`, keep the existing reasoning verbatim and add one clause noting the stamped value is the per-Config resolved vault rather than unconditionally this executor's. State the reason for the override in one sentence: every executor probes every Config, so stamping only the executor's own vault writes one logical probe once per executor, each copy in a different vault and a different phase.

5. In `pkg/probe/probe_test.go`, add a `Context` (sibling to the existing vault-regression context) that drives a runner whose own vault is `personal` and asserts the `TargetVault` field on BOTH published commands — create at call 0, update at call 1, type-asserted from `PublishArgsForCall` exactly as the existing cases do, checking the operation string first:
   - `DescribeTable` "a Config's targetVault wins over the runner's own vault": `TargetVault: "trading"` → both commands `trading`; `TargetVault: "openclaw"` → both commands `openclaw`; `TargetVault: "private-trading"` → both commands `private-trading`. Non-default values on purpose — a single one cannot tell a value read from the Config from a literal that happens to equal it — and the hyphenated entry is the shape the schema pattern must admit.
   - `DescribeTable` "an empty targetVault falls back to the runner's own vault": `TargetVault: ""` → both commands `personal`, and `NotTo(BeEmpty())` on both, since an empty `TargetVault` makes the controller fall back to `openclaw` and skip the create.
   - One non-table test that proves the resolution is per Config and not per run: a single `Get` returning two Configs (give them distinct assignees, e.g. `agent-a` / `agent-b`) — the first with `TargetVault: "trading"`, the second with it empty — must produce 4 publishes with call 0 create `trading`, call 1 update `trading`, call 2 create `personal`, call 3 update `personal`.
   - For the override case, also assert the published commands pass the library validator: `createCmd.Validate(ctx)` and `updateCmd.Validate(ctx)` must both `Succeed()` for a Config with `TargetVault: "trading"`. `CreateCommand.Validate` / `UpdateFrontmatterCommand.Validate` enforce `^[a-z][a-z0-9-]*$` on `TargetVault` (`bborbe/agent` `command/task/create-command.go:135-152`); the existing validation context only exercises the runner's own configured vault (`personal`, i.e. the fallback path), so without this a Config-derived vault is never run through the validator the consumer applies.
   Do not weaken or delete the existing phase assertions or the existing vault-regression table; every existing case must still pass unchanged.

5b. In `k8s/apis/agent.benjamin-borbe.de/v1/types_test.go`, add a `Describe("JSON round-trip for targetVault")` beside the existing `Describe("JSON round-trip for priorityClassName")`, with two `It`s:
   - a `ConfigSpec{Assignee: "claude-agent", Image: "example/image:latest", Heartbeat: "30m", TargetVault: "trading"}` survives `json.Marshal` → `json.Unmarshal` with `decoded.TargetVault == "trading"` — this is the wire contract the operator's `spec.targetVault` is deserialised through;
   - the same spec without `TargetVault` marshals to JSON whose string form does not contain `targetVault` (the `omitempty` half).
   A mistyped struct tag compiles and passes every struct-level test while silently never reading the operator's field, so this round-trip is the only thing that catches it.

6. In `docs/agent-crd-specification.md` § Fields, add a `spec.targetVault` row in the table's existing format (required column `no`): the vault this Config's healthcheck probe records are written to; empty means the probing executor's own task vault, so an unset field is a no-op on deploy; it exists because every executor probes every Config, so without it one logical probe is written once per executor, each copy in a different vault. Put the row immediately after `spec.trigger` (the only probe-related row) rather than at the end of the table, and carry the value syntax the `spec.jobNamespace` row also documents: *lowercase letters, digits and `-`, starting with a letter (`^[a-z][a-z0-9-]*$`), rejected at admission.* Do not restructure the table or edit other rows.

7. In `CHANGELOG.md`, add a `## Unreleased` heading directly above the first `## v` heading (currently `## v0.58.1`) with exactly one bullet:

   ```markdown
   - fix: a Config can name the vault its healthcheck probe is written to (`spec.targetVault`); every executor probing that Config now stamps it on both the create-task and the paired update-frontmatter, so one logical probe yields one record instead of one per executor in a different vault and phase. Empty keeps today's behaviour — the probing executor's own TASK_VAULT.
   ```

8. Self-check before finishing: re-run `<verification>`, then walk each requirement above against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT change the controller (`agent-task-controller`) or `vault-api` — neither lives in this tree, and no change to their behaviour is part of this fix.
- Do NOT change `factory.CreateHealthcheckRunner` or the `main.go` call site: the runner's constructor signature and the executor's own task-vault argument are unchanged, so no wiring edit is part of this fix.
- Do NOT change `probeTaskID`, `probeNamespace`, or the task identifier derivation — the comment there forbids it.
- Do NOT change `probePhase` or the phase selection; only the vault resolution is new.
- Enforce the slug shape in the CRD schema, not in `Run`: the `targetVault` property carries `Pattern: crdVaultSlug`, the same admission rule `spec.jobNamespace` and `spec.priorityClassName` already carry, so a non-slug value is rejected when the Config is written and never reaches the probe. Do NOT add a CEL expression, a `ConfigSpec.Validate` check, or a skip-and-log branch in `Run` for a value the schema already rejects — that branch would be untested code guarding a case admission makes unreachable.
- Do NOT add a CRD **YAML** manifest — but you MUST add `targetVault` to the in-repo OpenAPI schema in `pkg/k8s_connector.go` (requirement 1b). `desiredCRDSpec` sets `PreserveUnknownFields: false`, so an unlisted `spec.targetVault` is pruned from every Config and the field always reads empty.
- Do NOT change `status`, `phase`, `trigger_count`, `current_job`, `job_started_at`, `spawn_notification`, `task_type`, `assignee`, or `stage` in either command.
- Preserve the existing comment reasoning at both stamp sites and in `NewHealthcheckRunner`'s GoDoc — extend, never replace.
- Use `github.com/bborbe/errors` for any error wrapping; no `fmt.Errorf` for new error paths.
- Do NOT run `go mod vendor`; never use `-mod=vendor`.
- Existing tests must still pass.
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit` — must pass.

Then, from the repo root:

- `! grep -q 'TargetVault: r.targetVault' pkg/probe/probe.go` — must succeed (neither stamp site reads the runner field directly).
- `grep -c 'r\.targetVault' pkg/probe/probe.go` — must print `1` (the single fallback argument passed to the helper inside `Run`). The previous check alone passes if the agent inlines `TargetVault: probeTargetVault(config, r.targetVault),` at both sites, which is the same direct read spelled differently.
- `grep -c 'targetVault,omitempty' k8s/apis/agent.benjamin-borbe.de/v1/types.go` — must print `1`.
- `grep -c '"targetVault"' pkg/k8s_connector.go` — must print `1` (the CRD schema property; without it the API server prunes `spec.targetVault` from every Config and the feature is inert).
- `grep -c 's.TargetVault == o.TargetVault' k8s/apis/agent.benjamin-borbe.de/v1/types.go` — must print `1` (`ConfigSpec.Equal` enumerates every field).
- `grep -cE '^[[:space:]]*TargetVault:[[:space:]]' pkg/probe/probe.go` — must print `2` (both struct-literal stamp sites use the resolved value).
- `grep -q 'spec.targetVault' docs/agent-crd-specification.md` — must succeed.
- `grep -c '^## Unreleased$' CHANGELOG.md` — must print `1`.
- `awk '/^## /{sec=$0} /spec.targetVault/{print sec}' CHANGELOG.md` — must print `## Unreleased`.
</verification>
