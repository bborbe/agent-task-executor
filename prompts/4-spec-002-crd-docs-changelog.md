---
status: draft
spec: [002-config-job-namespace]
created: "2026-10-23T11:05:00Z"
branch: dark-factory/config-job-namespace
---

<summary>
- The CRD reference now documents the new namespace field, not merely its name: it states that the default is the executor's namespace and that naming the executor's namespace is rejected.
- The "Who Uses the CRD" table credits the Job Creator with reading the field.
- The changelog records the change under a new `## Unreleased` heading, above today's released version.
- No code changes; this prompt lands the settled semantics once the implementation prompts have shipped.
</summary>

<objective>
Document `spec.jobNamespace` in `docs/agent-crd-specification.md` with its default and its rejection rule, and record the feature under `## Unreleased` in `CHANGELOG.md`. End state: a reader of the CRD reference knows that an unset field means the executor's own namespace and that an explicit value equal to the executor namespace is refused, and the changelog carries the change above the released versions.
</objective>

<context>
Read these files fully before editing:

- `docs/agent-crd-specification.md`
  - The "Who Uses the CRD" table has a `Job Creator` row listing `spec.image`, `spec.resources`, `spec.env`, `spec.secretName`, `spec.volumeClaim`, `spec.volumeMountPath`.
  - The "Fields" table has one row per field, in the form `| `spec.<name>` | yes/no/conditional | <description> |`. Existing rows such as `spec.priorityClassName` and `spec.rerunOnLostClaim` show the expected prose style.
- `CHANGELOG.md`
  - The top is `# Changelog`, an intro paragraph, then `## v0.30.6` as the first section. There is no `## Unreleased` heading today; it must be created above `## v0.30.6`.
  - Entry format: `- <prefix>: <what> [context]`. This is a new capability, so the prefix is `feat:`.
  - Read `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` for the full style rules.
- Implementation already landed in prompts 1-3: `ConfigSpec.JobNamespace` (json `jobNamespace`), `pkg.ResolveJobNamespace`, spawner create in the resolved namespace, and all four lookups taking the namespace. This prompt documents exactly that behaviour; do not describe manifests or policy work, which live in `bborbe/agent`.
</context>

<requirements>
1. In `docs/agent-crd-specification.md`, add a row to the "Fields" table (place it after the `spec.priorityClassName` row):
   ```markdown
   | `spec.jobNamespace` | no | Namespace the executor creates this Config's Jobs in. Empty (the default) means the executor namespace — the Job lands beside the executor exactly as before, so an unset field is a no-op on deploy. A value equal to the executor namespace is rejected at spawn time with an ERROR log and no Job is created, so a Config can never opt out of isolation by naming the executor namespace. The namespace must already exist and already hold this Config's PVCs and Secrets before the field is set. |
   ```
   The text MUST contain the literal `jobNamespace` and the phrase `executor namespace`.
2. In the same file, add `spec.jobNamespace` to the `Job Creator` row of the "Who Uses the CRD" table, alongside the other fields that row lists.
3. In `CHANGELOG.md`, insert a new `## Unreleased` heading immediately above `## v0.30.6` (keep one blank line between sections), with exactly one bullet:
   ```markdown
   - feat: **agent Jobs can be spawned into a separate namespace.** `ConfigSpec` gains `spec.jobNamespace`; empty (the default) keeps creating the Job in the executor's own namespace, so the deploy is a no-op until a Config sets it. A non-empty value places the Job there, and a value equal to the executor's own namespace is rejected with an ERROR log and no Job created, so no Config can author an opt-out. `IsJobActive`, `JobExists`, `CountActiveJobs` and `ListActiveJobs` all resolve the same namespace the Job was created in, so the duplicate-spawn guard and the concurrency cap keep working across namespaces. Spawned Job pods keep the unconditional `app: agent` label.
   ```
   The bullet MUST contain `jobNamespace` (any case) so the acceptance check finds it under `## Unreleased`.
4. Do not modify any Go file, the spec file, or the chart (`bborbe/agent` is a separate repository).
</requirements>

<constraints>
- The documented default must match the implementation: empty means the executor's namespace, preserving existing behaviour exactly.
- Do not document a namespace, NetworkPolicy or RBAC creation step — those are out of scope and live in `bborbe/agent`.
- Do NOT edit the spec file.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run from the repository root:

```bash
grep -c 'jobNamespace' docs/agent-crd-specification.md
grep -ci 'executor namespace' docs/agent-crd-specification.md
awk '/^## /{sec=$0} /[Jj]ob[Nn]amespace/{print sec}' CHANGELOG.md
```

Both greps must print a number ≥ 1, and the `awk` output must contain `## Unreleased` (and must not be a released `## vX.Y.Z` section).

Then:

```bash
ROOTDIR=/workspace make precommit
```

`make precommit` must exit 0.
</verification>
