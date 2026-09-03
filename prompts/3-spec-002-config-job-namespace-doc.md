---
status: draft
spec: [002-config-job-namespace]
created: "2026-09-03T17:50:40Z"
branch: dark-factory/config-job-namespace
---

<summary>
- The Config CRD reference now documents the optional `spec.jobNamespace` field.
- The doc states the field's default — a per-environment `<executor-namespace>-agents-sandbox` sandbox — so operators know the safe value is applied when the field is omitted.
- The doc states the rejection rule — setting `jobNamespace` to the executor namespace is refused — so operators know the isolation guard exists and is not overridable.
- The row documents the settled semantics implemented by the two prior prompts, not merely the field name.
- No code changes — documentation only.
</summary>

<objective>
Document the settled `spec.jobNamespace` semantics in the Config CRD specification so operators know both the safe default and the isolation guard, satisfying the spec's acceptance criterion that the doc must state the default AND the rejection rule.
</objective>

<context>
Read `/workspace/docs/agent-crd-specification.md` fully before changing it — in particular the "Fields" table (the field rows use the pattern `| \`spec.xxx\` | no | <description> |`; see the `spec.priorityClassName` and `spec.trigger` rows for the style of a field with default/derived semantics).

No code files are touched by this prompt. The field, the `ResolveJobNamespace` function, and the spawner wiring already landed in the two prior prompts — the doc row must describe those settled semantics.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/documentation-guide.md` — docs conventions.
</context>

<requirements>
1. In `/workspace/docs/agent-crd-specification.md`, add a row to the "Fields" table for `spec.jobNamespace`. Place it directly after the `spec.priorityClassName` row. The row must document the default AND the rejection rule, in the existing table style:
   ```
   | `spec.jobNamespace` | no | Kubernetes namespace the agent's Jobs are spawned into. Default: `<executor-namespace>-agents-sandbox` (derived per environment, so dev and prod never share a sandbox). Setting `jobNamespace` to the executor namespace is rejected — no Config can opt out of isolation. |
   ```
   The row text MUST contain all three of: `jobNamespace`, `agents-sandbox`, and `executor namespace` (as substrings — the acceptance criterion greps for them). Keep the description accurate to the implemented behavior: empty → derived sandbox; explicit other value → that value; equal to the executor namespace → rejected.

2. Do NOT change any other content of the file (the CRD example YAML, the "Who Uses the CRD" table, or the field rows for other `spec.*` fields stay as-is). No code changes in this prompt.
</requirements>

<constraints>
- This is a documentation-only prompt — no Go, YAML, or Makefile changes.
- The row must state the field's default and its rejection rule, not merely the field name (a bare name-only row would pass a name grep while documenting nothing).
- Do NOT invent behaviors not implemented by the two prior prompts (empty → sandbox; explicit → that value; executor's own namespace → rejected; every spawned pod stamped `app: agent` is already covered elsewhere in the doc's scope and needs no mention here).
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass (there are no code changes, so `make precommit` is not required for this prompt).
</constraints>

<verification>
- `grep -c 'jobNamespace' docs/agent-crd-specification.md` — outputs ≥1.
- `grep -c 'agents-sandbox' docs/agent-crd-specification.md` — outputs ≥1.
- `grep -ci 'executor namespace' docs/agent-crd-specification.md` — outputs ≥1.
- `grep -n 'spec.jobNamespace' docs/agent-crd-specification.md` — shows the row and that its description mentions the sandbox default and the executor-namespace rejection.

Do NOT run `docker`, `make build`, `kubectl`, `dark-factory`, or `git` commands in this prompt — the container cannot execute them and the daemon does not check their exit codes.
</verification>
