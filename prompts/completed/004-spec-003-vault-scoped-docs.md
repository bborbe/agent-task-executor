---
status: completed
spec: [003-vault-scoped-config-resolver]
summary: 'Documented the vault-scoped {assignee}-{vaultName} match rule in the Config CRD spec (updated the spec.assignee Fields row and added a per-vault executor paragraph); the CHANGELOG ## Unreleased feat: vault entry already existed from prompt 1 and was left intact'
execution_id: agent-task-executor-vaultslot-exec-004-spec-003-vault-scoped-docs
dark-factory-version: dev
created: "2026-08-26T20:24:00Z"
queued: "2026-08-26T20:33:07Z"
started: "2026-08-26T20:42:33Z"
completed: "2026-08-26T20:48:48Z"
---

<summary>
- The Config CRD specification now documents the vault-scoped match rule instead of only naming the assignee field.
- The documentation states that a per-vault executor matches Config CRs by the composed `{assignee}-{vaultName}` value and that a plain unsuffixed assignee never matches.
- The changelog records the change under a fresh `## Unreleased` section.
- Only the new `## Unreleased` section is touched; no released changelog section is rewritten.
- No release is cut — release stays manual (`autoRelease:false`).
</summary>

<objective>
Document the settled vault-scoped match semantics in the Config CRD specification and record the change in the changelog under a fresh `## Unreleased` section, so operators deploying per-vault executors know how `spec.assignee` is matched and the change is visible to consumers. This is prompt 2 of 2: prompt 1 shipped the code (vault-scoped resolver), and this prompt documents the now-settled semantics.
</objective>

<context>
Read README.md for project orientation (this repo has no CLAUDE.md).

Read these files fully before changing anything:
- `/workspace/docs/agent-crd-specification.md` — the `spec.assignee` row in the Fields table (currently: "Matches the `assignee` field in task frontmatter") is where the composed match rule must be documented.
- `/workspace/CHANGELOG.md` — currently starts at `## v0.6.7` with no `## Unreleased` section; add one at the top.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` section placement, `feat:` prefix, only touch the Unreleased section.
</context>

<requirements>
1. In `/workspace/docs/agent-crd-specification.md`, update the `spec.assignee` row in the Fields table so it states the composed match rule. The updated row MUST contain both the word `assignee` and `vaultName` on the same line (acceptance grep). Suggested wording:
   ```
   | `spec.assignee` | yes | With a per-vault executor (one per Obsidian vault), matches the composed `{assignee}-{vaultName}` value — the task's plain `assignee` joined with the executor's `VAULT_NAME` (e.g. task `github-update-go-agent` + `VAULT_NAME=personal` → `github-update-go-agent-personal`). A Config CR whose `assignee` is the plain value without the vault suffix never matches. |
   ```

2. Add a short paragraph (2-4 sentences) under the Fields table (or in a small new subsection) explaining the per-vault executor model: each executor instance serves one Obsidian vault, learns its vault from the required `VAULT_NAME` environment variable, and resolves Config CRs by the composed `{assignee}-{vaultName}` key; Config CR names follow the `{assignee}-{vault}` convention; a Config CR without the vault suffix never matches, so unmatched tasks keep the existing unknown-assignee skip behavior. Do NOT change the YAML examples, the field types, the other table rows, or the Future Extensions section.

3. In `/workspace/CHANGELOG.md`, insert a `## Unreleased` section immediately after the `# Changelog` preamble (currently above `## v0.6.7`) — never before the `# Changelog` header (changelog-guide preamble-frozen rule) — with a `feat:` bullet describing the vault-scoped resolution. The bullet MUST contain the word `vault` (acceptance grep). Example:
   ```
   ## Unreleased

   - feat: scope Config CR resolution to the executor's own vault — the executor now requires `VAULT_NAME`
     and resolves each task's Config CR by the composed `{assignee}-{vaultName}` key (e.g.
     `github-update-go-agent-personal`), so a per-vault executor applies its own vault's `maxConcurrentJobs`
     slot. A plain assignee without the vault suffix never matches; unmatched tasks keep the existing
     unknown-assignee skip path.
   ```
   If a `## Unreleased` section already exists, append the bullet instead of adding a second heading.
</requirements>

<constraints>
- CHANGELOG edits touch only the new `## Unreleased` section; no released section is rewritten.
- Release stays manual (`autoRelease:false` in `.dark-factory.yaml`) — do NOT cut a release, bump a version, or create a tag.
- The doc change describes the settled match semantics only — do NOT change any resolver code, Config CR schema, or field types in this prompt.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run in `/workspace`:
```
make precommit
grep -n '^## Unreleased' CHANGELOG.md
sed -n '/^## Unreleased/,/^## /p' CHANGELOG.md | grep -c 'vault'
grep -n 'vaultName' docs/agent-crd-specification.md | grep -c 'assignee'
```
Expected:
- `make precommit` exits 0.
- `grep -n '^## Unreleased' CHANGELOG.md` returns ≥ 1 line.
- The `## Unreleased` section contains the word `vault` (count ≥ 1).
- `docs/agent-crd-specification.md` has ≥ 1 line containing both `vaultName` and `assignee`.

If `make precommit` exits non-zero, the prompt is NOT done — fix and re-run the failing target until it passes, then run `make precommit` once more.

Before you finish: re-run the verification block and confirm every expected line holds; walk each acceptance criterion against the change.
</verification>

## Definition of Done

- [ ] `docs/agent-crd-specification.md` documents the composed `{assignee}-{vaultName}` match rule (a line containing both `vaultName` and `assignee`)
- [ ] A short per-vault executor paragraph explains `VAULT_NAME`, the composed key, and that plain unsuffixed assignees never match
- [ ] `CHANGELOG.md` has a `## Unreleased` section with a `feat:` bullet mentioning `vault`
- [ ] No released CHANGELOG section rewritten; no release cut
- [ ] `make precommit` exits 0
