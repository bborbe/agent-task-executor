---
status: approved
spec: [014-job-watcher-sandbox-namespaces]
created: "2026-10-23T18:45:00Z"
queued: "2026-10-23T17:26:27Z"
branch: dark-factory/job-watcher-sandbox-namespaces
---

<summary>
- The changelog records that the Job watcher now observes every Config's sandbox namespace, as a `feat:` bullet so the release cut yields the next minor tag.
- The CRD reference records the observation surface next to the `jobNamespace` field, so the fact outlives this spec.
- The note says the watched set is derived from the Configs and that a namespace a Config stops naming stays watched until restart.
- The field's own semantics and rejection rule are unchanged; only the watcher's observation is documented.
- No code, RBAC, or manifest changes.
</summary>

<objective>
Record the watcher change in `CHANGELOG.md` and document the watcher's observation surface in `docs/agent-crd-specification.md`. End state: the changelog carries a `feat:` bullet containing the phrase `sandbox namespace`, and the CRD reference states that the watched namespaces come from the Configs and that a removed namespace is watched until restart.
</objective>

<context>
Read these files fully before editing:

- `CHANGELOG.md` — the top is `# Changelog`, an intro paragraph, then `## v0.31.0` as the first section. There is no `## Unreleased` heading today; create it above `## v0.31.0`. Entry format is `- <prefix>: <what> [context]`; this is a new capability, so the prefix is `feat:`.
- `docs/agent-crd-specification.md` — the `spec.jobNamespace` row in the "Fields" table (line ~52) documents the field's semantics and rejection rule. Do not change that row's rules. Add a short paragraph (or extend the row) describing the watcher's observation surface.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — full changelog style rules.
- Prompts 1-3 (executed before this one) introduce the implementation: the watcher takes the Config provider, derives the namespace set as the executor namespace plus every resolved `jobNamespace`, starts one informer pair per namespace, re-resolves every 60 seconds, and never stops an informer for a namespace that leaves the set. The spec is the source for these facts — this prompt edits no Go file, so do not go looking for them in `pkg/job_watcher.go`.

Do not describe manifests, RBAC, or chart work; those live in the separate `bborbe/agent` repository and are out of scope for this spec.
</context>

<requirements>
1. In `CHANGELOG.md`, insert a new `## Unreleased` heading immediately above `## v0.31.0` (one blank line between sections) with exactly one bullet:
   ```markdown
   - feat: **the Job watcher now observes every Config's sandbox namespace.** The watcher derives its watched set from the executor namespace plus every distinct `spec.jobNamespace` its Configs name, starts one Job/Pod informer pair per namespace, and re-resolves the set every 60 seconds so a Config flip is picked up without an executor restart. A terminal-failed Job in a sandbox namespace now produces the same synthetic failure and store cleanup as one in the executor namespace. A namespace no Config names is never watched, no informer is stopped for a namespace a Config stops naming, and no new RBAC is requested.
   ```
   The bullet MUST begin with `- feat: ` and contain the exact phrase `sandbox namespace` (lowercase). A `feat:` bullet is what bumps the next minor tag.
2. In `docs/agent-crd-specification.md`, add the observation-surface note without changing the field's rules. Add a paragraph immediately after the "Fields" table (or as a new subsection `## Job and Pod Observation`) containing at least these two facts:
   - the watched namespace set is derived **from the Configs** — the executor namespace plus every distinct `spec.jobNamespace` any Config names;
   - a namespace a Config stops naming keeps its informer **until restart**.
   Example wording (use it or close to it):
   ```markdown
   The executor's Job watcher observes Jobs and Pods in the executor's own namespace plus every distinct `spec.jobNamespace` named by a Config. The watched set is re-resolved every 60 seconds from the Configs, so a Config that starts naming a new namespace is observed without an executor restart. A namespace that a Config stops naming is not un-watched: its informer keeps running until restart. A namespace no Config names is never watched, and the executor requests no RBAC beyond what it already holds.
   ```
   The text MUST contain `from the Configs` and `until restart` (the acceptance check is case-insensitive). Do not change the `spec.jobNamespace` row's description of the default, the rejection rule, or the DNS-1123 syntax rule.
3. Do not modify any Go file, the spec file, or any manifest.
4. Before you finish, re-run `<verification>` and confirm every check passes; walk AC12 and AC13 from the spec against the two edits.
</requirements>

<constraints>
- The documented behaviour must match prompts 1-3: derived from the Configs, 60-second re-resolution, no informer stopped, no new RBAC.
- Do not document a namespace, NetworkPolicy, RBAC grant, or chart change; those are out of scope and live in `bborbe/agent`.
- Do not change the `spec.jobNamespace` field's semantics or rejection rule.
- Do not add an RBAC manifest of any kind; the no-new-RBAC check is a diff guard run on the host after merge, and this prompt introduces no such file.
- Do NOT edit the spec file.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run from the repository root:

```bash
grep -ci 'from the Configs' docs/agent-crd-specification.md
grep -ci 'until restart' docs/agent-crd-specification.md
awk '/^## /{sec=$0} /^- feat: / && /sandbox namespace/{print sec; exit}' CHANGELOG.md | grep -qx '## Unreleased'
```

Both greps must print a number ≥ 1. The `awk | grep -qx` pipeline must exit 0: the bullet's section must be exactly `## Unreleased`. Do not accept a `## vX.Y.Z` section — at prompt time the release has not cut, and the changelog guide requires `## Unreleased` on feature branches. A bullet filed under `## v0.31.0` folds a feature into a released section.

Then:

```bash
GOFLAGS=-buildvcs=false make precommit VERSION=dev
```

`make precommit` must exit 0.
</verification>
