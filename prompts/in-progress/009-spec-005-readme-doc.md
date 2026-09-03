---
status: approved
spec: [005-bug-executor-restart-drops-deferred-respawn-queue]
created: "2026-09-03T18:04:37Z"
queued: "2026-09-03T18:18:58Z"
branch: dark-factory/bug-executor-restart-drops-deferred-respawn-queue
---

<summary>
- The executor's `README.md` documents the three new environment variables the reconcile loop is configured from — `GITREST_URL`, `GITREST_GATEWAY_SECRET`, `TASK_GLOB` — so an operator deploying or debugging the executor knows what each controls and what the defaults are.
- The README states that `GITREST_GATEWAY_SECRET` references a K8s Secret by NAME (data key `gateway-secret`), never the secret value, and that the executor's ServiceAccount must be able to read that Secret.
- The README notes the deployment dependency: the Helm chart and config-repo values for these variables live outside this repo and are wired on the operator side (the chart must pass the secret NAME and grant `secrets/get` to the executor's ServiceAccount).
- No Go, YAML, or chart changes — documentation only.
</summary>

<objective>
Document the new executor env config surface added by the reconcile-loop feature (spec 005) in `README.md`, so operators know the variable names, defaults, semantics, and the ServiceAccount permission the git-rest gateway secret requires.
</objective>

<context>
Read `/workspace/README.md` fully before changing it. It currently describes the executor's role and links; the new content is a short "Reconcile loop configuration" section documenting the env vars added by this feature.

No code files are touched by this prompt. The env vars were declared on the `application` struct in the first prompt and consumed by the reconcile loop in the second prompt — this prompt documents the settled contract.

Deployment context (for accuracy, NOT for editing — the chart is outside this repo): the executor is deployed via the `agent` Helm chart, which wires executor env vars (see `JOB_KAFKA_CLIENT_CERT_SECRET` precedent in the chart's executor deployment template). The chart/config-repo values and the `secrets: get` Role grant for the executor ServiceAccount are operator-side work that belongs on the spec's verification ladder — do NOT attempt them here.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/documentation-guide.md` — docs conventions.
</context>

<requirements>
1. In `/workspace/README.md`, after the introductory paragraph (before the `## Links` section), add a `## Reconcile loop configuration` section documenting the three env vars in a table, using the existing plain-markdown style of the file. The section MUST contain, as substrings, each env var name (`GITREST_URL`, `GITREST_GATEWAY_SECRET`, `TASK_GLOB`) and the phrase `gateway-secret`:
   ```
   ## Reconcile loop configuration

   The reconcile loop re-derives running tasks from the vault (via git-rest) and
   live Jobs, so a task deferred behind `maxConcurrentJobs` whose in-memory
   deferral was lost across an executor restart resumes without a Kafka event or
   vault edit. It is configured by:

   | Env var | Default | Meaning |
   |---|---|---|
   | `GITREST_URL` | `http://vault-obsidian-openclaw:9090` | git-rest HTTP API base URL the reconcile loop reads vault task files through. |
   | `GITREST_GATEWAY_SECRET` | empty (auth disabled) | NAME of the K8s Secret carrying the git-rest gateway secret (data key `gateway-secret`) — never the secret value. The executor reads the value from the Secret at startup and uses it only in memory. The executor's ServiceAccount must be able to `get` this Secret (chart-side Role grant). |
   | `TASK_GLOB` | `24 Tasks/*.md` | git-rest single-level glob selecting the vault task files the reconcile loop evaluates. |
   ```
   The table text MUST contain all three env var names and the `gateway-secret` data-key phrase (the acceptance criterion greps for them), and must state that `GITREST_GATEWAY_SECRET` is a secret NAME (not the value) and that the ServiceAccount needs read access.

2. Do NOT change any other content of `README.md` (the intro, the `## Links` section, or the `## License` section stay as-is). No Go, YAML, Makefile, or chart changes in this prompt.
</requirements>

<constraints>
- This is a documentation-only prompt — no Go, YAML, or Makefile changes.
- The README must state the semantics exactly as implemented: `GITREST_GATEWAY_SECRET` is a K8s Secret NAME whose `gateway-secret` data key holds the value; the executor reads it at startup; the ServiceAccount needs `get` on that Secret. Do not document a value-in-env behavior.
- Do NOT edit the Helm chart or config-repo values — that deploy surface lives outside this repo and is covered by the spec's operator verification ladder (bump `EXECUTOR_VERSION`, chart env wiring, RBAC grant).
- Do NOT commit — dark-factory handles git.
- There are no code changes, so `make precommit` is not required for this prompt.
</constraints>

<verification>
- `grep -c 'GITREST_URL' README.md` — outputs ≥1.
- `grep -c 'GITREST_GATEWAY_SECRET' README.md` — outputs ≥1.
- `grep -c 'TASK_GLOB' README.md` — outputs ≥1.
- `grep -c 'gateway-secret' README.md` — outputs ≥1.
- `grep -n 'ServiceAccount' README.md` — the RBAC-read note is present.

Do NOT run `docker`, `make build`, `kubectl`, `dark-factory`, `gh`, or `git` commands in this prompt — the daemon does not check their exit codes, so keep verification to filesystem greps.
</verification>
