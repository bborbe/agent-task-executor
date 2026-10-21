# Agent Task Executor

Kafka event consumer + Kubernetes **Job spawner** for the
[bborbe/agent](https://github.com/bborbe/agent) task system. It consumes
`task.CreateCommand` events, matches each task's `assignee` to a registered agent
`Config` CR (`agent.benjamin-borbe.de/v1`), and spawns one Kubernetes Job per
task/phase using that agent's image, env, resources and secrets — propagating
`BRANCH` / `TOPIC_PREFIX` to the child Job.

Image tags: an agent `Config.spec.image` that already carries a tag (a semver
pin, e.g. `…/agent-claude:v0.1.1`) is used as-is; an **untagged** image gets the
running branch appended (`…/agent-backtest` → `…/agent-backtest:dev`). Deployed
via the `agent` Helm chart; published image `docker.io/bborbe/agent-task-executor`.

## Cluster routing

A task can name the cluster that runs it with the optional `cluster:` frontmatter field. The canonical values are `nukedev`, `nukeprod` and `sun`.

- `CLUSTER`: the cluster this executor runs in. A task whose `cluster` names a different cluster is skipped, logged as `skip task <id>: foreign cluster <name>`, and counted as `agent_executor_task_events_total{result="skipped_foreign_cluster"}`. A task without `cluster:` runs as before.
- `CONSUMER_GROUP`: the Kafka consumer group (default `agent-task-executor`). Each cluster must use its own group, e.g. `agent-task-executor-sun`, so that every cluster receives every task event. Keep an existing cluster on its current group, because a new group starts from the oldest retained offset and re-reads the topic.

- `STAGE_FILTER` (default `true`): skip tasks whose `stage` frontmatter (default `prod`) differs from `BRANCH`. Set it to `false` on a cluster whose controller already scopes tasks by cluster (sun), so tasks there need no `stage` field.

## Reconcile loop configuration

The reconcile loop re-derives running tasks from the vault (via git-rest) and
live Jobs, so a task deferred behind `maxConcurrentJobs` whose in-memory
deferral was lost across an executor restart resumes without a Kafka event or
vault edit. It is configured by:

| Env var | Default | Meaning |
|---|---|---|
| `GITREST_URL` | `http://vault-obsidian-openclaw:9090` | git-rest HTTP API base URL the reconcile loop reads vault task files through. |
| `GITREST_GATEWAY_SECRET` | empty (auth disabled) | NAME of the K8s Secret carrying the git-rest gateway secret (data key `gateway-secret`) — never the secret value. The executor reads the value from the Secret at startup and uses it only in memory. The executor's ServiceAccount must be able to `get` this Secret (chart-side Role grant). |
| `TASK_GLOB` | empty (required) | git-rest single-level glob selecting the vault task files the reconcile loop evaluates. Required: the executor refuses to start when it is empty, because git-rest treats an empty glob as match-everything over the whole vault. |

## Task frontmatter fields written by the executor

These keys are owned by the executor. An agent's own result write-back must never
emit them: the executor is the only writer, and a value arriving from a pod is a
clobber, not an update.

| Field | Written when | Format | Meaning |
|---|---|---|---|
| `queued_at` | The task is first deferred behind an agent's `maxConcurrentJobs` cap. | RFC3339, `2006-01-02T15:04:05Z07:00` | When the task started waiting. A re-deferral on a later reconcile cycle keeps the first value, so the field marks the queue entry and not the last retry. |

`queued_at` is stamped from the executor's injected clock through the same
`UpdateFrontmatterCommand` CQRS path `PublishSpawnNotification` uses, and only
when a vault read finds no existing value — the event payload alone does not
decide, because a deferred entry holds the pre-write copy of the task. The write
is logged at `V(1)` as `event=queued_at_written task=<id> queued_at=<value>`.

The guard is a vault read, not a lock, so it is best-effort rather than atomic:
two events processed inside one write's git-rest commit latency can each stamp,
and the value then advances by a few seconds right after the first write. It does
not move on later reconcile cycles.

Paired with `job_started_at` it yields a queue age, `job_started_at - queued_at`.
`job_started_at` is **not** executor-owned in practice: the agent pod rewrites the
frontmatter from its start-of-run snapshot, so that key reverts on write-back
while `queued_at` survives.

## Links

Dev:
https://dev.quant.benjamin-borbe.de/admin/agent-task-executor/setloglevel/3
https://dev.quant.benjamin-borbe.de/admin/agent-task-executor/agents
https://dev.quant.benjamin-borbe.de/admin/agent-task-executor/healthcheck-trigger

Prod:
https://prod.quant.benjamin-borbe.de/admin/agent-task-executor/setloglevel/3
https://prod.quant.benjamin-borbe.de/admin/agent-task-executor/agents
https://prod.quant.benjamin-borbe.de/admin/agent-task-executor/healthcheck-trigger

## License

This project is licensed under the BSD-style license. See the [LICENSE](LICENSE) file for details.
