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

- `CLUSTER`: the cluster this executor runs in. A task whose `cluster` names a different cluster is skipped, logged as `skip task <id>: foreign cluster <name>`, and counted as `agent_executor_task_events_total{result="skipped_foreign_cluster"}`. A task without `cluster:` runs as before. Every filter skip — including a foreign-cluster event — drops any respawn still pending for that task from an earlier deferral.
- `CONSUMER_GROUP`: the Kafka consumer group (default `agent-task-executor`). Each cluster must use its own group, e.g. `agent-task-executor-sun`, so that every cluster receives every task event. Keep an existing cluster on its current group, because a new group starts from the oldest retained offset and re-reads the topic.

- `STAGE_FILTER` (default `true`): skip tasks whose `stage` frontmatter (default `prod`) differs from `BRANCH`. A task whose `cluster` names this executor while its `stage` sits outside this executor's pool can never run here: it is logged as `event=pin_outside_pool` and counted as `agent_executor_task_events_total{result="pin_outside_pool"}` instead of `skipped_stage`, so a dead pin surfaces instead of stalling invisibly. A task with no `cluster` is a plain stage skip and keeps counting as `skipped_stage`. Keep this `true` on every pool member, sun included — a cluster that sets it to `false` runs tasks of every stage rather than filtering them.

## Reconcile loop configuration

The reconcile loop re-derives running tasks from the in-memory task view and
live Jobs, so a task deferred behind `maxConcurrentJobs` whose in-memory
deferral was lost across an executor restart resumes without a Kafka event or
vault edit. The view is rebuilt at startup from
`{TOPIC_PREFIX}-agent-task-v1-event` and kept current from the same topic; the
executor no longer reads git-rest.

The view becomes ready once every partition has replayed to the high-water mark
captured at startup. A reader failure exits the process, so the pod restart
rebuilds the view.

| Env var | Default | Meaning |
|---|---|---|
| `TASK_VAULT` | `VAULT_NAME` | Vault slug this executor owns tasks for. Must match `^[a-z][a-z0-9-]*$`. A task is owned when its stamped `vault` equals this value; a task with no stamped vault is never owned. A ready view that holds tasks but none for this vault logs `event=task_vault_mismatch` and drives nothing; a view in which every task is unstamped logs `event=task_vault_unstamped` and drives nothing. |
| `RECONCILE_INTERVAL` | `60s` | Interval between reconcile passes. |
| `RECONCILE_PASS_TIMEOUT` | `45s` | Deadline for one reconcile pass. |
| `CLAIM_MODE` | `off` | Claim dispatch mode: `off` (no claim client constructed and no secret read, today's behaviour), `shadow` (claims, never blocks), `on` (a held claim is a precondition of the Job). Any other value stops the process at startup. |
| `GITREST_URL` | — | git-rest HTTP API base URL the claim ref client writes leased claim refs through. Required when `CLAIM_MODE` is `shadow` or `on` (startup fails without it); unused in `off`. This is the claim transport only — task state is read from the in-memory task view, never from git-rest. |
| `GITREST_GATEWAY_SECRET` | — | **NAME** of the K8s Secret carrying the git-rest gateway secret (data key `gateway-secret`), never the secret value. Read only when `CLAIM_MODE` is `shadow` or `on`; empty disables gateway auth. |

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
