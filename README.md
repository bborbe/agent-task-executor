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

## Links

Dev:
https://dev.quant.benjamin-borbe.de/admin/agent-task-executor/setloglevel/3
https://dev.quant.benjamin-borbe.de/admin/agent-task-executor/agents
https://dev.quant.benjamin-borbe.de/admin/agent-task-executor/healthcheck-trigger

Prod:
https://prod.quant.benjamin-borbe.de/admin/agent-task-executor/setloglevel/3
https://prod.quant.benjamin-borbe.de/admin/agent-task-executor/agents
https://prod.quant.benjamin-borbe.de/admin/agent-task-executor/healthcheck-trigger
