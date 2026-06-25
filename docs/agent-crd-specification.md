# Config CRD Specification

Config (`agent.benjamin-borbe.de/v1`) is a Kubernetes Custom Resource Definition that declares an agent type. Both the controller and job creator read these to know which agents exist and how to handle them.

The agent is **runtime-agnostic** — `spec.image` can point to any container: Claude Code CLI (agent-claude, agent-trade-analysis), Gemini API (agent-backtest), static rule validators, shell scripts, other AI providers, etc. The framework only defines the contract (receive `TASK_CONTENT`, print result JSON to stdout).

## CRD Definition

```yaml
apiVersion: agent.benjamin-borbe.de/v1
kind: Config
metadata:
  name: backtest-agent
  namespace: dev
spec:
  assignee: backtest-agent        # matches task assignee field
  taskType: backtest              # task_type value (deprecated: prefer taskTypes list)
  image: backtest-agent:latest    # container image for K8s Job
  heartbeat: 15m                  # re-spawn interval for in_progress tasks
  resources:
    cpu: 500m
    memory: 512Mi
    ephemeral-storage: 1Gi
  env:                            # per-agent env vars (merged with shared)
    LOG_LEVEL: info
  secretName: agent-backtest      # K8s Secret mounted via envFrom
```

## Who Uses the CRD

| Component | Uses | For |
|-----------|------|-----|
| Controller | `spec.assignee`, `spec.heartbeat`, `spec.taskType`, `spec.taskTypes` | Match tasks, enforce heartbeat |
| Job Creator | `spec.image`, `spec.resources`, `spec.env`, `spec.secretName`, `spec.volumeClaim`, `spec.volumeMountPath` | Spawn K8s Job with correct image/limits/env/secret/volume |

## Fields

| Field | Required | Description |
|-------|----------|-------------|
| `spec.assignee` | yes | Matches the `assignee` field in task frontmatter |
| `spec.image` | yes | Docker image for the K8s Job (tag appended at runtime from branch) |
| `spec.heartbeat` | yes | Interval between re-spawns for `in_progress` tasks |
| `spec.taskType` | conditional | Task type this agent handles; must match `^[a-z0-9-]+$`, max 63 chars. **Deprecated: prefer `spec.taskTypes` (list).** Stays functional indefinitely. Required unless `spec.taskTypes` is non-empty. |
| `spec.taskTypes` | no | List of task_type values this agent handles; overrides nothing, supplements `taskType`. Each element must match `^[a-z0-9-]+$`, max 63 chars. At least one of `taskType` or `taskTypes` must be non-empty. Filtering on this list is enforced by a follow-up spec. |
| `spec.resources` | no | CPU/memory/storage requests for the job pod |
| `spec.env` | no | Per-agent environment variables, merged with shared vars (`TASK_CONTENT`, `TASK_ID`, `KAFKA_BROKERS`, `BRANCH`) |
| `spec.secretName` | no | Name of an existing K8s Secret mounted on the container via `envFrom` |
| `spec.volumeClaim` | no | Name of an existing PVC mounted into the container |
| `spec.volumeMountPath` | conditional | Container path for `volumeClaim` mount — required when `volumeClaim` is set |
| `spec.priorityClassName` | no | — | Kubernetes PriorityClass name to stamp onto spawned Job PodTemplates. When set, a matching `ResourceQuota` scoped to this class enforces the concurrent pod cap. Absent means no PriorityClass (unbounded concurrency, pre-spec-013 behavior). |
| `spec.trigger` | no | Per-agent trigger conditions (optional nested object with `phases` and `statuses` lists). When absent or empty, defaults apply: phases `[planning, in_progress, ai_review]` and statuses `[in_progress]`. |
| `spec.trigger.phases` | no | Task phases that allow spawning. Valid values: `todo`, `planning`, `in_progress`, `ai_review`, `human_review`, `done`. Empty or absent means default phases apply. |
| `spec.trigger.statuses` | no | Task statuses that allow spawning. Valid values: `todo`, `in_progress`, `backlog`, `completed`, `hold`, `aborted`. Empty or absent means default statuses apply. |

If `spec.trigger` is omitted, or if `trigger.phases` / `trigger.statuses` are absent or empty, the executor applies its built-in defaults (phases: `planning`, `in_progress`, `ai_review`; statuses: `in_progress`). There is no way to configure a Config that triggers on nothing — an empty list is equivalent to the field being absent.

## Properties

**Declarative** — apply a Config CRD, system picks it up. Remove it, system stops watching.

**Cheap** — 100 Config CRs cost zero resources until a matching task exists.

**Independent** — adding an agent never requires controller or job creator changes.

## Examples

```yaml
apiVersion: agent.benjamin-borbe.de/v1
kind: Config
metadata:
  name: trade-analysis-agent
spec:
  assignee: trade-analysis-agent
  image: docker.quant.benjamin-borbe.de:443/agent-trade-analysis
  heartbeat: 5m
  resources:
    cpu: 1
    memory: 1Gi
  secretName: agent-trade-analysis
  volumeClaim: agent-trade-analysis
  volumeMountPath: /home/claude/.claude
```

```yaml
apiVersion: agent.benjamin-borbe.de/v1
kind: Config
metadata:
  name: youtube-processor
spec:
  assignee: youtube-processor
  image: youtube-processor:latest
  heartbeat: 1m
  resources:
    cpu: 2
    memory: 2Gi
```

```yaml
apiVersion: agent.benjamin-borbe.de/v1
kind: Config
metadata:
  name: pr-reviewer-agent
spec:
  assignee: pr-reviewer-agent
  taskType: pr-review
  taskTypes:
    - pr-review
    - oauth-probe
  image: pr-reviewer-agent:latest
  heartbeat: 5m
```

## Future Extensions

Concurrency is now enforced K8s-natively: set `spec.priorityClassName` on a Config CR and apply a `ResourceQuota` with a `scopeSelector` matching that PriorityClass. The quota caps how many pods of that class can run simultaneously in a namespace; Jobs beyond the cap create successfully but block on pod admission until a slot frees. See `agent/claude/k8s/` for the four-file bundle (PriorityClass + per-env ResourceQuota + updated Config CR).

| Field | Purpose |
|-------|---------|
| `spec.timeout` | Max runtime before job is killed |
| `spec.retries` | Auto-retry count before human_review |
| `spec.serviceAccount` | K8s service account for job pods |
