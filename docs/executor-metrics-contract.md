# Executor metrics contract: `executor_config_accepted`

This document records the right-hand half of the cross-repo `unreachable-phase`
join. It is the executor-side counterpart of the controller's
`agent_controller_open_tasks`, and it is a contract: the controller's merged
PromQL reads these label names verbatim, so renaming one here silently breaks
the alert on the other side.

## The join

The controller publishes `agent_controller_open_tasks` with the labels `phase`
and `task_type`. The merged rule subtracts the executor side from it:

```promql
agent_controller_open_tasks
  unless on (phase, task_type)
executor_config_accepted
```

`executor_config_accepted` is the **right** operand. `unless` keeps every
left-hand record that has no matching right-hand series, so the right side is
the "someone can actually run this" allow-list. That direction is the whole
point: an open task whose `(phase, task_type)` no Config accepts is
unreachable, and the alert fires on it.

The failure mode the metric exists to prevent is an **empty right side**.
Under `unless`, an empty right operand matches nothing and therefore returns
**every** open record — the alert reports the entire controller backlog as
unreachable. A refresh must never manufacture that emptiness, which is why the
rules below are invariants rather than preferences.

## Shape

| Property | Value |
|---|---|
| Name | `executor_config_accepted` |
| Type | gauge (no `_total` suffix) |
| Unit | one series per accepted `(phase, task_type)` pair, value `1` |
| Labels | `phase`, `task_type`, `vault` — in that declaration order |

The three label names are **frozen by the merged rule**. The controller side
joins on `phase` and `task_type`; `vault` is carried for identification only
(see below). Changing any of the three names requires the controller rule to
change in the same release.

## The accepted set

The published set is the union, over every Config in the executor's namespace,
of:

```text
effectiveTriggerPhases(Config) × EffectiveTaskTypes(Config)
```

`effectiveTriggerPhases` falls back to `planning`, `execution`, `ai_review`
when the Config's `trigger.phases` is absent or empty. `EffectiveTaskTypes`
unions the deprecated `taskType` scalar with the `taskTypes` list. A Config
with no task types contributes nothing.

The set is **namespace-wide, not vault-scoped**. Every executor in the
namespace reads the same Configs and therefore publishes the same pairs; only
the `vault` label differs between instances.

## What `vault` means — and does not mean

`vault` names the **publishing instance**: its task vault, i.e. the `TASK_VAULT`
value falling back to `VAULT_NAME`. It does not scope the pair set. The pair set
above is the union over all Configs in the namespace regardless of which vault
the executor serves, so two executors with different vaults publish identical
`(phase, task_type)` tuples under different `vault` values.

Consequently, a `{vault="…"}` selector is **not sound** until the union itself
is scoped by vault. This metric does not do that, and the merged alert does
not use a `vault` selector today. Do not add one on the assumption that it
narrows the set to one instance's Configs.

## Never spuriously empty

- **Set then delete, never `Reset()`.** A refresh sets the pairs the Configs
  accept and deletes the ones they no longer do, one tuple at a time with
  `DeleteLabelValues`. A reset-then-set refresh would leave a scrape window in
  which the family is empty, and under `unless` that window returns every open
  record.
- **A read error is a non-event.** A failed Config read — including a cancelled
  pass — publishes nothing and deletes nothing. The previous accepted set
  survives a transient API error.
- **A scrape may see a superset for one interval, never a subset.** Sets are
  published before deletes under one lock, so an interrupted refresh can only
  leave a pair that should have been removed, never remove one that is still
  accepted.
- **A genuinely empty set is truthful.** When every Config is gone the family
  may empty. That is a real "no Config accepts anything" reading, not a
  refresh failure, and it is the one legitimate empty.

## Refresh counter

`executor_config_accepted_refresh_total{result}` counts refreshes with a closed
result set:

| Result | Increment unit |
|---|---|
| `ok` | once per refresh that read the Configs and published the set |
| `read_error` | once per refresh whose Config read failed or whose context was cancelled |

Both series are seeded at `0` in `init()`. The result vocabulary is closed at
compile time, which is what makes that seeding honest: `rate(...) == 0` can
fire on a refresh that never ran instead of skipping silently on an absent
series. The counter carries only `result`; it never labels the pairs.

## Cardinality

Cardinality is bounded by Configs × trigger phases × task types. No task id,
assignee, Config name or namespace is ever a label value. The `vault` label is
the one per-instance dimension, and its cardinality is the number of executor
deployments, not the number of tasks.
