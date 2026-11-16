# Executor metrics contract

Two contracts live here: `executor_config_accepted`, the right-hand half of the
cross-repo `unreachable-phase` join, and `executor_spawn_claim_total`, the
spawn-claim decision. Each section is independent.

## `executor_config_accepted`

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
  refresh failure.
- ⚠️ **The second empty is not truthful, and it is bounded by one reconcile
  interval.** `k8s.NewEventHandler` backs the provider with an in-memory map
  that starts empty, and `Get` returns that map with a **nil error** rather
  than blocking on `HasSynced`. A pod's first pass can therefore run before
  the Config informer has populated the store, read an empty list as a
  successful read, and publish nothing. The family is empty for up to one
  reconcile interval (60 s default) after a pod starts, and the refresh counts
  it as `result="ok"` — correctly, because nothing failed. Two things keep
  this from firing the alert: a restarting pod's registry is fresh, so there
  are no prior series for the window to blank, and the rule's `for: 1h`
  exceeds one reconcile interval by an order of magnitude. ⚠️ **That makes
  `for:` load-bearing, not cosmetic:** shortening it below the reconcile
  interval would turn every rollout into a false all-records page. The
  alternative repair — waiting for informer sync before the first refresh, or
  driving the refresh from the informer's own events — is deliberately not
  taken here; it would put the metric's liveness behind a second readiness
  condition for a window the alert already absorbs.

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

## `executor_spawn_claim_total`

`executor_claim_total` records what the **lease manager** did — every request it
made, by result. It cannot say what a given **spawn** decided, and the two are
not the same question: a claim that was never acquired and a claim that was
acquired and then not renewed both leave the same aggregate trace once the
window has closed and the log has rotated.

This counter answers the spawn question. It is the only place a fail-open spawn
is visible at all, because `claimFailureDecision` returns `true` in shadow mode
and the spawn proceeds with no claim and no other trace.

### Shape

| Property | Value |
|---|---|
| Name | `executor_spawn_claim_total` |
| Type | counter |
| Labels | `result`, `stage` — in that declaration order |

Cluster is **not** a label here. It arrives as the Prometheus `cluster`
external label, which reaches this metric the same way it reaches
`executor_claim_total`. Adding a series-level copy would duplicate the
dimension.

### The closed result set

| Result | Meaning | Written by |
|---|---|---|
| `acquired` | a claim is held and the spawn proceeds | `logClaimAcquired`, both the fresh-claim and the takeover call sites |
| `unclaimed` | no claim was obtained and the spawn is blocked | `claimFailureDecision` under claim mode `on` |
| `fail_open` | no claim was obtained and the spawn proceeds anyway | `claimFailureDecision` under claim mode `shadow` |
| `mode_off` | the spawn proceeds without attempting a claim at all | `acquireSpawnClaim`'s mode-off early return |
| `lapsed` | a held claim was lost or its renewal failed, after the spawn | the renewal tick and `claimLost` |

`acquired + unclaimed + fail_open + mode_off` equals the number of spawn
attempts. `lapsed` is orthogonal — it happens after the spawn — and is the
"acquired and then not renewed" class.

`unclaimed` and `fail_open` are the same failure decision under different
modes. Splitting them is the point of the metric: a single "claim failed"
result would leave a fail-open spawn indistinguishable from one that held a
claim.

### The closed stage set

`none`, `lease_manager`, `claim_scheme`, `derive_key`, `takeover`, `claim`,
`renew`, `lost`.

The first six mirror the stages `claimFailureDecision` already logs, so the
metric and the log line name the same thing. `renew` and `lost` are the
post-spawn stages of the `lapsed` result: `renew` is a failed renewal that may
still be retried, `lost` is a lease proven gone.

### Seeding and the reachable pairs

`SpawnClaimPairs` in `pkg/metrics/metrics.go` is the closed, reachable
`(result, stage)` enumeration, and `init()` seeds every pair at `0`. An absent
series therefore means the metric stopped being exported, not "no claims yet".

It is deliberately **not** the full cross product. `acquired` and `mode_off`
can only carry `none`; `lapsed` can only carry `renew` or `lost`. Seeding the
unreachable combinations would publish permanently-zero series that no branch
can ever move, which reads as evidence a branch exists when it does not.

### Cardinality

Fourteen pairs, fixed at compile time. No task id, claim key, assignee, repo or
cluster is ever a label value — the same rule `executor_claim_total` carries.
