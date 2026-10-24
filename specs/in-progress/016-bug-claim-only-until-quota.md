---
status: verifying
approved: "2026-10-24T19:27:54Z"
generating: "2026-10-24T19:30:21Z"
prompted: "2026-10-24T19:41:26Z"
verifying: "2026-10-24T20:14:35Z"
branch: dark-factory/bug-claim-only-until-quota
---

## Summary

- A cluster admits a task for spawn while comparing the running Job count against `maxConcurrentJobs` alone. Where that number exceeds the namespace `ResourceQuota` on pods, the cluster creates Jobs whose pods the quota refuses.
- Such a Job stays non-terminal, so the claim taken for it is held and renewed while the Job can never run. The peer cluster is refused with `lease not expired` on that task for the whole hold.
- Measured on nuke prod: `github-pr-review-agent` runs with `maxConcurrentJobs: 5` while its `ResourceQuota` allows `pods: 3`. The executor may therefore hold five claims for three spawnable slots.
- Fix: admission capacity is the smaller of the declared cap and the pod quota that scopes the agent's Jobs. A cluster claims only what it can start.
- No release-on-refused-spawn path is added: the capacity is readable at admission time, so prevention is available and is the behaviour the operator chose.

## Problem

The spawn path takes a dispatch claim immediately before creating a Job, and it decides whether to admit at all one step earlier, in `deferIfAtConcurrencyCap`. That decision compares the number of active Jobs for the assignee with `config.MaxConcurrentJobs` and nothing else. When the namespace `ResourceQuota` for that agent allows fewer pods than `maxConcurrentJobs`, the executor admits and claims for a Job the quota will not let run.

The consequences compound:

1. `SpawnJob` succeeds — Kubernetes accepts the Job object. The quota rejection happens later, when the Job controller tries to create the pod.
2. `finishSpawn` records the Job and publishes the spawn notification, so the task looks spawned.
3. The claim is not released by the spawn path: release runs on a Job terminal event, and this Job produces none. The lease loop renews it every 60 s, and the jobless pass frees it only after 300 s of continuous not-running — so the hold lasts about five minutes per over-admitted task, and every retry of that task re-opens it.
4. A peer cluster that wants the same task gets `ErrLeaseConflict` on `Claim` and `ErrLeaseNotExpired` on the one takeover attempt, for the whole hold.

The 2026-10-24 saturation run measured the result: prod pinned at `pods: 3/3` while sun never exceeded `1/8`, with sun logging **51** `claim_failed … error=lease not expired` and **1** `claim_acquired` over the same six minutes, and **6 of 10** test pull requests left unclaimed. Eleven slots existed and four were used.

`maxConcurrentJobs` is a declared intent. The pod quota is the enforced capacity. When they disagree, the executor currently believes the intent.

## Reproduction

Repo `bborbe/agent-task-executor` at `4debb9e` (`release v0.36.1`). dark-factory `v0.198.7`.

**Capacity read (nuke prod, 2026-10-24T19:2xZ):**

```
$ kubectlnukeprod -n agent get resourcequota github-pr-review-agent \
    -o jsonpath='{range .spec.scopeSelector.matchExpressions[*]}{.scopeName}={.operator}={.values}{"\n"}{end}{.status.hard.pods}'
PriorityClass=In=["github-pr-review-agent"]
3

$ kubectlnukeprod -n agent get configs.agent.benjamin-borbe.de github-pr-review-agent \
    -o jsonpath='{.spec.maxConcurrentJobs} {.spec.priorityClassName}'
5 github-pr-review-agent
```

The declared cap is 5. The enforced capacity for exactly that priority class is 3.

**RBAC read (nuke prod, same turn):** the executor's Role has no `resourcequotas` rule.

```
$ kubectlnukeprod -n agent get role agent-task-executor -o jsonpath='{range .rules[*]}{.apiGroups}{" | "}{.resources}{" | "}{.verbs}{"\n"}{end}'
["batch"] | ["jobs"] | ["create","get","list","watch","delete"]
["apps"] | ["statefulsets"] | ["create","get","update"]
[""] | ["pods"] | ["get","list","watch"]
["agent.benjamin-borbe.de"] | ["configs"] | ["get","list","watch"]
[""] | ["secrets"] | ["get"]
```

The Role is Helm-managed by the `agent` chart (`helm.sh/chart: agent-0.22.2`, release `agent-prod`), defined at `agent/helm/templates/executor-rbac.yaml`. The quota read therefore needs a chart change in `bborbe/agent` in addition to the code change here. Until that chart ships, the reader gets `Forbidden` and admission falls back to the declared cap — the fix is inert, not wrong.

**Runtime (the defect):**

1. Ten pr-review tasks become eligible on both clusters.
2. The prod executor admits up to five of them, because `running < 5`.
3. It takes a claim for each admitted task and creates the Job.
4. Three pods fit the quota. Two Jobs stay pod-less and non-terminal.
5. The two claims are renewed every 60 s and never released while the Job sits there.
6. The sun executor, with eight free slots, is refused `lease not expired` on both keys for the duration.

**Regression lock (post-fix, unit level):** a fake `JobSpawner` reports three active Jobs and a fake pod-quota limiter reports three, against a Config with `maxConcurrentJobs: 5`. Before the fix no limiter exists, so the only bound is the declared cap and the spawn is admitted; after it, the spawn must be deferred and the claim must not be taken. That difference is the lock — the spec above is the reproduction that is runnable today.

**The same gap on nuke dev, read 2026-10-24** (namespace `agent`): `github-pr-review-agent` has `maxConcurrentJobs: 5` and `ResourceQuota github-pr-review-agent` has `hard.pods=3`. So the fix is verifiable on dev without creating a fixture.

## Expected vs Actual

| | Expected | Actual (v0.36.1) |
|---|---|---|
| Active Jobs 3, declared cap 5, pod quota 3 | Defer: no claim taken, task parked for retry | Admit: claim taken, Job created, pod refused by the quota |
| Peer cluster wants the same task | Claims it, because the holder never claimed work it could not start | `lease not expired` until the hold lapses |
| Declared cap 5, pod quota 8 | Declared cap binds: 5 | 5 (unchanged) |
| Declared cap 0 (unlimited), pod quota 3 | Pod quota binds: 3 | Uncapped |

## Why this is a bug

The claim is a mutual-exclusion token over work in flight: holding it asserts that this cluster is running the task, and every other cluster must stand off. `docs/claim-lease-protocol.md` § Claim state machine defines the held state that way, and the fence and takeover rules exist only to bound how long a *running* holder may keep it.

A claim held for a Job the quota will never start asserts something false. The holder is not running the work, cannot start it, and still refuses every peer — which is exactly the state the protocol has no rule for, because the protocol assumes a claim is only taken when a Job is about to exist.

`k8s/apis/agent.benjamin-borbe.de/v1/types.go:150` documents `maxConcurrentJobs` as a cap the executor enforces; it says nothing about the quota, and the executor's own comment at `pkg/handler/task_event_handler.go:318` already records that over-cap Jobs are "rejected by the agent's ResourceQuota, loops on FailedCreate and burns its full activeDeadlineSeconds while merely queued before being killed without ever running". The code knows the quota rejects these Jobs and does not treat it as a bound.

## Workaround

Until the fix ships, set `maxConcurrentJobs: 3` on `github-pr-review-agent` in prod — the declared cap then equals the enforced capacity and no pod-less Job is created. That is an operator action on a live Config CR; Non-goal 5 constrains the *fix* from rewriting Config values, not the operator from applying this mitigation.

## Goal

A cluster never holds a claim for a task it cannot start. Admission is bounded by the enforced pod capacity of the agent's own priority class, and by the declared `maxConcurrentJobs` when that is smaller. Every task the cluster declines is left untouched — no claim, no Job, no frontmatter write beyond the existing deferral stamps — so the peer pool can take it immediately.

## Non-goals

- Do NOT change the claim lease constants (`LeaseDuration`, `RenewInterval`, `FenceMargin`, `TakeoverGrace`, `JoblessGrace`, `JoblessCheckInterval`).
- Do NOT add a release-on-refused-spawn path. The capacity is readable at admission, so prevention is available and is the chosen behaviour.
- Do NOT change claim keys, the takeover path, or the per-PR keying.
- Do NOT change any `ResourceQuota` object or any manifest in this repo. The `resourcequotas` read grant in `bborbe/agent`'s Helm chart is a separate change in that repo's own pipeline; this spec ships safely without it because a denied read falls back to the declared cap.
- Do NOT change `maxConcurrentJobs` on any live Config CR. A quota lower than the declared cap is now enforced; the Config value stays the operator's intent.
- Do NOT change the deferred-respawn loop, `queued_at`, or the admission ordering.
- Do NOT change the offset replay, the reconcile pass budget, or the Kafka view.

## Acceptance Criteria

1. [ ] **The pod quota bounds admission.** A spec drives the spawn path with a fake `JobSpawner` whose active set holds three Jobs for the assignee and a fake quota limiter reporting three, against a Config with `MaxConcurrentJobs: 5`. It asserts:
   - the spawn attempt is deferred — no spawn outcome, no error;
   - the lease manager's claim was never called;
   - `SpawnJob` was never called;
   - `TaskEventsTotal{result="deferred_concurrency_cap"}` rose by 1 and `DeferredConcurrencyCapTotal{assignee=<assignee>}` rose by 1.

   Evidence: `make test` exits 0 with the spec passing.
2. [ ] **The declared cap still binds when it is the smaller bound.** Same fixture with a pod quota of eight and `MaxConcurrentJobs: 5`: with five active Jobs the spawn is deferred; with four it is admitted and the claim is taken. Evidence: `make test` exits 0.
3. [ ] **No quota means today's behaviour.** A limiter reporting zero (no quota scoping the priority class) leaves `MaxConcurrentJobs` as the only bound: four active against five admits, five against five defers. Evidence: `make test` exits 0.
4. [ ] **Unlimited declared cap is still bounded by the quota.** `MaxConcurrentJobs: 0` with a pod quota of three defers at three active Jobs and admits at two. Evidence: `make test` exits 0.
5. [ ] **A quota read failure does not stop the pipeline.** With the limiter returning an error, admission falls back to `MaxConcurrentJobs` and the spawn proceeds when under that cap. On that path exactly one warning line is emitted; on a successful read, none. Evidence: `make test` exits 0 and the captured output of both fixtures is quoted in the completion report.
6. [ ] **The quota reader selects by priority class.** Table spec over the reader:
   - a quota whose `scopeSelector` is `PriorityClass In ["github-pr-review-agent"]` returns its `hard.pods`;
   - an unrelated quota in the same namespace is ignored;
   - a quota with no `scopeSelector` is ignored;
   - two matching quotas return the smaller limit and log one warning naming both;
   - no match returns 0 with a nil error;
   - an empty priority class returns 0 with a nil error and no API call.

   Evidence: `make test` exits 0.
7. [ ] **Quota reads are cached.** Two admission checks for the same namespace and priority class inside one 30 s window issue one `ResourceQuota` list; a check after the window issues a second. Evidence: `make test` exits 0 and the fake records exactly two list calls across the three checks.
8. [ ] **Both bounds are visible in the decision.** For the AC 1 fixture, the captured log contains one admission decision line carrying `running=3`, `cap=3`, `max=5` and `quota=3`, so an operator can tell which bound fired without reading the quota by hand. Evidence: `make test` exits 0 and the line is quoted in the completion report.
9. [ ] **Docs state the rule.** `docs/claim-lease-protocol.md` and `README.md` each contain a sentence naming both bounds, the smaller-binds rule, and the consequence that a task over the bound is never claimed. Evidence: `grep -ciE 'smaller of' docs/claim-lease-protocol.md` ≥ 1, `grep -ciE 'never claim' docs/claim-lease-protocol.md` ≥ 1, and the same two greps over `README.md` each ≥ 1.
10. [ ] **CHANGELOG.** Every line printed by `awk '/^## /{sec=$0} /pod quota/{print "sits under: " sec}' CHANGELOG.md` is `sits under: ## Unreleased`, at least one line is printed, and no line reads `sits under: ## v`. Evidence: stdout match.
11. [ ] `make precommit` exits 0 at the repo root. Evidence: exit code.
12. [ ] **Post-Deploy (Rung-2):** nuke dev already carries the pair — `github-pr-review-agent` has `maxConcurrentJobs: 5` against `ResourceQuota github-pr-review-agent` `hard.pods=3`, both read 2026-10-24. After the executor release is deployed there, a reviewer spawn decision names the quota as the binding bound and no more than three reviewer Jobs exist for that assignee. Evidence: `event=concurrency_admit … running=<n> cap=3 max=5 quota=3` with a timestamp, plus a Job count for that assignee ≤ 3.
    - `deploy_check:` `kubectlnukedev -n agent get deploy/agent-task-executor-openclaw -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
    - `deploy_target:` `$(git describe --tags --abbrev=0)` evaluated in the host repo root after the release tag is cut.

## Verification

### Container-executable

```
make test
make precommit
grep -ciE 'smaller of' docs/claim-lease-protocol.md
grep -ciE 'never claim' docs/claim-lease-protocol.md
grep -ciE 'smaller of' README.md
grep -ciE 'never claim' README.md
awk '/^## /{sec=$0} /pod quota/{print "sits under: " sec}' CHANGELOG.md
```

### Operator-executable (host, after merge)

```
# Prerequisite for the quota bound to be live at all: the chart must grant the read.
grep -c 'resourcequotas' ~/Documents/workspaces/agent/helm/templates/executor-rbac.yaml
kubectlnukeprod -n agent get resourcequota github-pr-review-agent -o jsonpath='{.status.hard.pods}'
kubectlnukeprod -n agent get configs.agent.benjamin-borbe.de github-pr-review-agent -o jsonpath='{.spec.maxConcurrentJobs}'
kubectlnukeprod -n agent logs deploy/agent-task-executor-openclaw --since=30m | grep 'event=concurrency_admit' | grep pr-reviewer
```

## Desired Behavior

1. **Effective capacity.** For a task whose Config resolves, the admission bound is the smaller positive value of `MaxConcurrentJobs` and the pod quota that scopes the agent's `priorityClassName`. A zero `MaxConcurrentJobs` means "no declared cap" and does not disable the quota bound. Zero for both means uncapped.
2. **Claim only after admission.** The existing order is preserved: admission is decided before the claim is taken, so a deferred task never holds a lease. No new release path is added.
3. **Deferral is unchanged.** A task over the bound is parked in the deferred-respawn queue exactly as today: `queued_at` stamped once, retried after 60 s, counted in `TaskEventsTotal{result="deferred_concurrency_cap"}` and `DeferredConcurrencyCapTotal{assignee}`.
4. **Quota lookup.** The limit is the `hard.pods` of the `ResourceQuota` whose `scopeSelector` matches `PriorityClass In [<priorityClassName>]`. No priority class, no match, or an empty value means no quota bound.
5. **Bounded cost.** The quota is read at most once per namespace and priority class per 30 s. A read error falls back to the declared cap, logs one warning, and never blocks a spawn on its own.
6. **Observability.** The admit and deferral lines carry `running`, `cap`, `max` and `quota` so the binding bound is identifiable from a log line alone.

## Constraints

- The quota reader is a new optional dependency, attached through the existing `HandlerOption` seam (`pkg/handler/task_event_handler_options.go`), not a new positional parameter and not a new method on `JobSpawner`. `NewTaskEventHandler` keeps its signature, the generated counterfeiter fake is untouched, and every existing test keeps its current behaviour because an absent limiter means "no quota bound".
- The reader itself lives in `pkg/spawner` and takes the kube client plus the injected clock.
- `ClaimTotal` labels stay a closed set. Keys, task ids and priority class names are never label values.
- No new metric is added; the existing `deferred_concurrency_cap` counters carry the signal.
- The per-assignee spawn lock still spans count, quota read and spawn.
- Existing tests pass with their assertions unchanged. The only permitted edits to existing test files are the additions the new dependency requires — a stub or fake gaining one zero-returning method, and a helper gaining one optional argument. No existing expectation may be relaxed to accommodate the change.

## Assumptions

- One spawned Job creates one pod at a time, so `hard.pods` is a sound bound on concurrent Jobs for an agent. A Job that retries leaves a failed pod behind until the TTL controller removes it; that makes the quota stricter than the Job count, never looser.
- A `ResourceQuota` scoping an agent is selected by `PriorityClass` and not by name. Verified on nuke prod and nuke sun: the quota is named `github-pr-review-agent` and scopes `PriorityClass In ["github-pr-review-agent"]`, while the Config's assignee is `pr-reviewer-agent` — so name matching would not work.
- `status.hard` mirrors `spec.hard` once the quota controller has processed the object; `spec.hard` is the fallback when `status.hard` is empty.
- The executor's Role has no `resourcequotas` grant today (read on nuke prod 2026-10-24). The reader is therefore written to treat `Forbidden` like any other read error — fall back to the declared cap, warn once — so shipping the executor release before the chart change is safe and changes no behaviour.
- The change is visible on deploy for agents whose declared cap is empty: nuke dev carries three (`claude-interactive`, `easy-agent-goreleaser`, `pi-interactive`), each with `hard.pods=1`, so they acquire a bound of 1 for the first time. That is the intended behaviour — the quota already enforces 1 — but it is a throughput change an operator should expect rather than diagnose.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection |
|---|---|---|---|
| Quota list call fails, times out, or is refused (403 until the chart grant ships) | Fall back to `MaxConcurrentJobs`, one warning, spawn proceeds when under that cap | The next admission re-reads; the warning names the error | warning line, and `quota=0` on the following decision line |
| Quota list call is throttled by client-go's rate limiter | The read is served from the 30 s cache when warm; when cold the admission waits for the limiter, the same shape `pkg/handler/job_cache.go:50` was added for | The cache absorbs the next window | one list call per namespace and priority class per 30 s |
| Quota deleted while Jobs run | No quota bound; declared cap applies | The first admission after the deletion logs `quota=0 cap=<max>` | `quota=0` in the decision line |
| Quota lowered below the running Job count | New spawns defer; running Jobs finish untouched | Once the in-flight Jobs reach terminal, `event=concurrency_admit` resumes with `cap=<new quota>` | `event=concurrency_cap … cap=<new>` |
| Quota raised above the declared cap | Declared cap binds | Operator raises `maxConcurrentJobs` too; the decision line then reads `cap=<max>` | `cap=<max>` in the decision line |
| `priorityClassName` empty on the Config | No quota bound; declared cap applies | none needed | `quota=0` |
| Two quotas match one priority class | The smaller limit binds, and one warning names both | Operator removes one | `cap` in the decision line plus the warning |
| Cache stale for up to 30 s after a quota change | One window of the previous bound | The first admission after the window logs the new `quota=` value | time-bounded by construction |
| Two executors admit against the same quota concurrently | Each reads the same limit and each may admit one Job past it in the overlap window. The quota, not the executor, stays authoritative: the extra pod is refused and that Job becomes the very pod-less shape this fix prevents | The claim for the refused Job is freed by the existing jobless grace | `event=concurrency_cap` and a `FailedCreate` Job condition in the same window |

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Quota reader in `pkg/spawner`: priority-class selector, `hard.pods` extraction, 30 s cache, table tests | 4, 5 | 6, 7 | — |
| 2 | Effective-capacity admission in the handler: the limiter option, `min(declared, quota)`, decision-line fields, admission tests | 1, 2, 3, 6 | 1, 2, 3, 4, 5, 8 | prompt 1 |
| 3 | Protocol doc, README, CHANGELOG | — | 9, 10, 11 | prompt 2 |

AC 12 is the operator's post-deploy rung and is deliberately carried by no prompt — it runs after the release, on the cluster, not in a container.

Rationale: prompt 1 is a self-contained read-only k8s query with its own tests; prompt 2 consumes it and owns the behaviour change; prompt 3 is prose plus the operator's rung. The `bborbe/agent` Helm grant that makes the quota read live is a separate change in that repo's own pipeline (YAML → direct edit, not a prompt) and is deliberately outside this repo's prompts.

## Do-Nothing Option

The declared cap stays the only bound, so every agent whose `maxConcurrentJobs` exceeds its pod quota claims work it cannot start. Each such claim blocks the peer pool for the length of the hold, and the pipeline caps at the smaller cluster's quota while the other sits idle. Measured on 2026-10-24: 4 of 11 slots used, 6 of 10 pull requests unclaimed. Not acceptable.
