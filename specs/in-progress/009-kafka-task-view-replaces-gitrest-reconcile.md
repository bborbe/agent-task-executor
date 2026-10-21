---
status: approved
tags:
    - dark-factory
    - spec
approved: "2026-10-21T18:29:44Z"
branch: dark-factory/kafka-task-view-replaces-gitrest-reconcile
---

## Summary

- The reconcile floor stops reading the vault over git-rest. It reads an in-memory task view instead, rebuilt on every start from the compacted Kafka task event topic and kept current from the same topic.
- A pass over ~15,000 tasks drops from 130-150 s of HTTP GETs, which abort at the 45 s pass timeout on OpenClaw, to an in-memory scan of well under one second.
- Task ownership comes from the vault name the controller now stamps on every task event, compared against a new `TASK_VAULT` setting (defaults to `VAULT_NAME`). The path-listing machinery goes away.
- A misconfigured `TASK_VAULT` fails loudly: if the view holds stamped tasks but none carry this executor's vault, the executor logs a mismatch, increments a metric and skips the pass. It does not idle silently.
- All git-rest configuration and code is deleted. The executor then has no read path into git, which matches the doctrine that task/controller owns git as the single writer and the other components read via Kafka (CQRS).

## Problem

The reconcile pass is the source-of-truth floor that re-drives tasks whose Kafka event was missed or whose deferral was lost in a restart. Today it lists the vault's task glob over git-rest and GETs every file. On the OpenClaw vault that means ~15,300 files at ~7 ms each, 130-150 s per pass. The default 45 s pass timeout aborts every pass with `evaluated=0`, so on that deployment the floor never runs and orphaned tasks stay stuck. Raising the timeout (v0.23.0) only moves the cost into 15k GETs per minute against git-rest. The executor also reads git directly, which breaks the fleet doctrine: task/controller owns git as the single writer, and every other component reads task state through Kafka. Ownership has the same problem. It is derived from which paths a completed git-rest pass listed, so a vault too large for one pass freezes ownership, and newly filed tasks get skipped as foreign.

## Goal

The executor keeps an in-memory map from task identifier to the latest task, built from the compacted task event topic `{TOPIC_PREFIX}-agent-task-v1-event`. Every reconcile pass reads only that map. A pass costs memory-speed work proportional to the number of tasks, and it never touches git-rest or the network for task content. Ownership is a plain comparison between a task's stamped vault and the executor's `TASK_VAULT`, and it applies the same way on the live event path and on the reconcile path. When the executor sees tasks but none are its own, it says so in the log and in a metric on every pass. No git-rest configuration, client, secret reader or path cache remains in the codebase.

## Non-goals

- Do NOT change the Helm chart or deployment manifests (they live in `bborbe/agent`). Leftover `GITREST_*` / `TASK_GLOB` env in the chart is ignored, and the chart cleanup plus setting `TASK_VAULT=personal` on the personal executor is a separate change.
- Do NOT add claims, leases or any cross-instance coordination for task ownership.
- Do NOT add reconnect-without-exit for the view's Kafka reader. A reader failure ends the process and the pod restart rebuilds the view.
- Do NOT change the live consumer-group consumer that spawns on events: same group, same offsets, same handler.
- Do NOT change the `/readiness` or `/healthz` HTTP probes to reflect view readiness.
- Do NOT change Config CR resolution. It keeps composing `{assignee}-{VAULT_NAME}`.
- Do NOT backfill or re-publish tasks whose latest event carries no vault. That is a controller-side concern.
- Do NOT add an opt-out flag that keeps the git-rest reader. The Kafka view is the only reconcile source. If a future consumer needs a second source, that is a separate spec.
- Do NOT make the mismatch check configurable: no threshold and no disable flag.

## Acceptance Criteria

Container-executable criteria are observed by `make test` / `make precommit` exiting 0 with the named spec in the passing list. "Test" below means a Ginkgo spec in this repo.

- [ ] **AC1 — git-rest is gone from live code.** Evidence (negative): `git grep -nE 'gitrestclient|GITREST_URL|TASK_GLOB' -- ':!CHANGELOG.md' ':!prompts' ':!specs'` returns 0 lines. Also, `git grep -nE 'GITREST_GATEWAY_SECRET|ErrTaskGlobEmpty|CreateGitRestClient|ReadGitRestGatewaySecret|prioritizeUncachedPaths|pathIDs|TaskGlob' -- ':!CHANGELOG.md' ':!prompts' ':!specs'` returns 0 lines, and `for p in pkg/gitrestclient pkg/gitrest_gateway_secret.go mocks/git_rest_client.go; do test ! -e "$p" || echo "$p"; done` fails for all three paths.
- [ ] **AC2 — Dependency carries the vault field.** Evidence: `grep -n 'github.com/bborbe/agent v0.113.0' go.mod` returns exactly 1 line.
- [ ] **AC3 — The view replays the topic correctly.** A test feeds the view, on at least two partitions, a create, an update of the same identifier, a create of a second identifier and a tombstone for the second identifier, all below the high-water mark captured at start. The view then holds exactly one task, the updated version of the first identifier, and reports not-ready until the last pre-high-water-mark message on every partition has been applied and ready afterwards. A partition with zero messages does not hold readiness back. A message delivered after ready is applied to the view. Evidence: test passing in `make test` output. Additionally: after ready, one message delivered **on each partition** is applied to the view; `event=task_view_ready` is logged exactly once; a message whose body fails to decode removes that identifier's entry and is counted, never kept stale.
- [ ] **AC4 — A pass before the view is ready does nothing and is not counted as success.** In a test, `ReconcileOnce` against a not-yet-ready view makes 0 spawner calls, emits log line `event=reconcile_view_not_ready task_vault=<x>`, increments `executor_reconcile_passes_total{result="view_not_ready"}` by exactly 1 and leaves `{result="ok"}` unchanged. Evidence: test passing.
- [ ] **AC5 — A task whose live event was missed is re-driven by the next pass.** In a test, the view holds an owned task at a trigger phase/status with no live Job, no event is delivered to the live handler, and one `ReconcileOnce` produces exactly 1 spawner call for that task identifier. The pass logs `event=reconcile_redrive task=<id>`, and `executor_reconcile_redriven_total` increments by 1. Evidence: test passing.
- [ ] **AC6 — Ownership is `Task.Vault == TASK_VAULT` on both paths.** In a test with `TASK_VAULT=openclaw`, three otherwise identical eligible tasks with `Vault` values `openclaw`, `personal` and `""` result in exactly 1 spawn (the `openclaw` task) on the reconcile path. Delivering the same three as live events also results in exactly 1 spawn, and `agent_executor_task_events_total{result="skipped_foreign_vault"}` increments by exactly 2. Evidence: test passing.
- [ ] **AC7 — `TASK_VAULT` mismatch fails loudly.** In a test with `TASK_VAULT=openclaw` and a ready view holding tasks stamped `trading`, `personal` and `trading`, plus one with `Vault=""`, `ReconcileOnce` emits `event=task_vault_mismatch task_vault=openclaw seen=personal,trading`, increments `executor_reconcile_passes_total{result="task_vault_mismatch"}` by exactly 1, makes 0 spawner calls and leaves `{result="ok"}` unchanged. Negative companion: adding one `openclaw` task to the same view produces no `task_vault_mismatch` line, and the pass increments `{result="ok"}`. Evidence: both tests passing.
- [ ] **AC7a — An all-unstamped view fails loudly.** A test with a ready view of 3 tasks, all `Vault=""`, makes `ReconcileOnce` emit `event=task_vault_unstamped tasks=3`, increment `{result="task_vault_unstamped"}` by exactly 1, make 0 spawner calls, and leave `{result="ok"}` unchanged. Evidence: test passing.
- [ ] **AC8 — `TASK_VAULT` defaults to `VAULT_NAME` and is independent of it.** A test in `main_internal_test.go` covers three cases. With `TASK_VAULT` unset and `VAULT_NAME=openclaw`, the effective task vault is `openclaw`. With `TASK_VAULT=personal` and `VAULT_NAME=shared`, the effective task vault is `personal` and the Config resolver still composes with `shared`. With `TASK_VAULT=Bad_Vault`, `Run` returns an error whose message contains `task-vault`. Evidence: test passing.
- [ ] **AC9 — Startup no longer needs git-rest.** A test shows that `application.Run`'s validation succeeds with no `TASK_GLOB` and no git-rest settings, and that no K8s Secret read happens at startup (the fake clientset records 0 `get secrets` actions). Evidence: test passing.
- [ ] **AC10 — `queued_at` comes from the view.** In a test, the view holds task T with frontmatter `queued_at: 2026-10-21T10:00:00Z`, and a live event for T without `queued_at` is deferred at the concurrency cap. The result: 0 `PublishQueueEntry` calls, and the deferred entry carries `queued_at=2026-10-21T10:00:00Z`. When the view entry has no `queued_at`, exactly 1 `PublishQueueEntry` call happens (pre-spec behaviour). Evidence: test passing.
- [ ] **AC11 — A pass over 15,000 tasks finishes in under 1 s.** A test builds a ready view of 15,000 tasks (at least 100 of them owned and eligible, the rest foreign or ineligible) with fake spawner and job lookups, runs `ReconcileOnce` and asserts that it returns in < 1 s wall-clock, logs `event=reconcile evaluated=<n>` where n equals the number of owned eligible tasks, makes a spawner call count equal to that number (or the concurrency cap, if lower), and logs no `event=reconcile_aborted`. Evidence: test passing, with the measured duration printed in the test output.
- [ ] **AC12 — The reconcile summary line keeps its fields and swaps `glob=` for `task_vault=`.** Evidence: `git grep -n 'event=reconcile evaluated=%d re_driven=%d deferred=%d skipped=%d capped=%d task_vault=%q' -- pkg` returns 1 line, and (negative) `git grep -n 'glob=' -- pkg main.go` returns 0 lines.
- [ ] **AC13 — Reconcile outcome metric labels match the new outcomes.** A test in `pkg/metrics` asserts that after init `executor_reconcile_passes_total` has series `ok`, `aborted`, `view_not_ready`, `task_vault_mismatch` and `task_vault_unstamped`, all at 0, and none for `vault_unavailable` or `list_failed`. Evidence: test passing.
- [ ] **AC14 — README documents the new contract.** Evidence: `grep -n 'TASK_VAULT' README.md` returns ≥1 line in the env table naming the `VAULT_NAME` default, and (negative) `grep -nE 'GITREST_|TASK_GLOB' README.md` returns 0 lines.
- [ ] **AC15 — `make precommit` exits 0** in the worktree root. Evidence: exit code 0.
- [ ] **AC17 — No log line still carries a file path.** Evidence (negative): `git grep -nE 'event=(reconcile_redrive|redrive_capped|reconcile_skip)[^"]*path=' -- pkg` returns 0 lines.
- [ ] **Post-Deploy (Rung-2):** (AC16) on nuke dev the view becomes ready and passes complete. Evidence: `kubectlnukedev -n dev logs deploy/agent-task-executor --since=15m | grep -E 'event=task_view_ready|event=reconcile evaluated='` returns ≥1 `task_view_ready` line and ≥2 `event=reconcile evaluated=` lines carrying `task_vault=`. The same window's `grep -cE 'event=reconcile_aborted|event=task_vault_mismatch'` returns 0.
  - `deploy_check:` `kubectlnukedev -n dev get deploy/agent-task-executor -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `v0.24.0`

**Scenario coverage: none added.** The restart-mid-publish case (an executor restarts between the controller publishing a task event and the live consumer acting on it, and the task must start within one reconcile interval) is covered by AC 3 (replay rebuilds the view) plus AC 5 (a view task with no live event is re-driven). Those two need no real broker. Condition (a) of the scenario rule fails, so no scenario is written. A future scenario candidate, only if a regression is observed: kill the executor pod right after a task's TaskUpdated lands, then assert `event=reconcile_redrive task=<id>` within `RECONCILE_INTERVAL` + replay time.

## Verification

### Container-executable

```
make test
make precommit
git grep -nE 'gitrestclient|GITREST_URL|TASK_GLOB' -- ':!CHANGELOG.md' ':!prompts' ':!specs'   # expect no output
grep -n 'github.com/bborbe/agent v0.113.0' go.mod                                           # expect 1 line
```

### Operator-executable (after merge and deploy)

- Nuke dev: AC 16 command.
- OpenClaw executor on sun (where the 45 s abort is observed today): the pod log shows `event=task_view_ready tasks=<n>` with n ≈ 15,000 and the `elapsed` value. Subsequent `event=reconcile evaluated=` lines appear once per `RECONCILE_INTERVAL`, with no `event=reconcile_aborted`.
- Personal executor in prod (`VAULT_NAME=shared`): until the chart sets `TASK_VAULT=personal`, the log shows `event=task_vault_mismatch task_vault=shared`. That is the intended loud failure, and it confirms the chart change must ship with or before this image.

## Desired Behavior

1. **Task view replay.** On start, the executor reads `{TOPIC_PREFIX}-agent-task-v1-event` from the earliest offset on every partition and decodes each message exactly as the live event path does. For each task identifier it keeps the latest task, and a tombstone removes the identifier. It captures each partition's high-water mark at start and becomes ready once every partition has been consumed up to that mark. Partitions that are empty at start count as ready immediately. At ready it logs `event=task_view_ready tasks=<n> partitions=<p> elapsed=<duration>` exactly once.
2. **Task view stays current on every partition.** After ready, the view keeps applying every new message on every partition of the topic, independent of which partitions this instance's consumer group assignment holds. Executors in one cluster share `CONSUMER_GROUP`, so the group consumer alone sees only a subset of partitions (see the measured 2026-10-14 note on vault ownership in the handler).
3. **Reconcile reads the view.** Each pass takes a point-in-time snapshot of the view's owned tasks and runs the existing gather → filter → sort-by-admission-priority → drive flow over it. The filter is the same eligibility filter as today: foreign cluster, stage, trigger status/phase from the resolved Config, the parked-assignee restore, the re-drive cap and the recent-success skip. The pass makes no network call for task content. It stays bounded by `RECONCILE_PASS_TIMEOUT` and paced by `RECONCILE_INTERVAL`.
4. **Not-ready skip.** While the view is not ready, a pass logs `event=reconcile_view_not_ready task_vault=<x>`, increments `executor_reconcile_passes_total{result="view_not_ready"}` and drives nothing. A not-ready pass never counts as `ok`.
5. **Ownership by stamped vault.** A new setting `TASK_VAULT` (env `TASK_VAULT`, arg `task-vault`) defaults to `VAULT_NAME` when empty. It must match `^[a-z][a-z0-9-]*$`, otherwise startup fails. The executor owns a task when and only when the task's `Vault` equals `TASK_VAULT`, and a task with an empty `Vault` is not owned. The same rule gates the live event path and the reconcile path, and it replaces the path-derived ownership set and its fail-open window. A foreign task on the event path increments `agent_executor_task_events_total{result="skipped_foreign_vault"}` and logs `skip task <id>: foreign vault vault=<v> task_vault=<x>` at V(2). `VAULT_NAME` keeps its current meaning for Config CR resolution only.
6. **Fail loud on vault mismatch.** When the view is ready, holds ≥1 task with a non-empty `Vault`, and 0 tasks with `Vault == TASK_VAULT`, the pass logs `event=task_vault_mismatch task_vault=<x> seen=<distinct non-empty vaults, sorted, comma-joined>`, increments `executor_reconcile_passes_total{result="task_vault_mismatch"}` and drives nothing. When the view is ready, holds ≥1 task, and **every** task has an empty `Vault`, the pass instead logs `event=task_vault_unstamped tasks=<n>`, increments `executor_reconcile_passes_total{result="task_vault_unstamped"}`, and drives nothing. A pre-v0.113.0 controller therefore fails loudly too, rather than idling as `evaluated=0`.
7. **`queued_at` from the view.** When a deferral finds no `queued_at` on the incoming task, it takes `queued_at` from the view's entry for that identifier. Only when the view has none does it stamp and publish a new one, so the duplicate-event protection the git-rest read gave is kept.
8. **git-rest removed; reader failure is fatal.** The git-rest URL, gateway secret, task glob settings, their startup validation, the git-rest client package, the gateway-secret reader and the path→identifier cache are deleted. The `path=` field disappears from the `reconcile_redrive`, `redrive_capped` and `reconcile_skip` lines, because the view has no file path. Every other field on those lines stays. An unrecoverable error from the view's Kafka reader is returned to the process runner, so the process exits and the pod restart replays the view.

## Constraints

- Topic `{TOPIC_PREFIX}-agent-task-v1-event` (`lib.TaskV1SchemaID.EventTopic(topicPrefix)`) has `cleanup.policy=compact` and infinite retention on all clusters (operator-verified 2026-10-21). The view depends on this: a non-compacted, time-retained topic would silently drop old tasks from the view.
- Messages are cqrs EventObjects keyed by task identifier, and their body is `lib.Task` from `github.com/bborbe/agent` v0.113.0, which carries `Vault string json:"vault,omitempty"`. A tombstone (empty value) deletes. The view must decode exactly as the live event path decodes, through one shared decode routine and not a second parser.
- The live group consumer (`CreateConsumer`, `CONSUMER_GROUP`, `OffsetConsumerHighwaterMarks` from oldest) keeps its group, its offset commits and its handler semantics unchanged.
- The view's reader must not join or commit to `CONSUMER_GROUP` or any other consumer group, so it never steals partitions from the live consumer or moves its offsets. **Open implementation detail for the prompt:** read the existing consumer construction in `main.go` / `pkg/factory` and the `github.com/bborbe/kafka` v1.25.x package (partition consumer, simple consumer, high-water-mark helpers) and choose the non-group reader from offset oldest. Do not invent APIs. The recommended shape is one reader that replays and then keeps running past the high-water mark, which satisfies DB 1 and DB 2 with one component.
- `RECONCILE_INTERVAL` and `RECONCILE_PASS_TIMEOUT` keep their names, defaults (60 s / 45 s) and startup validation (> 0).
- The pass keeps its two-phase gather → sort-by-admission-priority → drive structure, the `admissionPriority` order, the re-drive cap, the recent-success skip, the per-pass job cache on the pass context, and the `reconcile_aborted` path on timeout.
- The `event=reconcile` summary line keeps `evaluated`, `re_driven`, `deferred`, `skipped` and `capped` in that order, and `glob=%q` becomes `task_vault=%q`.
- Cluster, stage-filter and run-untagged gates are unchanged on both paths.
- The deferred-respawn loop, the job watcher, the zombie sweeper and the service reconcile loop are unchanged.
- A reconcile pass never blocks the view's feed for the duration of the pass. It works on a snapshot, and the copy and locking strategy is the agent's decision at impl time.
- Memory: the view holds the full task (frontmatter and content) for owned tasks, because spawning renders the content into `TASK_CONTENT`. For non-owned tasks, keeping the full task or only its vault name is the agent's decision at impl time, as long as DB 6 still sees every distinct non-empty vault.
- All existing tests pass after their git-rest fakes are replaced by view fakes. A test asserting old behaviour that this spec intentionally removes (path-based ownership, git-rest unavailability outcomes) is deleted, not kept passing.

## Assumptions

- The chart sets TASK_VAULT=personal on the personal executor before this image rolls out to prod; otherwise that executor spawns nothing.
- The deployed task/controller publishes `lib.Task.Vault` on every task event (agent ≥ v0.113.0) before or together with this executor image.
- After compaction, the latest event per task identifier reflects that task's current vault file, because the controller is the single writer.
- On the order of 10⁴ tasks per topic replay within seconds to low minutes on every cluster. This is unverified until AC 16 and the sun operator check report `elapsed`.

## Failure Modes

| Trigger | Expected behavior | Recovery | Detection |
|---|---|---|---|
| Kafka unreachable at start (external unavailability) | Replay cannot reach the high-water mark and the view stays not-ready. Every pass is skipped with `reconcile_view_not_ready`. If the reader returns an error, the process exits and the pod restarts | Broker returns. The pod restart replays, and `event=task_view_ready` appears | `executor_reconcile_passes_total{result="view_not_ready"}` rising, and `{result="ok"}` flat |
| Kafka drops mid-run after ready (external unavailability) | The reader's error is returned and the process exits. No reconnect logic | Pod restart, then replay, then ready | Pod restart count, plus a fresh `task_view_ready` line |
| Controller older than agent v0.113.0, so events carry empty `Vault` (schema drift) | Empty-vault tasks are not owned and are never driven on either path. If every task in the view is unstamped, the pass logs `event=task_vault_unstamped` and drives nothing | Deploy the stamping controller **before** this executor: its restart republishes every task (its scanner's hash map starts empty; read from agent-task-controller `pkg/sync/sync_loop.go` and `pkg/scanner/vault_scanner.go`, not re-verified at runtime), so the compacted topic is fully re-stamped and no backfill is needed | `executor_reconcile_passes_total{result="task_vault_unstamped"}` > 0 |
| Task whose latest event predates controller stamping (schema drift, partial rollout) | Not owned until the controller publishes a new event for it | Controller-side re-publish (out of scope), or any edit to the task file | The task sits at a trigger phase with no `reconcile_redrive` line |
| `TASK_VAULT` wrong or defaulted to `shared` (personal executor) | Reconcile: `event=task_vault_mismatch task_vault=shared seen=…` on every pass, nothing driven, no `ok` count. Live path: every event is skipped as `skipped_foreign_vault`, so the executor spawns nothing until `TASK_VAULT` is set | Set `TASK_VAULT` in the chart (bborbe/agent) and roll the pod | `executor_reconcile_passes_total{result="task_vault_mismatch"}` > 0 |
| Message body fails to decode during replay or live feed | Log `event=task_view_skip partition=<p> offset=<o> err=<e>`, remove any existing view entry for that message key, and advance. This never blocks readiness | Next valid event for that identifier restores it | Warning log lines |
| Executor restarts between the controller publishing an event and the live consumer handling it (partial progress) | Replay includes the event. Once ready, the next pass re-drives the task, and the existing active-Job check prevents a duplicate if the live consumer also handles it | Automatic, within one `RECONCILE_INTERVAL` after `task_view_ready` | `event=reconcile_redrive task=<id>` |
| Executor crashes mid-pass (partial progress) | No persistent state is written by the view. On restart the view is rebuilt and the pass reruns | Automatic | Fresh `task_view_ready` |
| Broker throttles the replay reader (rate limiting) | Readiness is delayed and passes are skipped as not-ready until it completes. The live consumer is unaffected | Automatic once throughput returns | Value of `task_view_ready elapsed=` and the `view_not_ready` count |
| View memory exceeds the pod limit (resource exhaustion) | OOMKill, restart, then replay again (crash loop if the limit stays too low) | Raise the executor memory limit in the chart (out of scope) | Pod `OOMKilled` reason, plus restart count |
| Crash loop repeats full replays (resource exhaustion on broker) | Each restart re-reads the whole topic. This is bounded by Kubernetes restart backoff | Fix the root cause of the crash | Restart count, plus repeated `task_view_ready` |
| Clock skew between controller and executor | No new exposure. Admission order uses frontmatter timestamps as before, and readiness uses offsets, not clocks | n/a | n/a |

## Security / Abuse Cases

- Trust boundary is unchanged: any principal able to produce to the task event topic could already make the executor spawn a Job. A forged `Vault` value adds no new capability, because ownership was previously derived from git-rest and the event path already trusted event content.
- Attack surface shrinks: the executor no longer reads a K8s Secret at startup and no longer holds a git-rest gateway credential in memory.
- Hang: the replay target is the high-water mark captured once at start, so a continuously written topic cannot keep the view from becoming ready. The pass stays bounded by `RECONCILE_PASS_TIMEOUT`.
- Unbounded input: the view grows with the number of distinct task identifiers on the topic. Tombstones shrink it. There is no per-message size limit beyond the broker's own.

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Bump `github.com/bborbe/agent` to v0.113.0 (fix any compile drift). Add the task view component: non-group replay from oldest on all partitions, latest-wins, tombstone delete, ready at the high-water mark captured at start, keep applying after ready, `task_view_ready` / `task_view_skip` logs, reader error returned. Wire it into `service.Run` without consumers yet | 1, 2, 8 (reader-failure part) | 2, 3 | — |
| 2 | `TASK_VAULT` config plus validation. Vault ownership on the event path and the reconcile path. Reconcile reads the view snapshot. Not-ready skip, mismatch fail-loud, `queued_at` from the view, metric label set, summary line `task_vault=`, the 15k-task timing test | 3, 4, 5, 6, 7 | 4, 5, 6, 7, 7a, 8, 10, 11, 12, 13, 17 | prompt 1 |
| 3 | Delete git-rest: config fields, `ErrTaskGlobEmpty`, client package, gateway-secret reader, factory constructor, mocks, the path→identifier cache and `path=` log fields. Update README and CHANGELOG | 8 | 1, 9, 14, 15 | prompt 2 |

Rationale: prompt 1 builds the data source in isolation, so it is testable without touching reconcile. Prompt 2 switches reconcile and ownership onto it while git-rest code still compiles, so behaviour changes and deletion land separately. Prompt 3 is pure removal plus docs once nothing references git-rest. AC 16 is verified post-deploy, after all three.

## Do-Nothing Option

The floor stays dead on the OpenClaw executor on sun, where every pass aborts at 45 s with `evaluated=0`, so tasks orphaned by a restart or a missed event stay stuck until someone edits them by hand. Raising `RECONCILE_PASS_TIMEOUT` to ~180 s makes the floor run at about one pass per three minutes, at the cost of ~15k git-rest GETs per pass, and it grows linearly with the vault. Path-derived ownership keeps its fail-open window and its freeze-on-aborted-pass failure. The executor keeps a second read path into git against the single-writer / CQRS doctrine. Alternatives considered: a git-rest batch-read endpoint (needs a git-rest change and keeps the doctrine violation) and a longer timeout (does not scale). Both were rejected in favour of the Kafka view.
