# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

- test: **the stage × cluster routing contract is pinned.** No behaviour change: a task naming a different `cluster:` is skipped, and a task with no `cluster:` is eligible on every cluster whose `stage` matches its `BRANCH` — that is already what the executor does. It is pinned now because the claims rollout depends on it and nothing tested it. A 20-entry table covers dev / prod / absent stage × no-pin / pin-self / pin-other across dev, prod and sun executors, plus the `RUN_UNTAGGED=false` rows that keep a pool member out of the untagged set while claims are off — with claims off, an untagged task eligible on two clusters is a duplicate run, so that guard stays until the assigner is retired.
- test: **a live-broker test proves two consumer groups on one topic both receive every event.** The task event topic has one partition, so a shared group hands the whole topic to a single member; that member filters by assignee, skips what it cannot run, and commits those offsets, and the other clusters never see the events. `pkg/kafkatest` runs two groups against one single-partition topic and asserts both receive every message. Needs `KAFKA_BROKERS`; skips without one, the same way `pkg/envtest` skips without `KUBEBUILDER_ASSETS`.

## v0.26.2

- fix: `ReadGitRestGatewaySecret` now returns an error when the `gateway-secret` data key exists but holds an empty value. It previously returned an empty string and a nil error, which silently disabled gateway auth despite the doc promising a loud startup failure; only an unset secret name now means auth is disabled.

## v0.26.1

- refactor: restore `pkg.ReadGitRestGatewaySecret`, which reads the git-rest gateway secret by NAME from a K8s Secret (data key `gateway-secret`) — never logged, never in env, never in the Deployment manifest. The task-view change removed it along with the git-rest client; the leased claim ref transport needs it back.
- feat: add the `ClaimMode` type (`off` | `shadow` | `on`) and `ParseClaimMode`, which treats an unset variable as `off` and an explicitly empty or unrecognised value as an error rather than defaulting. `ClaimMode.TakesClaim` reports whether a mode needs the claim transport; it is an allow-list, so a zero-value or unparsed mode reports false and fails closed. No runtime wiring yet.

## v0.26.0

- feat: **the reconcile floor reads the in-memory task view instead of listing and fetching vault files, and task ownership is a `Task.Vault` comparison.** Each pass snapshots the view, classifies the snapshot against the new `TASK_VAULT` setting (defaulting to `VAULT_NAME`), and drives only the tasks stamped with this executor's vault; a pass over 15,000 tasks is an in-memory scan that completes well under a second. A not-ready view counts `view_not_ready`; a snapshot with tasks but none from this vault counts `task_vault_mismatch` and logs the vaults it saw; a snapshot in which every task is unstamped counts `task_vault_unstamped`. The same `Task.Vault == TASK_VAULT` rule gates the live event path, replacing the path-derived ownership set and counting a foreign task as `agent_executor_task_events_total{result="skipped_foreign_vault"}`, and `queued_at` is read from the view rather than git-rest. `TASK_VAULT` is validated as a vault slug at startup; `TASK_GLOB` is no longer required.
- refactor: **remove the git-rest client, the gateway-secret reader, and the `GITREST_URL` / `GITREST_GATEWAY_SECRET` / `TASK_GLOB` settings.** The executor now reads task state only through Kafka, so the startup refusal on an empty task glob and the git-rest ServiceAccount/Secret grant are gone with them.

## v0.25.0

- feat: **the executor builds an in-memory task view from the compacted task event topic at startup.** The new `TaskView` replays `{TOPIC_PREFIX}-agent-task-v1-event` from the oldest offset on every partition, keeps the latest version of each task, honours tombstones, and reports readiness once every partition has caught up to the high-water mark captured at start. It reads through a non-group simple consumer, so it never joins `CONSUMER_GROUP` and never moves the live consumer's offsets; a decode failure drops the stale entry and increments `executor_task_view_skipped_total` without blocking readiness. The view is wired into `service.Run`, and a reader failure exits the process so a pod restart rebuilds it.
- chore: bump `github.com/bborbe/agent` to v0.113.0 (task events now carry the vault name), with the transitive bumps `go mod tidy` resolved.

## v0.24.0

- feat: add a claim lease client library for git-rest `refs/claims` endpoints, with a compare-and-swap lease manager, fake-clock unit tests and `docs/claim-lease-protocol.md`.
- feat: add `spec.rerunOnLostClaim` to the Config CRD — a per-agent optional boolean (default `true`) deciding whether a task is re-dispatched or parked at `human_review` after another executor takes over this agent's expired claim; the property is declared in the executor-installed CRD schema so the API server does not prune it, and a pure `ConfigSpec.LostClaimAction` maps it to `RedispatchAfterLostClaim`/`ParkAfterLostClaim` (no runtime wiring yet)
- fix: **a parked task whose task_type two Configs claim is re-driven to the Config its `previous_assignee` names.** On sun two Configs declared `github-update-go`, so the reconcile floor's task_type lookup returned `task_type "github-update-go" matches 2 configs` and skipped 66 tasks, including parked ones whose assignee had been cleared at escalation. `ResolveByTaskType` now takes a preferred assignee: when several Configs match, it narrows them the way `Resolve` matches an assignee (`<assignee>-<vault>` first, then the plain `<assignee>`) and returns the single survivor, logging the tie-break at V(2) with the task_type, the chosen Config and the match count. The reconcile path passes the task's `previous_assignee`. A `previous_assignee` naming none of the matches, or no `previous_assignee` at all, still returns `ErrConfigAmbiguous` (its message now names the `previous_assignee` it tried), and it never selects a Config that does not declare the task_type.

## v0.23.5

- fix: **Evicted deferred respawns stay evicted.** A replay or reconcile drive whose entry was evicted mid-flight no longer spawns or re-parks the stale snapshot, and every event filter skip now evicts the task's deferred entry.

## v0.23.4

- fix: **a task retagged to another cluster while parked behind the concurrency cap no longer runs on the cluster it was parked on.** The deferred-respawn entry held the pre-retag snapshot and was spawned locally once a slot freed (prod 2026-10-21, nukeprod ran a `cluster: sun` task). A foreign-cluster event or reconcile read now evicts the pending entry.

## v0.23.3

- fix: **the concurrency cap no longer counts against a running reconcile pass's frozen Job snapshot.** The per-pass active-Job cache was a handler field armed for the whole pass, so the Kafka consumer and the deferred cap retries, running concurrently, were answered from it too. It was invalidated only when a Job was created, never when one completed. On sun, where a pass runs for minutes, a snapshot taken at 6 stayed at 6 while Jobs finished: the executor logged `concurrency_cap running=6 cap=6` while only 4 Jobs were active and the quota showed 4/6. Measured 2026-10-21: 132 refusals between 10:46 and 11:00Z, and queued tasks stranded once their retries ran out. The cache now travels on the pass's context, so only the pass's own lookups use it. Each entry also records the assignee's spawn generation, which every Job creation on any path bumps, so a Job the event path spawns mid-pass is never missed by the pass's own cap check, and a spawn for one assignee leaves the others cached. An entry expires after 10s, so a long pass sitting at the cap re-reads instead of refusing with free slots for its whole lifetime.

## v0.23.2

- fix: **a task whose Job already succeeded is no longer re-spawned while its result is still on its way to the vault.** Measured on nuke dev 2026-10-21: task `a055d019`'s claude-headless Job finished in ~20 s, the task file still read `phase: execution` with no `current_job` (neither the spawn notification nor the write-back had synced through git-rest), so the reconcile floor found no active Job and spawned it again on every ~60 s tick — 3 starts for one task, and the same on sun. A spawned Job now carries the phase it ran for (annotation `agent.benjamin-borbe.de/task-phase`); the job watcher records each task's latest success with that phase and the Job's `completionTime` (so the informer's re-delivery of an old Job cannot refresh it, and a restart re-learns it from Jobs still within their TTL); and `spawnIfNeeded` skips a task still at that same phase whose Job succeeded within the 300 s respawn grace window (`event=skip_recent_success`, counter `skipped_recent_success`). A task the agent advanced to its next phase is spawned at once, a failed Job records nothing so it is still re-driven up to the existing redrive cap, and the record is pruned to the grace window once per reconcile pass.

## v0.23.1

- fix: **a freshly delegated task is no longer starved by the filename-ordered admission queue.** The reconcile floor walked the vault paths in the order git-rest returned them and spawned the first eligible one, so with `maxConcurrentJobs: 1` a child delegated today — named `20261021-…`, and therefore last — was never reached while any older child remained eligible. Measured on prod 2026-10-21: 67 eligible children against one slot, and both admissions in a 20-minute window were the first two files in filename order. Both admission paths now order by `admissionPriority`: a task that has never been attempted (no `job_started_at`) goes ahead of every task that has, then the earliest queue-entry time (`queued_at`, falling back to `job_started_at`) wins. `evalDeferredRespawns` sorts its due entries the same way — it previously spawned in Go map-iteration order, so each tick's winner was drawn at random. The reconcile pass is now two phases: gather and filter every file, sort, then drive, so admission follows the queue rather than the path listing.

## v0.23.0

- feat: **the reconcile pass timeout and interval are configurable.** New envs `RECONCILE_PASS_TIMEOUT` (default `45s`) and `RECONCILE_INTERVAL` (default `60s`) replace the package constants; a non-positive value is rejected at startup. On sun the openclaw executor reads ~15k task files over the git-rest ingress (~7 ms each), so every 45s pass aborted with `evaluated=0` and redrive/backfill never ran there. Nuke dev and nuke prod keep the defaults.

## v0.22.0

- feat: **a cluster can refuse untagged tasks.** The new `RUN_UNTAGGED` env (default `true`) decides whether a task with no `cluster:` frontmatter runs here. With `false`, the executor skips it before Config resolution on both the Kafka path and the reconcile path, logs `skip task <id>: untagged`, and counts `agent_executor_task_events_total{result="skipped_untagged"}`. Set it on sun (`CLUSTER=sun`, controller `PUBLISH_UNTAGGED=false`): until now an untagged task was local on every executor, so with `STAGE_FILTER=false` sun ran untagged prod tasks. Nuke dev and nuke prod keep the default.

## v0.21.1

- fix: **`govulncheck` no longer fails `make precommit` on `GO-2026-6809`.** The Go toolchain moves 1.27.2 → 1.27.3 in `go.mod` and in the `Dockerfile` builder image, which is where the advisory is fixed (stdlib `crypto/x509`, distrust of compromised intermediate certificates). CI on `master` had been red since the v0.21.0 push: `vulncheck` runs inside `make precommit`, and the ID was not in `VULNCHECK_IGNORE`, so the target exited 1 on every push.

## v0.21.0

- feat: **a cluster can opt out of the stage check.** The new `STAGE_FILTER` env (default `true`) turns off the stage-vs-`BRANCH` skip on both the Kafka path and the reconcile path. Set it to `false` on a cluster whose controller already scopes tasks by cluster (sun, with `CLUSTER=sun` and `PUBLISH_UNTAGGED=false`); a task there then needs only `cluster: sun`, with no `stage:` field. `BRANCH` is unchanged and still sets the topic prefix and the child Job env. Nuke dev and nuke prod keep the default and still respect `stage`.

## v0.20.0

- feat: **an agent task can name the cluster that runs it.** A task carrying `cluster: <name>` frontmatter now runs only on the executor whose new optional `CLUSTER` env var matches. Every other executor logs `skip task <id>: foreign cluster <name>` and counts `agent_executor_task_events_total{result="skipped_foreign_cluster"}`. The check runs before Config resolution on both the Kafka path and the reconcile path, so a foreign task is not miscounted as an unknown assignee. A task without `cluster:` is unchanged. The Kafka consumer group is now set by `CONSUMER_GROUP` (default `agent-task-executor`, the previous hard-coded value). Each cluster needs its own group so every cluster sees every event; a shared group splits the partitions, and a message delivered to the wrong cluster is lost.

## v0.19.0

- feat: **a task deferred behind the concurrency cap now records a durable `queued_at` frontmatter field.** The executor stamps an RFC3339 UTC timestamp once, at the moment the first wait begins, through a new `PublishQueueEntry` on `ResultPublisher` (an `UpdateFrontmatterCommand` carrying exactly `{"queued_at": …}`), so a consumer reading the task file can compute a queue age for the agent-execution stage. A re-deferral never moves the timestamp: `stampQueueEntry` reads the field from the in-memory entry first, then from the vault through git-rest (a duplicate event can arrive after the publish but before the controller applies it), and publishes only when neither carries it. A failed publish is not retried — the in-memory copy suppresses the next attempt rather than publishing a later, wrong time. A spawn that is not deferred never writes the field.

## v0.18.14

- fix: **a task filed after the last completed reconcile pass is no longer skipped as foreign when later passes abort.** Measured on nuke-prod 2026-10-20: the OpenClaw vault reached ~14.9k files, every pass after 13:14Z ended `event=reconcile_aborted reason=context_cancelled`, and because an aborted pass publishes nothing the ownership set froze at 13:14Z — each pr-review task filed afterwards (bborbe/claude-supervisor#458, bborbe/agent#110) logged `skip task <id>: not in this executor's vault` and never spawned. The executor now keeps a path→identifier cache across passes: `prioritizeUncachedPaths` prunes it to the listed paths, rebuilds ownership from it, and visits never-parsed paths first; `recordPathID` owns each parsed identifier immediately instead of at pass completion. A new task is therefore owned within one pass, completed or not.

## v0.18.13

- fix: **a reconcile pass now completes instead of aborting on `context_cancelled`, and a spawn that fails for infrastructure reasons no longer consumes a trigger attempt.** Measured on nuke-prod 2026-10-19: `github-update-go` dispatch was at **zero** — no Job existed, no task left `planning` — while the concurrency cap was never reached (`agent_executor_deferred_concurrency_cap_total{assignee="github-update-go-agent"}` = 0, every admit `running=0 cap=1`). Three mechanisms combined. ⚠️ **Two list calls per eligible task.** `spawnIfNeeded` issued `IsJobActive` and `CountActiveJobs` for every task, so a pass over ~70 eligible tasks made ~140 calls at client-go's default 5 QPS, which alone consumed the 30 s pass budget — the `spawn_error … context deadline exceeded` in the log. The new `JobSpawner.ListActiveJobs` answers both from one `list jobs` per assignee, memoized for the duration of a pass by `beginJobCache`/`activeJobsFor` and invalidated after every spawn; outside a pass lookups go straight to the API, so the Kafka path never reads a frozen snapshot. `reconcilePassTimeout` rises 30 s → 45 s as headroom. ⚠️ **The trigger counter was published before `SpawnJob`.** Every `spawn_error` therefore charged one of the task's `max_triggers` attempts without the agent ever running — the mechanism that walked github-update-go tasks to `trigger_count` 5–7 while producing nothing, and the reason a counter reset alone could not hold. `applyTriggerBudget` splits into `checkTriggerBudget` (writes nothing) and `recordTriggerSpawn` (published only after `SpawnJob` succeeds); the Job is stored before the counter write, so a failed write still leaves the in-memory store correct. A failed counter write still propagates as an error, as before — only its position changed.

## v0.18.12

- fix: **a concurrent rollout of the executors no longer kills the losers with a fatal 409 on the Config CRD they all share.** `SetupCustomResourceDefinition` did a bare Get→Update on `configs.agent.benjamin-borbe.de` — cluster-scoped, and installed on start by every instance (openclaw/personal/trading) — so when two rolled at once the loser wrote against a resourceVersion the winner had already bumped and died on the conflict at `main.go:144`. Observed 2026-10-15 16:08 UTC on nuke-prod (Sentry `849801b716284f5b85261ae001864c75`, pod `agent-task-executor-trading-5644c4cdd6-gcnz6`): one restart each on personal and trading, none on openclaw — the losers are whichever pods happen to lose the race, not a property of any one deployment. The Get→Update now runs inside `retry.RetryOnConflict`, and each attempt re-Gets, so a retry carries a fresh resourceVersion instead of replaying the stale one. ⚠️ **The Update is now skipped when the stored spec already matches, which is what makes the conflict rare rather than merely survivable** — the write is what bumps resourceVersion and opens the window for every other instance, so an unchanged executor that restarts now issues no write at all. That comparison is only meaningful because both sides are normalised through the API server's own CRD defaults first: `desiredCRDSpec()` leaves `spec.conversion` nil while the server stores `strategy: None` on create, so a raw `equality.Semantic.DeepEqual` against the desired spec would report a difference on *every* start and re-issue exactly the needless write the check exists to prevent. The fake clientset does not apply that defaulting, so the obvious test would have stayed green while prod kept writing — `pkg/k8s_connector_internal_test.go` pins the normalisation directly, and removing it fails that spec while removing the retry fails the conflict spec (both confirmed by mutation).

## v0.18.11

- fix: **the reconcile floor now recovers a task an agent parked by clearing its own `assignee`, so a parked task re-enters the spawn filter with no operator vault write and no controller restart.** An agent that cannot finish a task clears its own `assignee` before parking it; the task stays `status: in_progress` and keeps its `previous_assignee`, so it is not terminal — but every path back into the spawn filter tests `assignee` and skipped it by construction, and nothing re-emitted it, because `TaskUpdated` is published only on a content-hash change and a parked file does not change. The park was therefore permanent, and the only lever was a human writing the assignee back by hand: measured on prod 2026-10-14, 20 Go repositories sat on `go 1.27.1` with an already-fixed gate while 46 parked tasks were re-driven manually, the same class already hand-driven once before (84 tasks) with no durable mechanism landing in between. `reconcileTask` now resolves a task's declared `task_type` when its `assignee` is empty — the task's own type still identifies exactly one Config — and the floor writes that Config's `assignee` back through the same `UpdateFrontmatterCommand` path every other executor-originated frontmatter write uses, so the controller applies it and the resulting hash change publishes the `TaskUpdated` the normal spawn path already consumes. ⚠️ **The floor does not spawn for a parked task — the restore is the whole action — and the branch sits before the consecutive-re-drive cap.** The restore consumes no spawn budget, so letting the cap gate it would mean a task the floor already re-drove `maxConsecutiveRedrives` times at its current phase returns `capped` and stays parked forever, which is exactly the re-drive-and-re-park class this recovery exists for. ⚠️ **The value written is the Config's `assignee`, never the task's `previous_assignee`.** That key records what a task carried before it was parked, not what it should carry next; reading it as a routing source is the substitution the operator runbook records a full night lost to (2026-09-13, two prod probes written to an agent whose effective `task_type` set excludes the task's type, sitting with a `## Failure` for hours). The write carries exactly one key — no `trigger_count`, no `trigger_scope`, no `current_job`, no `status`, no `phase` — so a capped task stays capped. ⚠️ **The Kafka consumer path is deliberately unchanged.** The fallback lives behind a new `resolveConfigForReconcile` that only `reconcileTask` calls; `parseAndFilter` keeps its assignee-only `resolveConfig` and its `skipped_assignee` guard, because routing the consumer through the by-type lookup would let an ambiguous declared type return an error out of `ConsumeMessage` and tear the consumer loop down with the offset unmarked, wedging on redelivery. A task whose declared type resolves to no Config, or to more than one, is left untouched and its skip logged naming the task and the type — the no-Config case reuses the existing `skipped_unknown_assignee` label on `TaskEventsTotal` rather than adding a metric, since that counter is keyed on a routing value and there is none. No new Config field, environment variable, CLI flag or CRD field, and the controller's `RedriveSweep` keeps its own empty-assignee guard as spec 009's constraints require.

## v0.18.10

- fix: **an executor instance now spawns only tasks that live in the vault it serves, so the consecutive-re-drive cap bounds the task rather than each pod.** v0.18.9 moved the cap into `spawnIfNeeded` and it does engage — `event=redrive_capped … streak=3 cap=3` fired 44 times on the dev fixture — but the streak is an in-memory map on the handler, i.e. one per instance, while a task's events are delivered to whichever instance owns the Kafka partition rather than to the instance whose vault holds the file (the consumer group `agent-task-executor` is shared across every deployment). Measured on dev 2026-10-14 over 91 min: **one parked fixture spawned by three different instances** — three Jobs from the first, two from the second, one from the third — **>=7 Jobs against a cap of 3**, each instance starting its own streak at zero. Twice, two instances reached `spawnIfNeeded` for the same task within the same second; only `job_spawner.go:173`'s `already exists` check kept it to one Job. The cross-vault traffic is not fixture-specific: over 3 h one instance spawned another vault's tasks, and five task identifiers were spawned by two instances. ⚠️ **Ownership is read from the vault, not from the event.** A task event carries only `TaskIdentifier`, frontmatter and content (`lib.Task`) — it names no vault — so the test uses the identifier set the 60 s reconcile pass already builds: `reconcileOne` records every identifier it parses, eligible or not, and `ReconcileOnce` publishes the set once the pass completes, so the check costs one map lookup and no extra vault read. Publishing only on a completed pass is deliberate: an aborted pass would disown every file it had not reached yet. The gate is fail-open until the first pass completes — a freshly started instance has not scanned, and the window is one pass (<=60 s) which its own floor covers. Every task observed on dev sits inside exactly one instance's glob, so a foreign spawn is a duplicate of work the owning floor already does, not the only driver. Adds the `skipped_foreign_vault` label on `TaskEventsTotal` and a V(2) `skip task …: not in this executor's vault glob=…` line.

## v0.18.9

- fix: **the consecutive-re-drive cap now counts every spawn, on every path — v0.18.8's reconcile-only counter did not bound the loop, and dev proved it.** Re-verified against the same parked fixture on dev 2026-10-14, 70 min on `v0.18.8`: **four Jobs, still spawning at the end of the window** (`…152638` 15:26:38 · `…154151` 15:41:51 · `…161245` 16:12:45 · `…162923` 16:29:23), with `capped=0` on all 68 `event=reconcile` pass summaries and zero `event=reconcile_redrive_capped` lines. The floor initiated only two of the four; the other two arrived on the Kafka path (`kafka_consumer-offset.go` → `ConsumeMessage`), and each one cleared the floor-only streak before it could reach the cap. The counter and the check move into `spawnIfNeeded`, the one funnel the Kafka consumer, the deferred-respawn loop and the reconcile floor all pass through, so no path can bypass the cap. ⚠️ **The reset is the phase, and the phase comparison is not redundant with the record.** The streak is only rewritten after a spawn succeeds, so a task that advanced still carries its old phase until that spawn happens — a count-only check caps it before it can ever be released, stranding exactly the task the reset exists to free. Caught by the new phase-advance spec, which failed on the first implementation of this change. ⚠️ **`trigger_scope` is clobbered by the agent pods, which is the real reason `trigger_count` never rises.** Traced through the fixture's own vault history: every job completion rewrites the frontmatter from a snapshot taken at the start of the agent's run, restoring `trigger_scope: 'planning:'` and a `job_started_at` from the task's creation while `phase` stays `execution`. That stale write is what makes `scopeChanged` true on every spawn, resets the persisted `trigger_count` to 1, and disarms `applyTriggerBudget`'s `!scopeChanged` cap guard — so the "`planning:` ↔ `execution:` flip" v0.18.8 attributed to a phase oscillation is a frontmatter clobber, not a phase change. Any counter persisted in that file is reset by the loop it must count, which is the second reason the streak stays in-memory. Renames `executor_reconcile_redrive_capped_total` → `executor_redrive_capped_total` and `event=reconcile_redrive_capped` → `event=redrive_capped` (now naming the phase) — both introduced in v0.18.8 and not yet in any dashboard. The streak is no longer cleared by a task leaving the eligibility filter, nor by another driver spawning it: a task pinned at one phase across three consecutive spawns stays capped until it moves phase or the executor restarts.

## v0.18.8

- fix: cap the reconcile floor's consecutive re-drives of the same task, so a task that parks after a failed step stops producing a Job every 60 s forever. The floor's intended bound is the scoped trigger budget (spec 005), but that budget cannot bound this shape: a task carrying neither `ref` nor `max_triggers` keeps the cap disarmed by design — arming it re-creates the 2026-08-27 Daily Sentry Triage incident (spec 006) — and `triggerScope` scopes such a task on phase alone, so the observed `planning:` ↔ `execution:` flip handed out a fresh budget on every pass and `trigger_count` never rose above 1. Observed on dev 2026-10-13: four Jobs in ~35 min for one parked hypothesis task, each from `event=reconcile_redrive`, with `maxConcurrentJobs: 1` only serialising them. The streak is in-memory and clears in two cases: the task leaves the eligibility filter, or another driver has spawned it — detected by comparing the task's `current_job` against the Job the floor created, once that Job is no longer running. ⚠️ **Keying the clear on the Job rather than on the task event is load-bearing.** `PublishSpawnNotification` writes `spawn_notification`/`current_job` into the task file on every spawn, and task publication is edge-triggered on vault changes, so the floor's own spawn re-enters as a task event and `parseAndFilter` does not gate it — an event-keyed clear resets the streak the floor just recorded, every pass, and the cap never engages. The liveness clause is the other half: while the floor's own Job is still running, a differing `current_job` is a lagging notification rather than a second spawner. `IsJobActive` is per task, not per Job, so a genuine second spawner is detected one pass later, once its Job finishes — a late clear costs one extra re-drive, a spurious clear costs the cap. Deliberately in-memory: a persisted counter would have to be written to the task file, and that write is itself a vault change that re-triggers the event path, so it would reset the loop it exists to bound; losing it on restart costs nothing, since the floor's purpose is recovering restart-orphaned tasks and after a restart every streak is empty. Adds `executor_reconcile_redrive_capped_total`, an `event=reconcile_redrive_capped` log line naming the task and streak, and `capped=` on the pass summary — an unobservable cap is indistinguishable from a quiet fleet, the failure shape `ReconcilePassesTotal` already exists to avoid.

## v0.18.7

- fix: stamp the Config's declared priority class onto a service agent's StatefulSet pod template, so a `type: service` agent is admitted at its declared priority instead of 0 and is no longer the first workload a higher-priority Job evicts

## v0.18.6

- fix: **Go 1.27.2 now passes `make precommit`.** The standalone `errcheck:` and `gosec:` targets are both retired — each replaced by a comment naming why — and both linters now run embedded in golangci-lint, which `.golangci.yml` already enabled. `GOLANGCI_LINT_VERSION` moves v2.13.1 → v2.14.0 (whose bundled reader handles export data v5), and the `go` directive and `Dockerfile` move to 1.27.2 in the same commit. ⚠️ **Neither target was repairable, which is why both are removed rather than repointed.** `go run github.com/kisielk/errcheck@v1.20.0` and `go run github.com/securego/gosec/v2/cmd/gosec@v2.26.1` each build in their **own** module context, and each pins a `golang.org/x/tools` reader that stops at export-data version 4 while the 1.27.2 toolchain emits version 5 — failing with `internal error: package "X" without types was imported from <pkg>`, at any `go.mod` directive, whatever this repo's graph says. The obvious repair — drop the `@$(ERRCHECK_VERSION)` pin so errcheck resolves from the repo graph — does not work either: `precommit`'s first step is `ensure` → `go mod tidy`, and tidy removes the require because nothing imports errcheck, so the unpinned target dies with `no required module provides package github.com/kisielk/errcheck`. Holding it would need a build-tagged `tools.go`, the pattern the fleet deliberately migrated away from. ⚠️ **Coverage is preserved, not traded away.** `settings.errcheck.exclude-functions` carries the retired target's `-ignore '(Close|Write|Fprint)'` list and `settings.gosec.excludes: [G104]` carries `-exclude=G104`, so the embedded linters report the same set instead of double-reporting what those flags excluded. One honest caveat: the pre-existing `std-error-handling` exclusion preset additionally suppresses unchecked **stdlib** error-handling calls, so the embedded errcheck is marginally looser than the retired target on those; a positive control — an unchecked call to a local error-returning function, which no preset can filter — confirmed the embedded errcheck genuinely fires rather than reporting a vacuous `0 issues`. The retired target's `grep -v k8s/client` exclusion needed no counterpart: errcheck now scans that tree and reports it clean. ⚠️ **The `GO-2026-6599…6617` ignores are removed from both `VULNCHECK_IGNORE` and `.osv-scanner.toml`, and that is a fix rather than a tidy-up.** Those entries suppressed Go 1.27.1 stdlib advisories and were held only because the directive could not move; with the directive at 1.27.2 they are genuinely fixed — verified by removing them and re-running both scanners, which report `No unignored vulnerabilities found` and `0 packages affected by 0 known vulnerabilities`. An ignore that outlives its fix is indistinguishable from a real suppression, which is exactly what v0.18.5 said to avoid. Verified end-to-end by running the gate itself, not a neighbouring target: `make precommit` exit 0 on the 1.27.2 toolchain, with `lint` reporting 0 issues and `trivy` clean. Also picks up a `golines` reformat of `pkg/handler/task_reconcile_test.go`, which `make precommit` applies to the untouched tree.

## v0.18.5

- fix: bump `golang.org/x/net` to v0.60.0 and ignore the Go 1.27.1 stdlib advisories in `VULNCHECK_IGNORE` and `.osv-scanner.toml`, so `make precommit` goes green again. Advisories published after the last CI run (2026-10-02) made `vulncheck` and `osv-scanner` fail on the **untouched** tree — CI runs `make precommit` with Go pinned from `go.mod`, so any push today would have failed, not just this branch. ⚠️ **The stdlib half is ignored, not fixed, and the blocker is a tool rather than this repo.** The real fix is the `go 1.27.2` directive, and it cannot pass `make precommit`: gosec v2.29.0 — the newest release — cannot type-check a 1.27.2 module, failing SSA analysis for 24 packages with `export data version 5 is greater than maximum supported version 4` (reproduced with a clean `GOCACHE` and the toolchain pinned), and `-no-fail` would only mask that while silently degrading the scan to its non-SSA findings. `golangci-lint` v2.14.0 and the retirement of the standalone `errcheck:` target are required by the same bump and are deferred with it. **The x/net advisories in the same scans are fixed for real** — x/net v0.60.0 needs only Go 1.26, so it is independent of the block. Remove the `GO-2026-6599…6617` ignores from both files once gosec supports Go 1.27.2 and the `go` directive here moves to it; see `coding/docs/go-tools-versioning-guide.md` § "Adding a transient stdlib OSV ignore". Also bumps `osv-scanner` v2.3.1 → v2.6.0: the pinned v2.3.1 panics on CI during call-graph analysis in `golang.org/x/tools@v0.38.0/go/ssa` (`panic: unexpected expr: *ast.KeyValueExpr`), and it is deterministic — reproduced on a re-run of the same job — while v2.6.0 carries a fixed x/tools and scans this tree clean.

## v0.18.4

- fix: render a service agent's container env in a deterministic key-sorted order, so an unchanged Config produces a byte-identical StatefulSet and the pod stops rolling once a minute

## v0.18.3

- fix: give a service agent a readiness probe, so its `Ready` state means something. The reconciler built the service container and never set a probe nor declared a port, so `/readiness` was served and **nothing ever called it**: `Ready` was true from the moment the container started and stayed true, and the criterion "readiness fails when the provider is unreachable" was **unobservable rather than false** — the distinction that matters, because the only way to tell a working probe from a missing one is to look for the probe before trusting a green pod. The probe now targets `/readiness` on the agent's listen port (`http`, 9090) and the container declares that port. **The timings are set by the criterion's own budget** — NotReady within 30s — against an endpoint that *dials* the provider with a 5s timeout before it can answer 503, so a failing probe costs ~5s and the kubelet's cycle is the period **plus** that cost. Worst case is therefore `initialDelay + failureThreshold*(period + timeout)` = 2 + 2*12 = **26s**. The first cut used `failureThreshold: 3` at an 8s period, which is **44s** by that same arithmetic — comfortably outside the budget it was written for, and the kind of number that looks fine until someone does the sum; the spec now asserts the worst case rather than the tidy one. Two consecutive failures rather than one, so a single transient dial cannot take a healthy agent out of service. Audited the rest of the service path for the same defect while here — the `whenDeleted: Delete` retention policy and the `prometheus.io/scrape` annotations are both genuinely wired, so this was the last one.

## v0.18.2

- fix: give a service agent's session volume the cluster's default StorageClass when none is configured, rather than an explicit empty one. `SetStorageClass("")` emits `storageClassName: ""`, which Kubernetes reads as *"bind to a PV that has no storage class"* — not as "use the default" — so the claim never bound and the pod never scheduled (`pod has unbound immediate PersistentVolumeClaims`). The builder always emits the pointer and hardcodes `standard` when unset, so neither leaving it alone nor passing `""` expresses the default; the reconciler now nils it after building. Observed live on nuke dev: `datadir-pi-service-0` sat Pending with an empty STORAGECLASS while every chart-managed claim on the same cluster carried `local-path` explicitly. Worth recording how it survived review: the flag's own usage text said *"empty uses the cluster default"* and the code comment said the same, so the belief was written twice and the two agreed with each other — both wrong. No spec covered the empty case at all; it is now the first thing the suite pins.

## v0.18.1

- fix: `ServiceReconciler.UndeployService` now removes a StatefulSet only when this executor owns it, and stops reporting removals that never happened. The loop calls it for every Config that is not a service, and a StatefulSet's name here is just the Config's name — so deleting by name alone let a job Config tear down any workload in the namespace that happened to share that name (`recurring-task-creator` is itself a StatefulSet, and the unit charts deploy workloads matching leaf-agent Config names). The ownership mark is the Config controller ownerRef `buildStatefulSet` already sets; a StatefulSet without it is left untouched and logged as *kept*, never as removed. The log mattered as much as the delete: the deployer's `Undeploy` returns nil for a missing object while logging at V(4), so the previous wrapper printed `removed service statefulset <name>` for every non-service Config — 16 false removals on the first live reconcile in dev, none of which deleted anything. That is the same silent-failure class this repo keeps hitting, inverted: manufacturing evidence instead of hiding it, and it made a no-op read as an outage.

## v0.18.0

- feat: add a `type` discriminator to the Config CRD (`job` | `service`; absent means `job`) and exempt `service` from the `taskType`/`taskTypes` requirement — the executor only knew one shape, a short-lived `batch/v1` Job spawned per task and phase, so there was no way to declare a long-running identity agent that is addressed directly rather than routed by task. `ConfigSpec.Type` is a typed enum (`AgentTypeJob`/`AgentTypeService` plus `AvailableAgentTypes`, rejected by `Validate()` when unrecognised), never a bare string, and the schema constrains it to a two-value enum so a typo fails at admission instead of silently routing an identity agent down the Job path. The `taskType` requirement — enforced twice, by the CEL rule in `configSpecValidations()` and by `ConfigSpec.Validate()` — now applies to job agents only: a service agent is addressed directly and carries neither field, so without the exemption every `type: service` Config would have been rejected at admission. Only an explicit `type: service` is exempt — the clause is `has(self.type) && self.type == 'service'`, not a `!has(self.type) ||` leading guard, which would wrongly exempt an untyped Config that resolves to job and must still carry a taskType. `type` is declared in `configSpecProperties()` **and** the Helm chart's `crds/config-crd.yaml`, because this connector overwrites the cluster CRD on every executor start and a field present only in the chart is pruned within seconds — the v0.5.0 `maxConcurrentJobs` failure, where a pruned integer read as 0 and the feature was inert in prod for weeks. Both copies are covered by the guard tests that exist for that defect class.

- feat: reconcile a long-running StatefulSet for a Config whose `spec.type` is `service`, alongside the per-task Jobs the executor already spawns — the binary only knew the Job shape, so there was no workload that outlives a single task for a person to hold a conversation with. `ServiceReconciler` builds the StatefulSet (one replica, the Config as its controller owner, its own `volumeClaimTemplates` datadir mounted at `VolumeMountPath` or pi's `~/.pi/agent/`), and `ServiceReconcileLoop` re-ensures it once a minute, mirroring the existing task reconcile floor. Two details are load-bearing rather than incidental: the StatefulSet sets `persistentVolumeClaimRetentionPolicy.whenDeleted: Delete`, because Kubernetes retains `volumeClaimTemplates` claims by default and the session PVC would otherwise outlive the CR; and the executor stamps `AGENT_TYPE` from `spec.type` into the container env, so a runner can tell the shapes apart without the type being hand-wired into every CR. `AgentConfiguration` and the resolver carry the type through. A Config that is not a service never gets a StatefulSet — the loop undeploys the name it would have owned, so a Config edited from `service` to `job` (or with `type` dropped) loses its workload instead of orphaning it, since the ownerRef collects only on CR *deletion*, not on edit. An unset type resolves to job. Both are pinned by specs. `SERVICE_STORAGE_CLASS` selects the storage class, empty meaning the cluster default.

## v0.17.1

- fix: honour the Config's `trigger.phases`/`trigger.statuses` on the reconcile path — `reconcileTask` gated on the hardcoded `defaultTriggerPhases`/`defaultTriggerStatuses`, so a Config that narrowed its phase list was authoritative on the Kafka path (`parseAndFilter`) and silently ignored by the reconcile floor. A task sitting at a dropped phase was therefore re-driven every reconcile tick (`defaultReconcileInterval`, 60s) indefinitely, and narrowing the Config to stop it provably changed nothing. Observed 2026-09-24 on the build-fix lane: its Config was narrowed to `[execution]` to match the single phase `bborbe/security-review-agent` registers for `build-fix`, and a hand-written `planning` fixture kept being re-driven at ~1/min (`event=reconcile_redrive`, `trigger_count` 37 → 54) until the executor was patched. `effectiveTriggerPhases`/`effectiveTriggerStatuses` still fall back to the defaults when a Config omits `trigger`, so a Config without the block behaves exactly as before. Supersedes spec 005 AC 1's literal wording, which wrote the default set into the criterion; the floor's purpose is unaffected, since an orphaned task sits at a phase its own Config lists.

## v0.17.0

- feat: count concurrency-cap deferrals by assignee in `agent_executor_deferred_concurrency_cap_total{assignee}`, incremented alongside the bare `TaskEventsTotal{result="deferred_concurrency_cap"}` counter which is kept for continuity — the bare counter says a deferral happened but not which agent is queuing, so `AgentQueueHigh` cannot exclude an agent that is capped on purpose. `github-update-go-agent` runs at `maxConcurrentJobs: 1` by the owner's explicit choice, and its intended backpressure fired the alert on nukeprod for that assignee alone (2026-09-08), costing a manual silence every morning. Filtering the bare counter is not an option: it carries no `assignee` label, so an `assignee!="..."` matcher matches no series and silently disables the alert entirely. Deliberately not pre-initialised — the alert is a positive threshold (`increase(...) > 5`), where an absent series correctly means "no deferrals", and the assignee domain comes from agent Config CRs so it cannot be enumerated at init.

## v0.16.0

- feat: count reconcile pass outcomes in `executor_reconcile_passes_total{result}` (ok, vault_unavailable, list_failed, aborted; pre-initialised in `init()` so `rate()` sees a 0 series rather than no data) — the reconcile backstop logged `reconcile_list_failed` once a minute for months with no metric behind it and no successful pass ever recorded. Counting the `ok` side is what makes `rate(ok)==0` an alertable "the backstop is dead" signal; a failure-only counter saturates and then looks identical to a healthy quiet fleet.

## v0.15.1

- fix: refuse to start when `TASK_GLOB` is empty instead of running the reconcile loop with an empty pattern (git-rest treats an empty glob as match-everything over the whole vault, so a misconfigured executor looked like a healthy quiet fleet) and remove the built-in `24 Tasks/*.md` default — the glob now has to be supplied by the deployment

## v0.15.0

- feat: stamp pod `fsGroup` (new global config `job-fs-group` / `JOB_FS_GROUP`, default 65534) onto spawned agent Jobs when Kafka cert mounting is active (both cert secrets set). Cert files are projected with defaultMode 0440 and root-owned, so a non-root agent image (e.g. cloud-build-watcher, which runs as `USER app` to hold CDB credentials) cannot read `/client-cert/file` without the pod fsGroup being added to its supplementary groups — the first occurrence crashed the watcher Pattern B Job on octopus dev (2026-09-08). `0` disables; cert mounting disabled ⇒ pod spec byte-identical to before.

## v0.14.0

- feat: add .reviewignore for the PR size gate

## v0.13.2

- fix: default-engage the scoped spawn-trigger cap for repo-backed tasks — a task whose frontmatter carries a `ref` is now capped at the lib default of 3 spawns per `phase`+`ref` scope even when `max_triggers` is absent, closing the uncapped respawn loop for the `github-update-go` watcher; an explicit `max_triggers` still overrides the default, and recurring tasks (no `ref`, no `max_triggers`) stay uncapped

## v0.13.1

- fix: `tracksJob` guard stall on respawned-second-success with a late first-job clear. When a task's first job succeeded and was cleared, the task respawned, but the store entry was re-admitted with the stale first-job name, a late first-job `clear_dedupe` arrived after the second spawn, and the second job's success then compared storedJob (first job) vs eventJob (second job) and skipped — pinning `current_job` to the dead first-job name until the agent-publish path resolved it. `handleSucceeded`/`publishSyntheticFailure` now evict a mismatched entry when the tracked job is no longer active (any job for the task still running keeps the entry — the PR #45 stale-re-delivery protection is unchanged).

## v0.13.0

- feat: `ConfigSpec.configMapItems` maps ConfigMap keys to file paths inside the mount. ConfigMap keys cannot contain slashes, so delivering nested paths (e.g. `.claude/CLAUDE.md`) is impossible with a plain key-name mount; `items` (mirroring `corev1.KeyToPath`) closes that. Validated: items require `configMapName`, paths must be relative and free of `..`.

## v0.12.0

- feat: `ConfigSpec.configMapName` / `configMapMountPath` mount a ConfigMap into the spawned agent Job, so an agent can take its behaviour files (`CLAUDE.md`, prompts) from configuration instead of from its image -- a new agent becomes a new ConfigMap rather than a new image build. Wired through `AgentConfiguration`, the config resolver, the self-installed CRD schema (`configSpecProperties`) and the `/agents` handler; `ConfigMapMountPath` is required when `ConfigMapName` is set.
- fix: the PVC and ConfigMap mounts are collected into a single `SetVolumes` call (`applyVolumeMounts`, formerly `applyVolumeMount`). `k8s.PodSpecBuilder` exposes only `SetVolumes` (replace) and no `AddVolumes` (append), so applying the two mounts through separate calls would have silently dropped the PVC volume for any agent configuring both -- with the Job spawning successfully and reporting no error.

## v0.11.6

- chore: update Go to 1.27.1

## v0.11.5

- fix: `make build` refuses to stamp a version onto a tree that is not that version's tag (`check-version-tag`, escape hatch `ALLOW_UNTAGGED_BUILD=1`). `VERSION` defaults to the newest tag in the repo regardless of what is checked out, so an operator-run build from an untagged or older tree silently stamps the newest tag's number onto whatever tree is present (observed 2026-08-30: a v0.7.x checkout republished as `v0.6.8`, prod outage ~30 min). The guard compares `git describe --tags --exact-match HEAD` against `$(VERSION)` and exits non-zero on mismatch.

## v0.11.4

- fix: hold `k8s.io/{api,apiextensions-apiserver,apimachinery,client-go}` at v0.36.4 and `github.com/bborbe/k8s` at v1.14.16, restoring the `exclude` guard against k8s.io v0.37.0 that the v0.11.2 dependency bump removed. v0.37.0 makes the executor OOMKill (exit 137) within ~16s of startup against a 2Gi limit while v0.36.4 runs the same workload at ~20Mi -- 71 restarts in 8h on nuke prod and 109 in 12h on nuke dev, taking the whole agent fleet down. The exclude block now carries a comment explaining why it exists, so a future automated dependency update does not silently drop it again (the 2026-08-27 update run added the same exclude with no recorded reason; the 2026-09-04 run removed it and shipped v0.37.0 as v0.11.2).

## v0.11.3

- fix: extend the respawned-second-success race fix (v0.11.1) to the job-informer path — `JobWatcher.HandleJob` now evicts the in-memory TaskStore entry only when the store entry's `current_job` matches the job being processed (a stale terminal re-delivery of a PREVIOUS job's success/failure no longer evicts the entry tracking the re-spawned job). The v0.11.1 guard covered `cleanupTerminalTask` (Kafka path); the informer re-delivers a terminal Job every ~5 min, and a re-delivered success from the first job evicted the just-re-spawned task's entry, silently no-oping the second job's `current_job` clear (observed live on prod 2026-09-04 for task 67120247: job-1's re-delivered success evicted job-2's store entry).

## v0.11.2

- chore: update github.com/bborbe/agent to v0.85.1, github.com/bborbe/k8s to v1.14.17, github.com/bborbe/kafka to v1.25.11, github.com/bborbe/metrics to v0.6.1, github.com/bborbe/run to v1.10.2, github.com/bborbe/vault-cli to v0.121.0, k8s.io/api to v0.37.0, k8s.io/apiextensions-apiserver to v0.37.0, k8s.io/apimachinery to v0.37.0, k8s.io/client-go to v0.37.0

## v0.11.1

- fix: keep the in-memory TaskStore entry for a re-spawned task when a stale terminal event arrives — `cleanupTerminalTask` now skips eviction when the stored task's `current_job` differs from the terminal event's (the store entry is recorded under the actually-spawned job at spawn time). Previously a delayed `completed` event from a previous job's chain evicted the just-re-spawned task, so the new job's success-path `current_job` clear silently no-oped and left `current_job` set forever (respawned-second-success race, observed on prod 2026-09-03 for task 893c33b9).

## v0.11.0

- feat: add a periodic reconcile loop that re-derives running tasks from the vault (via git-rest) and live Jobs, so a task deferred behind `maxConcurrentJobs` whose in-memory deferral was lost across an executor restart resumes without a Kafka event or vault edit; observable via `executor_reconcile_redriven_total` and `event=reconcile*` log lines, with the per-assignee spawn lock taken unconditionally to prevent reconcile/event double-spawns
- docs: document the reconcile loop env config (`GITREST_URL`, `GITREST_GATEWAY_SECRET`, `TASK_GLOB`) in README.md, including the ServiceAccount `get` permission required for the git-rest gateway Secret (data key `gateway-secret`)

## v0.10.0

- feat: label `skipped_unknown_assignee` with the assignee name — new `agent_executor_skipped_unknown_assignee_total{assignee="..."}` counter incremented alongside the existing `task_events_total{result="skipped_unknown_assignee"}`. The bare counter says a skip happened but not which assignee; the 2026-08-26 → 2026-09-03 sentry-deep-analyzer incident stranded 12 tasks for 8 days with only the bare counter moving, invisible as a routable alert. Observability-only; routing behavior unchanged.

## v0.9.1

- fix: clear `current_job` on the job-success path — the executor's JobWatcher now publishes a `current_job=""` frontmatter command (new `ResultPublisher.PublishClearCurrentJob`, deduped per job like `PublishFailure`) before evicting a succeeded job's task from the TaskStore. Previously a succeeded job left `current_job`/`job_started_at` set forever: the agent's result write-back never clears it and the zombie sweeper is blind to the task once it leaves `Snapshot()`, so `github-update-go` fleet sweeps accumulated hundreds of stale task references.

## v0.9.0

- feat: stamp every published frontmatter command with the task's `target_vault` — `UpdateFrontmatterCommand`/`IncrementFrontmatterCommand` from `pkg/result_publisher.go` now carry `TargetVault` sourced from the task's own frontmatter key, never the executor's `VAULT_NAME` (the shared executor serves all vaults), so controllers can skip cross-vault commands before scanning their vault. Tasks without the key publish exactly as before (`omitempty` keeps the wire shape stable).
- chore: move the pkg package's Ginkgo suite entry-point (`TestPkg`) from the top of `agent_configuration_test.go` into the conventional `pkg/suite_test.go` location so spec discovery is explicit

## v0.8.5

- fix: repoint `DOCKER_REGISTRY` default off the dead `docker.quant` host to `docker.prod.nuke.benjamin-borbe.de:443`

## v0.8.4

- chore: update github.com/bborbe/agent to v0.85.1, github.com/bborbe/kafka to v1.25.11, github.com/bborbe/metrics to v0.6.1, github.com/bborbe/run to v1.10.2, github.com/bborbe/vault-cli to v0.121.0

## v0.8.3

- chore: update github.com/bborbe/agent to v0.85.0, github.com/bborbe/cqrs to v0.6.10, github.com/bborbe/cron to v1.8.29, github.com/bborbe/kafka to v1.25.10, github.com/bborbe/metrics to v0.6.0, github.com/bborbe/vault-cli to v0.118.5, github.com/prometheus/client_model to v0.6.3

## v0.8.2

- fix: `ConfigResolver` falls back to the plain assignee when no `{assignee}-{vaultName}` Config exists — the singular shared-topic executor keeps resolving its un-suffixed (v0.6.x) Config CRs on the v0.7+ line; per-vault installs still match composed first, so vault scoping wins whenever a composed Config exists

## v0.8.1

- fix: split `BackoffLimitExceeded` from `DeadlineExceeded` in the job failure classifier — a Job that exhausted its backoff limit now publishes the pod's terminated reason + exit code (dedicated `pod_oom_killed` for OOM kills) instead of a misleading `deadline_exceeded`

## v0.8.0

- feat: scope the spawn trigger cap to `phase`+`ref` — the executor now records a `trigger_scope` (`<phase>:<ref[:8]>`) alongside `trigger_count`, so an opted-in cap no longer burns its budget across unrelated work. A re-dispatch representing real progress (a new lifecycle phase, or a new commit on the target repo) resets the budget to 1; repeated attempts at the same phase and ref burn it down, which is the shape of a deterministic failure such as a gate broken at one commit. Phase comes from the normalizing accessor so an alias cannot split a scope and grant a second budget; tasks with no `ref` (not repo-backed) scope on phase alone
- fix: an absent `trigger_scope` is adopted, never treated as changed — every task in flight predates the field, so reading absent as "scope changed" would have granted each of them a free budget reset on the first event after deploy, including one already looping at cap. Adoption carries the existing count forward (`n+1`); a task already at cap is skipped outright with no publish at all
- note: the cap stays **opt-in** (an absent `max_triggers` still means no cap). Scoping does not make it safe to default on: a recurring task is not repo-backed, so its scope is constant across re-dispatches and the default-3 fallback would still strip `assignee` and kill the loop — the v0.7.1 / 2026-08-27 Daily Sentry Triage incident. The regression spec for it now names that reasoning inline

## v0.7.2

- chore: update github.com/bborbe/agent to v0.84.0, github.com/bborbe/cqrs to v0.6.9, github.com/bborbe/cron to v1.8.28, github.com/bborbe/errors to v1.6.0, github.com/bborbe/http to v1.26.25, github.com/bborbe/k8s to v1.14.16, github.com/bborbe/log to v1.6.25, github.com/bborbe/run to v1.10.1, github.com/bborbe/sentry to v1.10.0, github.com/bborbe/service to v1.10.10, github.com/bborbe/time to v1.27.11, github.com/bborbe/validation to v1.4.23, github.com/bborbe/vault-cli to v0.116.5, github.com/onsi/gomega to v1.43.0

## v0.7.1

- fix: make the spawn trigger cap opt-in — an absent `max_triggers` no longer blocks spawn at the lib default of 3, so a recurring task that accumulates `trigger_count` across re-dispatches keeps re-dispatching to its routing assignee (2026-08-27 prod incident: Daily Sentry Triage collector leg dead from re-dispatch #2 onward).

## v0.7.0

- feat: Scope Config CR resolution per executor vault — the executor now requires `VAULT_NAME` at startup and resolves each task's plain assignee against the composed `{assignee}-{vaultName}` Config assignee (`pkg/config_resolver.go`), so per-vault `maxConcurrentJobs` slots take effect and Config CRs from other vaults are ignored. The `ConfigResolver` interface and its `ErrConfigNotFound` no-match contract are unchanged.
- fix: address PR review — thread `ctx` cancellation through Config `convert`/`copyEnv` (`pkg/config_resolver.go`) and validate `VAULT_NAME` against `^[a-z][a-z0-9-]*$` at startup (`main.go`), so a cancelled resolve aborts cleanly and a malformed vault slug fails fast instead of silently never matching a Config CR.

## v0.6.7

- chore: update Go to 1.27.0 and github.com/IBM/sarama to v1.60.2, github.com/bborbe/agent to v0.83.0, github.com/bborbe/cqrs to v0.6.8, github.com/bborbe/cron to v1.8.27, github.com/bborbe/errors to v1.5.20, github.com/bborbe/http to v1.26.24, github.com/bborbe/k8s to v1.14.14, github.com/bborbe/kafka to v1.25.9, github.com/bborbe/log to v1.6.24, github.com/bborbe/metrics to v0.5.15, github.com/bborbe/run to v1.9.37, github.com/bborbe/sentry to v1.9.27, github.com/bborbe/service to v1.10.9, github.com/bborbe/time to v1.27.10, github.com/bborbe/validation to v1.4.22, github.com/bborbe/vault-cli to v0.116.1, github.com/onsi/ginkgo/v2 to v2.32.1, github.com/prometheus/client_golang to v1.24.1, k8s.io/api to v0.36.4, k8s.io/apiextensions-apiserver to v0.36.4, k8s.io/apimachinery to v0.36.4, k8s.io/client-go to v0.36.4
## v0.6.6

- chore: update Go to 1.27.0 and github.com/IBM/sarama to v1.60.2, github.com/bborbe/agent to v0.83.0, github.com/bborbe/cqrs to v0.6.8, github.com/bborbe/cron to v1.8.27, github.com/bborbe/errors to v1.5.20, github.com/bborbe/http to v1.26.24, github.com/bborbe/k8s to v1.14.11, github.com/bborbe/kafka to v1.25.9, github.com/bborbe/log to v1.6.24, github.com/bborbe/metrics to v0.5.15, github.com/bborbe/run to v1.9.37, github.com/bborbe/sentry to v1.9.27, github.com/bborbe/service to v1.10.9, github.com/bborbe/time to v1.27.10, github.com/bborbe/validation to v1.4.22, github.com/bborbe/vault-cli to v0.115.0, github.com/onsi/ginkgo/v2 to v2.32.1, github.com/prometheus/client_golang to v1.24.1, k8s.io/api to v0.36.4, k8s.io/apiextensions-apiserver to v0.36.4, k8s.io/apimachinery to v0.36.4, k8s.io/client-go to v0.36.4

## v0.6.5

- chore: Bump errcheck to v1.20.0 and golangci-lint to v2.13.1 for Go 1.27 support

## v0.6.4

- fix: include `maxConcurrentJobs` in `ConfigSpec.Equal` (`k8s/apis/agent.benjamin-borbe.de/v1/types.go`). The executor's Config watch cache (`eventHandlerAlert.OnUpdate`) calls `Equal` to decide whether a Config changed; the field was missing from the comparison, so a `maxConcurrentJobs`-only edit to a live Config CR was treated as "nothing changed" and had no effect until the executor pod restarted. Measured 2026-08-18: a CR patched to `maxConcurrentJobs: 3` left the executor enforcing cap 1 until a rollout restart forced a full re-sync. Added a regression test asserting the field participates in `Equal`.

## v0.6.3

- fix(logging): downgrade the `not in task store` resync warning from `Warning` to `V(3)` in `pkg/job_watcher.go`. The Job informer re-delivers an already-handled terminal Job every ~5 minutes; each redelivery re-enters the missing-task branch and the old `Warning` read as a failure. The synthetic failure was already published on the first (real) observation, so the resync is expected noise (2026-08-10 silent job-failure wedge investigation).
## v0.6.2

- fix: declare `zombieSweeperIntervalSeconds` and `zombieJobTimeoutSeconds` in the CRD schema this connector installs (`pkg/k8s_connector.go`). Same defect class as the v0.6.1 `maxConcurrentJobs` fix: both fields were declared in `ConfigSpec`, read live by the zombie sweeper (`zombie_sweeper.go`) and mirrored in `AgentConfiguration`, but missing from `configSpecSchema()` — so `SetupCustomResourceDefinition` overwrote the cluster CRD without them on every executor start and the API server pruned them from every Config. The sweeper silently fell back to defaults. Added a guard test asserting both fields (with the chart's 10 / 30 minimums) so a future config field cannot ship missing from the CRD again.

## v0.6.1

- fix: declare `maxConcurrentJobs` in the CRD schema this connector installs (`pkg/k8s_connector.go`). v0.5.0 added the field to `AgentConfiguration` and to the Helm chart's `crds/`, but not here — and `SetupCustomResourceDefinition` **overwrites the cluster CRD on every executor start**. The field was therefore pruned from every Config within seconds of any restart, and a pruned integer reads as `0`, which the cap treats as *unlimited*. Net effect: `maxConcurrentJobs` has never durably applied in any cluster, and hand-applied CRD fixes appeared to "revert" — each revert was simply the next pod start. Added a test asserting the schema declares the field, so a future config field cannot ship missing from the CRD again.

## v0.6.0

- fix: `maxConcurrentJobs` is now actually enforced. It was a check-then-act race: `spawnIfNeeded` is reached from two goroutines — the Kafka consumer and the deferred-respawn loop, both started by `service.Run` — and each read the same live Job count before either had created its Job, so both concluded they were under the cap. Measured in prod with cap `1`: 36 tasks released at once admitted 17 Jobs, 15 admitted 15. Every over-cap Job was then rejected by the agent's ResourceQuota, looped on `FailedCreate`, and burned its full `activeDeadlineSeconds` while merely queued before being killed without ever running. A per-assignee mutex now spans the whole count→spawn sequence. Correct only at `replicas: 1`; scaling the executor reinstates the race across processes and would need a lease.
- feat: log the concurrency **admit** decision (`event=concurrency_admit`, V(1)) alongside the existing deferral line. Overshoot previously left no trace at all and could only be found by counting Jobs in the cluster by hand.
- chore: bump `golang.org/x/mod` to v0.40.0 (GO-2026-6179, GO-2026-6180). Unrelated to the fix above, but `make precommit` fails `vulncheck` on master without it, so CI cannot go green otherwise.
- chore: Go 1.26.5 → 1.26.6 in `go.mod` and the Dockerfile (GO-2026-5026, GO-2026-5972, GO-2026-6089, GO-2026-6090, GO-2026-6218). Same reason — `osv-scanner` gates precommit on the stdlib version.

## v0.5.1

- docs: add a License section to the README

## v0.5.0

- feat: `maxConcurrentJobs` on the agent Config CRD caps how many Jobs one agent may run at once. Spawns over the cap are **deferred** through the existing deferred-respawn loop and retried after 60s, never dropped — a skipped spawn would be lost outright, since task publication is edge-triggered on vault file changes and being over the cap produces no such change. `0` (the default) means unlimited, so behaviour is unchanged for every agent that does not set it.
- feat: spawned Jobs carry an `agent.benjamin-borbe.de/assignee` label so concurrency is counted per agent. A fleet-wide limit would throttle cheap agents to protect expensive ones; `pr-reviewer-agent` routinely runs several at once and must not be capped alongside `github-update-go-agent`.

  Sizing, measured 2026-08-10 against the 1800s `activeDeadlineSeconds`: one uncontended `github-update-go` Job took **669s** for a small repo (`cron`) and **903s** for a large one (`kafka`) — both comfortably inside the deadline, so the deadline is not the problem. 20 Jobs spawned within 40s all exceeded it, implying the cluster sustains fewer than ~7.4 concurrently. A cap of 4 puts the heaviest repo at roughly 8 minutes, 27% of the budget.

## v0.4.7

- fix: `IsJobActive` no longer reports a deadline-killed Job as active. An `activeDeadlineSeconds` kill sets the `JobFailed` condition but leaves `.status.active`/`.status.failed` at zero, so the counter-only check fell through to "active" until the Job was garbage-collected — and every retry spawn in that window was silently skipped as `active job exists`. The job-state predicates are now exported from `pkg` (`IsJobFailed`, `IsJobSucceeded`) and reused by the spawner, so the watcher and the spawner can no longer disagree about whether a Job has finished. This wedged 26 `github-update-go` tasks at `trigger_count: 2` against a cap of 3 on 2026-08-10, with no error state and no retry.

## v0.4.6

- chore: update dependencies to latest (15 bborbe libs incl. agent v0.77.2, vault-cli v0.101.1, cqrs, kafka, k8s, time, service; + IBM/sarama v1.50.3→v1.60.0)

## v0.4.5

- Bump `golang.org/x/text` to v0.39.0 (CVE-2026-56852)

## v0.4.4

- fix: cancel a pending grace-window deferred respawn when a task reaches a terminal status (`completed`/`aborted`). The terminal event is skipped by the status filter before the terminal-phase gate that clears the entry, so a deferred entry created during the grace window fired ~300s later and respawned a job for an already-done task ("path C" — a second probe spawn observed on dev after the v0.4.3 run-state-reset fix). Now cleared alongside the taskStore entry.

## v0.4.3

- fix: clear executor-owned run-state (`current_job`, `job_started_at`, `spawn_notification`) in the healthcheck re-trigger `UpdateFrontmatterCommand`, alongside the existing `trigger_count`/`retry_count` reset. Reused probe files carried stale run-state from the prior run, defeating the executor grace window and respawning 2-3 Jobs per probe (plus a phantom `deadline_exceeded` from the zombie sweeper on the ancient job ref). Every re-trigger now starts clean.

## v0.4.2

- Update bborbe/agent, cron, errors, k8s, metrics, service, vault-cli dependencies
- Update golang.org/x/sys and golang.org/x/term
- gofmt struct tag alignment in main.go

## v0.4.1

- fix: mount the Kafka cert secret volumes at their directory (`/client-cert`, `/client-key`, `/server-cert`) so the `path: file` item projects the file to `/client-cert/file` etc. — mounting directly at `/client-cert/file` made that path a directory (`read /client-cert/file: is a directory`), crashing spawned agent Jobs against mTLS Kafka. Matches the executor's own cert mount.

## v0.4.0

- feat: Mount Kafka mTLS client cert/key and CA cert into spawned agent Jobs when the new
  `job-kafka-client-cert-secret` and `job-kafka-ca-cert-secret` executor config values are both
  set — projects three read-only (0440) secret files at `/client-cert/file`, `/client-key/file`,
  `/server-cert/file`. When neither or only one is set, spawned Jobs are unchanged (plaintext-Kafka
  deployments unaffected).

## v0.3.3
- Update dependencies, Go 1.26.5, alpine 3.24
- Ignore openpgp advisory GO-2026-5932 in govulncheck and trivy (unmaintained by design, no fix)

## v0.3.2

- fix: only append the branch as an image tag when the Config's `spec.image` has no tag
  already. Previously the resolver always did `image + ":" + branch`, so a semver-pinned image
  (`…/agent-claude:v0.1.1`) became an invalid `…:v0.1.1:dev`. Tag detection treats a `:` after
  the last `/` as an existing tag (registry-port colons excluded; digests preserved). Untagged
  images (e.g. quant-native `agent-backtest`) still get the branch tag as before. This unblocks
  semver-pinned agent images rendered by the Helm chart.

## v0.3.1

- refactor: converge build to the `bborbe/kafka-topic-reader` publish-only model — `make buca`
  now builds and pushes `docker.io/bborbe/agent-task-executor:$(VERSION)` (semver from git tag),
  replacing the private-registry `:$(BRANCH)` flow and the separate `publish` target. Deployment
  moves to the quant config repo; removed `k8s/*.yaml`, `Makefile.k8s`, `Makefile.env`, and the
  stage `.env` files (kept `k8s/apis` + `k8s/client` CRD code).

## v0.3.0

- feat: add `make publish` target to build and push a semver-tagged public image to
  Docker Hub (`docker.io/bborbe/agent-task-executor:<version>`), independent of the
  private-registry `buca` flow. Pattern mirrors `bborbe/kafka-topic-reader`.

## v0.2.0

- feat: propagate `TOPIC_PREFIX` (from `TopicPrefix` config) to spawned per-task Jobs, alongside the existing
  `BRANCH` env var, so child agents (agent-claude/code/gemini/pi/sentry-issue-analyzer) can build their Kafka
  result topics.

## v0.1.0

- feat: add explicit `TopicPrefix` config (`arg:"topic-prefix"` / `env:"TOPIC_PREFIX"`), replacing the implicit
  `Branch`-derived Kafka topic prefix. `Branch` (`env:"BRANCH"`) is unchanged and keeps its non-topic uses
  (child-job `BRANCH` env propagation, config image tagging, stage matching). Bumps
  `github.com/bborbe/agent` to v0.72.0 and `github.com/bborbe/cqrs` to v0.6.0.
