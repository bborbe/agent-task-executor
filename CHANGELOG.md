# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

- feat: **a repo-keyed claim now acquires both the per-repo and the per-PR ref, so a mixed fleet cannot spawn the same task twice.** `DeriveClaimKeysWithBody` returns the per-repo key as primary and, for a `pr-review` task whose body carries a standalone pull-request URL, the per-PR key as secondary; tasks whose two derivations are equal keep the single-ref path byte-for-byte. `LeaseManager.ClaimPair` writes the primary first and the secondary second, releases the primary and returns `ErrLeaseConflict` when the secondary conflicts, and displaces a secondary ref left behind by an expired predecessor. `TakeoverPair` extends the same pair handling to takeover, and a successful dual acquire logs `event=claim_dual_acquired` while a secondary conflict logs `event=claim_dual_conflict`. During the dual-write rollout, reviews of different pull requests in one repository serialize on the shared per-repo primary.
- feat: **every claim release path now acts on both refs of a dual claim as one unit.** `Release`, `ReleaseAfterSuccess`, the success hold, the fence, the jobless and release-after-running-check paths and takeover delete the primary and the secondary ref, counting one removal for the pair; a non-`409` failure on either ref leaves the whole claim pending-release and retried on the next tick instead of half-releasing it. A lost claim still drops the pair without contacting git-rest. A renew tick writes both refs with a single `lease_until` and counts one renew outcome, and a failed primary renew keeps the pair's effective expiry at its older value so the fence cannot read a lease the primary never got.

## v0.34.0

- feat: **a task whose claim is taken over is now parked at `human_review` instead of re-run when its Config says so.** `ConfigSpec.LostClaimAction()` had no production caller, so `rerunOnLostClaim: false` was inert and a lost claim re-ran the task on whichever cluster won the next claim. The takeover branch in `pkg/handler/claim_wiring.go` § `acquireSpawnClaim` now consults it: `ParkAfterLostClaim` publishes `phase: human_review` and creates no Job, logging `event=parked_lost_claim key=… task=…` so the park is attributable independently of the vault write's propagation, and counting `executor_parked_lost_claim_total{result}` because the new default makes an unexpected fleet-wide park alertable rather than log-only. Only `CLAIM_MODE=on` parks — shadow's contract is to take claims without ever blocking a dispatch. The phase write is retried, and the success line is logged only once it lands: returning without the write is not fail-safe on its own, since `releaseJoblessClaims` drops the held claim after 300 s and a task still in a spawnable phase would then run again. `RerunOnLostClaim` is carried through `AgentConfiguration` and `TaggedConfigurations` so the decision is reachable from the claim path. **The unset default flips to `false`**: a Config that leaves the field out now parks rather than re-dispatches, so the guard covers every agent that has not explicitly opted back in. `human_review` was already in the terminal-phase set, so a parked task is never re-spawned.

## v0.33.0

- feat: **the Job watcher now observes every Config's sandbox namespace.** The watcher derives its watched set from the executor namespace plus every distinct `spec.jobNamespace` its Configs name, starts one Job/Pod informer pair per namespace, and re-resolves the set every 60 seconds so a Config flip is picked up without an executor restart. A terminal-failed Job in a sandbox namespace now produces the same synthetic failure and store cleanup as one in the executor namespace. A namespace no Config names is never watched, no informer is stopped for a namespace a Config stops naming, and no new RBAC is requested.
- fix: **a sandbox Job is now classified from its own namespace's Pod cache.** `JobWatcher.PodLister` takes the namespace whose lister is wanted and returns nil for a namespace that is not watched or has not synced, so `terminalPodDetail` reads the Job's namespace instead of the executor's. The zombie sweeper resolves each task's namespace from the Configs it already lists — first Config whose assignee matches, falling back to the executor namespace when none matches or the Config's `jobNamespace` is unresolvable — and skips only that task while its namespace's cache is still syncing, rather than reading the executor's cache or stopping the whole tick.

## v0.32.0

- feat: **the Job watcher now observes every namespace a Config places Jobs in.** It receives the Config provider and derives its watched set from the executor's own namespace plus each Config's resolved `spec.jobNamespace`, starting one Job/Pod informer pair per namespace with the existing 5-minute resync and task-id label selector. A Config whose `jobNamespace` equals the executor namespace is skipped with an ERROR and no informer; every other namespace keeps watching. Handler dispatch is gated per namespace: an event is handled only once its own namespace's cache has synced, and an unconfigured namespace is never handled. The stale-terminal re-delivery guard now asks about the Job's own namespace rather than always the executor's. Provider, informer-construction and handler-registration failures fail `Run`; list/watch failures during cache sync remain environmental.
- feat: **the Job watcher now re-derives its watched namespace set every 60 seconds.** After the startup resolution, `Run` ticks on a package-level interval, re-lists the Configs, and starts informers only for namespaces not already watched, so a Config flip that names a new `jobNamespace` is picked up without an executor restart. A Config provider failure on a tick is logged at ERROR and retried on the next tick; a wiring failure while starting a new namespace's informers still fails `Run`. A namespace removed from the Config set keeps its informer until process exit.
- feat: **the executor counts task re-runs.** `executor_task_reruns_total{task_type}` increments when a Job is spawned for a task whose last success was recorded for the *same phase* inside a 1 h window — a re-run, meaning the same unit of work running twice. The phase scoping is deliberate: a task that succeeds at planning and is spawned again for execution has advanced, not re-run, and counting that would fire a re-run alert on every ordinary task. The series is created at 0 on every spawn, so the absent-tier alert can tell "no re-run yet" from "the metric stopped being exported". The success record's retention moves from the 300 s respawn grace to a new `taskRerunWindow` (1 h) so a re-run an hour later is still recognisable; `defaultRespawnGracePeriod` and `claim.SuccessGracePeriod` keep their 300 s, so spawn refusal and claim holds are unchanged. A task whose first success ran on another cluster is not in this executor's store and is not counted here; cross-cluster re-runs stay visible via `executor_claim_total{result=~"taken_over|adopted"}`.

## v0.31.1

- fix: **a stale success no longer releases a claim inside a newer run's success hold.** The Job watcher keeps re-reporting an older Job of the same task as succeeded. That completion is already past its own window, so `ReleaseAfterSuccess` used to delete the claim the newer run's hold protects, and the peer cluster ran the task again seconds later (seen live on v0.30.7 for update-go and releaser tasks). The call now keeps a claim whose hold is still in the future, logs `event=release_kept_success_grace`, and extends a hold but never shortens it.

## v0.31.0

- feat: **agent Jobs can be spawned into a separate namespace.** `ConfigSpec` gains `spec.jobNamespace`; empty (the default) keeps creating the Job in the executor's own namespace, so the deploy is a no-op until a Config sets it. A non-empty value places the Job there, and a value equal to the executor's own namespace is rejected with an ERROR log and no Job created, so no Config can author an opt-out. `IsJobActive`, `JobExists`, `CountActiveJobs` and `ListActiveJobs` all resolve the same namespace the Job was created in, so the duplicate-spawn guard and the concurrency cap keep working across namespaces, and the claim fence (`IsJobRunning`/`DeleteJob`) resolves it the same way so a claim that cannot renew still deletes its Job — resolving the task from the replayed task view rather than the spawn-time store, because startup recovery asks about Jobs the pod ran before it restarted, and recovery waits (bounded) for that view before deciding a claim. Spawned Job pods keep the unconditional `app: agent` label.

## v0.30.9

- fix: **a held claim is now released when its Job has been continuously absent for 300 s.** A Job deleted by hand, or whose terminal event was lost, left its claim held and renewed forever, so the task could never be respawned and the repository stayed blocked. The lease loop now asks `JobControl.IsJobRunning` at most once a minute per claim and sends one compare-and-swap `DELETE` with the held sha once the Job has been gone for the grace period, counting `jobless_released` and logging `event=claim_jobless_released`. The success hold, a pending release, a pending Job deletion and an unanswered adopted running check keep their own handling. Each jobless pass starts no new check after 10 s, or as soon as the context is done; claims it did not reach are checked first on the following ticks.
- fix: **a fence whose Job deletion fails no longer logs a release.** It logs `event=claim_fence_pending` and counts `fenced` once; the `event=claim_released … result=fenced` line follows when a later tick deletes the Job and the ref.
- fix: **a `pr-review` task is now claimed per pull request.** A task whose body carries the repository's own standalone pull-request URL is claimed at `claims/pr/<owner>-<repo>-<n>` instead of `claims/repo/<owner>-<repo>`, so reviews of different pull requests in one repository run in parallel while two tasks for the same pull request still exclude each other. A body with no usable PR link falls back to the repository key, logs `event=claim_key_fallback` and increments `executor_claim_key_fallback_total`; the other repo-keyed types are unchanged.

## v0.30.8

- fix: **the consecutive-re-drive cap is now shared across clusters and survives a restart.** The streak lived only in each executor's memory, so two clusters driving the same task each counted their own spawns and a restarted pod started at zero — task `1788bf3c` was re-driven past the cap that way. The count is now also written to `claims/redrive/<taskid>/<phase>/<n>` refs in the same git-rest instance as the dispatch claims, and the cap refuses when the larger of the stored and in-memory counts reaches 3. A failed store read or write falls back to the in-memory floor with a warning, and claim mode `off` is unchanged: no store is built and no request is sent.

## v0.30.7

- fix: **the success hold now survives a plain release.** When a task's terminal phase reached the executor that ran it, the phase gate and terminal cleanup called `LeaseManager.Release`. That deleted the claim at once and ignored the `holdUntil` that `ReleaseAfterSuccess` had set. The terminal phase reaches the winning cluster's vault view before a peer's, so the hold ended exactly while the peer still read the task as eligible. Measured on v0.30.5, 2026-10-23: task `70688157` succeeded on nuke prod at 09:45:51 with hold_until 09:50:51. Sun claimed and ran it again at 09:48:20, still reading phase `execution`. Six re-runs in two hours followed the same shape. `Release` now keeps a claim whose hold has not expired (`event=release_kept_success_grace`), and the hold's own expiry releases it.

## v0.30.6

- fix: **a cap-deferred task whose `queued_at` write can never land no longer re-publishes that write on every retry.** `stampQueueEntry` decided whether to publish from the event payload and the task view, and both only learn the value from a *successful* vault write. For a task whose file is gone — or whose write keeps failing — neither ever carries `queued_at`, so every deferral stamped a fresh value and published another `UpdateFrontmatterCommand`, once per `concurrencyCapRetryDelay`, indefinitely; the controller answered each with `update-frontmatter: task file not found … skipping` and moved on, so nothing bounded the loop. Measured on nukedev 2026-10-23: one command per minute for task `1AACF3FC`, whose vault file had been deleted the day before, and the same pattern for a live task (`be07bfab`) whose writes were not landing either. The executor now records the `queued_at` it published, keyed by task, and reuses that value instead of re-stamping; the record is dropped when the task leaves the deferred state, so the map stays bounded by the deferred working set. The record is executor-local by design — it is the one place the value is known without a successful write, and it survives a publish failure, which is deliberately not retried.

## v0.30.5

- fix: **a cold-started executor no longer re-runs tasks another cluster just completed.** Two changes close the same gap from both ends. First, the event path now waits for the task view to be ready, exactly as the reconcile path already did: a pod that has just started resumes its consumer from the last committed offset, so its first events are a backlog of snapshots that may already have been handled elsewhere, and the anti-duplicate checks that would refuse them read the view and the task store — both empty on a cold pod. Skipped events are driven by the next reconcile pass once the view has replayed, so nothing is dropped. Second, a **successful** Job now holds its claim for `claim.SuccessGracePeriod` instead of releasing it immediately: a succeeded Job is not yet a finished task, the vault learns about the result on the controller's own cycle, and until it does a peer reads the task as eligible with no record that it just ran. Holding the claim makes the peer's `Claim` fail (409) instead of running the task twice; a failed Job still releases at once so retries are not delayed, and the hold is bounded by the grace window and by the lease. Measured 2026-10-23: five task ids ran on both nuke prod and sun within minutes of sun coming back, two verified as double successes — task `1788bf3c` Complete on prod 05:55:00 and again on sun 05:56:15, both successful.
- fix: **the foreign-Job guard can no longer suppress a task forever.** The guard added by this release's `current_job` fix suppresses a spawn while this executor's store names a different Job than the frontmatter does. Nothing clears that store entry except the job informer processing that Job's terminal event, and the informer registers no `DeleteFunc` — so a Job garbage-collected before its terminal update was observed left an entry nothing ever removed, and the task was suppressed on every pass, silently and indefinitely. The guard now asks the cluster whether the stored Job still exists (`JobSpawner.JobExists`); when it is gone the entry is stale, it is dropped, and the spawn proceeds. A failed read keeps suppressing rather than risking a duplicate.

## v0.30.4

- fix: **a `current_job` that names another cluster's Job is no longer treated as finished.** `current_job` lives in the shared vault frontmatter, so an executor can read a Job name that another cluster created, while `IsJobActive` only lists its own cluster's Jobs — "not active here" was read as "finished" and authorised the respawn. On 2026-10-22 task `94c1801c` ran on sun and nuke prod concurrently for 2 m 33 s that way. A dispatch pass now suppresses the spawn and re-defers when this executor's store still names a different Job than the frontmatter does, counting `skipped_foreign_job`. The no-store-entry case keeps the previous behaviour: nothing local distinguishes a foreign Job from a finished one there, and bounding it by the Job's deadline would withhold a legitimate respawn for up to 30 minutes.

## v0.30.3

- fix: **startup recovery no longer shuts the service down.** v0.30.2 ran `Recover` from a run func that returned as soon as recovery finished, and `service.Run` wraps every run func in `run.CancelOnFirstFinishWait` — it cancels all the others as soon as any one completes. The recovery step therefore cancelled the lease loop, the HTTP server and the cron a few seconds after boot: every executor on nuke dev, nuke prod and sun exited **0** with `claim lease tick failed: context canceled` followed by `application finished`, leaving the previous ReplicaSet to serve traffic. The step now recovers once and then holds until the context is cancelled, like every other run func. Two regression tests pin it — one at the claim layer, one at the factory — asserting the recover func does not return while the service context is live.

## v0.30.2

- fix: **`Release` no longer drops a claim while this cluster's Job is still running.** It deleted the ref and removed the held entry with no Job check, and because the fence only iterates the held set, a release that fired while the Job ran left that Job running unclaimed and invisible to the fence — so another cluster could take the task over and run it a second time. A release now keeps the claim held, so the loop keeps renewing it, and logs `event=release_deferred_job_running`; the Job's own terminal event releases the claim as before. A release for a Job that is already gone is unchanged.
- fix: **startup recovery now actually runs.** `LeaseManager.Recover` was implemented, tested and on the interface but had no production call site, so a restarted executor began with an empty held set: nothing renewed and nothing fenced the Jobs it had been running, their leases lapsed, and another cluster legitimately took the work over while the original Job ran on to its natural end. Every rollout replaces a pod, so this happened on every apply — on 2026-10-22 it produced a 2 m 33 s duplicate of task `94c1801c` across sun and nuke prod. The lease manager's run funcs now begin with a recovery step that re-adopts, releases or fences the claims this cluster held before the restart, retried a bounded number of times; a git-rest outage at boot is logged and never crash-loops an executor that can still process tasks.

## v0.30.1

- fix: **a repo-keyed task whose frontmatter carries `clone_url` instead of `repo` now derives its claim key.** Every pr-review task the GitHub watcher creates carries `clone_url` (9,779 of 9,779 in the OpenClaw vault on 2026-10-22) and no `repo` key, so `claimOwnerRepo` returned an empty owner and `DeriveClaimKey` rejected the task with `claim owner must not be empty`. In `shadow` that only logged; in `on` it fails closed, so no pr-review task would ever have run. The key is now derived from an https, `ssh://` or `git@host:owner/repo` clone URL (optional `.git`), `repo` still wins when both are present, and a task with neither — or an unparseable URL — still fails loudly.

## v0.30.0

- feat: allow `CLAIM_MODE=on`. The lease lifecycle now ships — release at every end point, renewal and fence from the service run list, and takeover of an expired claim — so a claim no longer outlives its task. An unset `CLAIM_MODE` remains `off`: each cluster opts in explicitly through its own config.

## v0.29.0

- feat: attempt one lease takeover on a 409 claim conflict and proceed on an expired lease
- feat: renew a held claim while its Job runs, and retry a release an end point could not deliver, from one lease-loop entry in the service run list. `off` adds no loop.

## v0.28.1

- fix: wire a real Kubernetes `JobControl` into the claim lease manager. The fence and restart recovery previously ran against a control that refused every call, so a claim that could not renew logged an error and left its Job running unclaimed — the duplicate the claims exist to prevent.

## v0.28.0

- feat: release the dispatch claim when its Job ends or its spawn fails
- feat: release the dispatch claim when a task reaches a terminal status or phase

## v0.27.0

- feat: gate task dispatch on a git-rest lease claim after all existing gates (off/shadow/on; off unchanged). A claim-taking mode now also requires `CLUSTER` and a well-formed `GITREST_URL` (`http`/`https` with a hostname), both refused at startup rather than on the first dispatch — a deployment that set only `GITREST_URL` will fail to start until `CLUSTER` is set. `CLAIM_MODE=on` is refused outright until the lease lifecycle ships: nothing releases, renews or takes over a lease yet, so a claim would outlive its task and block every later dispatch of that key. `shadow` is unaffected, because it proceeds on a conflict.

## v0.26.4

- fix: **spawned Job names are always lowercase.** `jobNameFromTask` concatenated the raw task-id prefix, so an uppercase UUID produced `trading-backtest-agent-1AACF3FC-...`, which Kubernetes rejects because `metadata.name` must be a lowercase RFC 1123 label. The whole name is now lowercased.
- fix: **a failed Job create in the deferred-respawn loop no longer exits the executor.** A `SpawnJob` failure is logged with the task id, counted as `agent_executor_task_events_total{result="deferred_respawn_spawn_failed"}`, and the loop continues with the next due entry; the entry is not re-parked and the reconcile floor re-drives the task. Every other deferred-loop error stays fatal.
- feat: add the `CLAIM_MODE` setting (`off` | `shadow` | `on`) and the `GITREST_URL` / `GITREST_GATEWAY_SECRET` settings, all validated at startup. An absent `CLAIM_MODE` is `off`; an explicitly empty or unrecognised value is an error rather than a silent default; and a claim-taking mode with no `GITREST_URL` refuses to start instead of failing on its first dispatch. `GITREST_GATEWAY_SECRET` is the NAME of the K8s Secret, never the secret value. The claim client itself is wired in the follow-up change.
- feat: construct the git-rest claim ref client in the claim-taking modes. The gateway secret is read by NAME from a K8s Secret (`GITREST_GATEWAY_SECRET`) and never logged, never placed in the Deployment manifest and never set as an env var; the client itself is built lazily on the first claim. `off` reads no secret and builds no client. The client keeps a git-rest transport of its own because leased claim refs are written through git-rest even though task state is now read from the in-memory task view — an intended exception to the change that removed the executor's git-rest reader.

## v0.26.3

- fix: **the task view stops retaining a task's body once that task reaches a terminal phase.** A body is held only so a spawn can render the agent's task file, and a terminal task is never spawned again, so keeping it was pure cost: at 15,000 tasks the bodies are ~88% of the view's bytes and put the live heap near 105 MiB, which the default GC target doubles past a 256Mi container limit — v0.26.0 therefore OOM-killed every cold start at ~100 s and never reached ready on nuke dev. Retaining bodies only for tasks that can still spawn takes the same 15,000-task view to ~40 MiB. Frontmatter is always retained, since the skip decisions read it, and a reopened task publishes a new event that carries its body back, so nothing is lost. The reconcile floor now also refuses a task at a terminal phase: it previously gated only on the Config's `trigger.phases`, so a Config listing `human_review` or `done` — both documented as valid values — could drive a terminal task from the view snapshot and render an empty task file for the agent.

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
