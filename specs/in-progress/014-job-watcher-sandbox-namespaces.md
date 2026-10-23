---
status: verifying
approved: "2026-10-23T16:32:53Z"
generating: "2026-10-23T16:38:33Z"
prompted: "2026-10-23T16:52:07Z"
verifying: "2026-10-23T18:27:36Z"
branch: dark-factory/job-watcher-sandbox-namespaces
---

## Summary

- The Job watcher watches one namespace today: the executor's own. Jobs spawned into a sandbox namespace are invisible to it, so their terminal state produces no synthetic failure result and no store cleanup.
- The watcher gains the Config provider and derives the namespace set from it: the executor's namespace plus every distinct `JobNamespace` a Config names.
- One informer per namespace, all started under the same context, each namespace gated on its own cache sync.
- The set is re-resolved on an interval, so a Config that starts naming a new namespace is watched without an executor restart.
- No new RBAC and no change to Job spawning — this spec only widens what the watcher observes.

## Problem

Spec 002 (`specs/in-progress/002-config-job-namespace.md`) lets a Config place its Jobs in a sandbox namespace, and its `## Follow-ups` names this gap as the thing it deliberately left out: `pkg/job_watcher.go` builds its shared informer with `WithNamespace(string(w.namespace))`, so a Job created in `dev-agents-sandbox` is never seen.

The consequence is not cosmetic. The watcher is the component that publishes a synthetic failure when a Job dies without the agent having published a result, and that evicts the task's entry from the `TaskStore`. With the Job out of view, a failed sandbox Job leaves its task stuck with a `current_job` pointing at a dead Job and nothing reporting the failure. The zombie sweeper is the only remaining detector, and it fires on a much longer horizon.

The parent goal's criterion — "a failed Job in `<env>-agents-sandbox` produces the same failure result and store cleanup as one in the executor's namespace" — cannot be met while the informer is namespace-scoped to the executor.

## Goal

The executor observes terminal Job and Pod events in every namespace its Configs name, and handles them exactly as it handles its own: same synthetic failure publication, same store cleanup, same pod-state classification. Flipping `JobNamespace` on a Config becomes safe with respect to failure reporting.

## Non-goals

- **A cluster-scoped informer**, and the RBAC it would need. A `ClusterRoleBinding` already grants the executor cluster-wide Job/Pod access on nuke dev, so a cluster-scoped informer would work today — it is rejected because it makes every namespace the executor's business and because spec 002 forbids leaning on that grant. The namespace set is derived from the Configs instead.
- **A static namespace list rendered by the `bborbe/agent` chart**, the remedy spec 002's own follow-up proposed. Rejected: it puts the answer in a second repo that must be kept in sync with the Configs, and it cannot reflect a Config flip without a chart change and a pod roll.
- Watching Configs across namespaces — Configs stay in the executor's namespace
- Changing Job spawning, namespace resolution semantics, or the CRD
- Prod verification — the change is cluster-agnostic; the prod read belongs to the nuke fleet-move task
- Detecting a namespace that a Config stopped naming — an informer, once started, runs until process exit

## Assumptions

- **Configs are operator-authored and version-controlled** (the `bborbe/agent` helm chart). This is the entire trust boundary for letting the Config set decide what the executor watches; it is the same assumption spec 002 relies on for `jobNamespace` itself.
- **The executor already holds list/watch RBAC in every namespace it will be told to watch.** Verified on nuke dev 2026-10-23: `kubectlnukedev -n dev-agents-sandbox auth can-i list jobs --as=system:serviceaccount:dev:agent-task-executor` returns `yes`, as does `watch pods`, via the Helm-managed `agent-task-executor-dev` ClusterRoleBinding. This spec requests no grant and does not widen the executor's authority.
- **`spec.jobNamespace` is released but not yet deployed on dev.** Spec 002 shipped in v0.31.0 and `docs/agent-crd-specification.md` documents the field including its rejection rule; dev still runs v0.30.9, whose CRD has no `jobNamespace` property. It appears only with the v0.32.0 deploy, so the disposable `watcher-e2e` Config may be created only *after* that deploy — created earlier, its unknown field is pruned, the Job lands in `dev`, and AC15 would pass vacuously.
- **The namespace set is small** (single digits). Informer memory is linear in it, and the set is bounded by the operator-authored Config count.

## Acceptance Criteria

- [ ] **AC1.** `make precommit` exits 0
- [ ] **AC2.** The watcher receives the Config provider — `awk '/^func CreateJobWatcher/,/^}/' pkg/factory/factory.go | grep -c 'Provider\[agentv1.Config\]'` returns ≥1, and `grep -c 'Provider\[agentv1.Config\]' pkg/job_watcher.go` returns ≥1. The `awk` scoping is what discriminates: `factory.go` already carries a `Provider[agentv1.Config]` parameter on `CreateServiceReconcileLoop`, which would satisfy an unscoped grep without touching the watcher.
- [ ] **AC3.** The namespace set is derived from the Configs, not hand-listed — `TestJobWatcherNamespaces` covers four fixtures: (a) executor `dev` with Configs naming `""`, `dev-agents-sandbox`, `dev-agents-sandbox` (duplicate) and `dev-other` → exactly `{dev, dev-agents-sandbox, dev-other}`; (b) the same Configs with the empty one removed → `dev` is **still** in the set, proving the executor namespace is included unconditionally rather than only via the empty-value rule; (c) no Configs at all → `{dev}`; (d) a Config whose `jobNamespace` equals `dev` → that Config is skipped and logged, `Run` does **not** fail, and the set is still `{dev}`. Case (d) is the error policy: `ResolveJobNamespace` rejects that value, and a single misconfigured Config must not stop the watcher for every other namespace. `go test ./... -run TestJobWatcherNamespaces -v` stdout contains `--- PASS: TestJobWatcherNamespaces`
- [ ] **AC4.** **A terminal-failed Job in a non-executor namespace produces both effects.** `TestJobWatcherTerminalFailureAcrossNamespaces` configures two namespaces and delivers a terminal-failed Job in the non-executor one; it asserts `PublishFailure` was called exactly once with that Job's name **and** that the task's `TaskStore` entry is gone. Asserting only the publish call is the lazy implementation this AC exists to exclude. `go test ./... -run TestJobWatcherTerminalFailureAcrossNamespaces -v` stdout contains `--- PASS`
- [ ] **AC5.** **Coverage scales with the Config set, not with a hardcoded two.** `TestJobWatcherThreeNamespaces` configures three Configs naming three distinct namespaces and delivers one terminal-failed Job in each; all three publish and all three store entries are removed. A two-namespace implementation passes AC4 and fails this one. `go test ./... -run TestJobWatcherThreeNamespaces -v` stdout contains `--- PASS`
- [ ] **AC6.** A namespace no Config names is not watched — `TestJobWatcherIgnoresUnconfiguredNamespace` delivers a terminal-failed Job in a namespace absent from the Config set and asserts `PublishFailure` was called zero times and the store entry is still present. Negative evidence: publish call count is 0. This is also the behavioural proof that no cluster-scoped informer was used — a cluster-scoped one would observe the unconfigured namespace. `go test ./... -run TestJobWatcherIgnoresUnconfiguredNamespace -v` stdout contains `--- PASS`
- [ ] **AC7.** **A wiring failure fails loudly; an environmental one does not.** `TestJobWatcherFailsLoudly` covers two cases: (a) the Config provider errors at startup, and (b) constructing one namespace's informers or adding its event handlers returns an error. In both, `Run` returns a non-nil error rather than running with only the executor's namespace. Negative evidence: in neither case does `Run` return nil. **Out of scope for this test:** a list/watch error during cache sync (a missing namespace, a 403) — DB3 makes that retry, never fail the run. `go test ./... -run TestJobWatcherFailsLoudly -v` stdout contains `--- PASS`
- [ ] **AC8.** **A namespace added to the Config set mid-run is watched without a restart.** `TestJobWatcherReresolvesNamespaceSet` starts the watcher against one Config, adds a second Config naming a new namespace, and advances the re-resolution tick; it asserts exactly one new informer was started (call count 1) and that the already-watched namespace was not restarted (call count 0). The tick interval is a package-level `var` overridden by the test — never a 60-second sleep and never an injected clock dependency. Without this AC the laziest passing implementation resolves the set once at startup and never notices a Config flip. `go test ./... -run TestJobWatcherReresolvesNamespaceSet -v` stdout contains `--- PASS`
- [ ] **AC9.** **No event is handled before its own namespace has synced.** `TestJobWatcherAwaitsCacheSync` delivers a terminal-failed Job in a namespace whose cache has not synced and asserts `PublishFailure` was called zero times and the store entry is still present; it then delivers the same event in a *different*, already-synced namespace and asserts that one publishes — proving the barrier is per namespace, not global. Negative evidence: the pre-sync call count for the unsynced namespace is 0. Without this AC a lazy implementation skips the sync barrier entirely and still passes every other AC. `go test ./... -run TestJobWatcherAwaitsCacheSync -v` stdout contains `--- PASS`
- [ ] **AC10.** The Pod lister follows the Job's namespace — `TestPodListerPerNamespace` asserts the lister handed to `terminalPodDetail` and to the zombie sweeper is the one for the Job's (or task's) namespace, and that a namespace whose cache has not synced yields `nil` rather than another namespace's lister. It also covers the sweeper's fallback: a task whose assignee matches no Config resolves to the executor's namespace, which is the behaviour the sweeper has today. A single ambiguous lister would silently read "not synced" for every namespace but one, and the sweeper would skip every tick. `go test ./... -run TestPodListerPerNamespace -v` stdout contains `--- PASS`
- [ ] **AC11.** **Anti-empty-test guard for AC3–AC10** — an empty test body still prints `--- PASS`, so the named tests must carry real assertions. `awk '/^func Test/{t=$2; sub(/\(.*/,"",t)} /(Fatalf|Errorf|require\.|assert\.|Expect\()/{c[t]++} END{for(k in c) print k, c[k]}' pkg/job_watcher_namespaces_test.go` lists all eight test names, each with a count ≥ 2. A `grep -c` line count is not used: comments and identifier mentions would satisfy it.
- [ ] **AC12.** `CHANGELOG.md` carries the change, and the bullet survives the release cut — `awk '/^## /{sec=$0} /^- feat: / && /sandbox namespace/{print sec; exit}' CHANGELOG.md` prints a heading matching `^## (Unreleased|v[0-9]+\.[0-9]+\.[0-9]+)$`. The bullet must begin with `- feat: ` (the release process bumps minor on `feat:` — v0.30.9 → v0.31.0 — so a `feat:` bullet yields the next minor tag) and contain the exact phrase `sandbox namespace`. Asserting "under `## Unreleased`" would be unsatisfiable at verification time: the release cut renames that heading to the new tag before the Post-Deploy rung runs.
- [ ] **AC13.** The observation surface is recorded where it outlives this spec — `grep -ci 'from the Configs' docs/agent-crd-specification.md` returns ≥1 **and** `grep -ci 'until restart' docs/agent-crd-specification.md` returns ≥1. Two greps rather than one because the two facts may legitimately sit in different sentences.
- [ ] **AC14.** No RBAC manifest was added — `git diff --name-only origin/master | grep -ciE 'rbac|clusterrole|rolebinding'` prints 0. Read the number, not the exit status: `grep -c` exits 1 on no match. This repo ships no manifests, so the guard is that nothing new of that shape enters the diff; the RBAC that matters is in the quant config repo and is observed by the operator rung below.
- [ ] **Post-Deploy (Rung-2):** **AC15.** a deliberately failing agent Job in `dev-agents-sandbox` reaches a failed task result on nuke dev. Setup, in this order: the v0.32.0 deploy lands first, because only then does the CRD carry `jobNamespace`; then a disposable Config `watcher-e2e` in `dev` with `spec.jobNamespace: dev-agents-sandbox`, `spec.assignee: watcher-e2e`, and an image whose entrypoint exits non-zero immediately; then a task assigned to `watcher-e2e` is created. The read is taken, and the Config is deleted afterwards. Creating the Config before the deploy makes this AC pass vacuously in the executor's own namespace. Evidence: that task file gains a `## Failure` section naming the sandbox Job (`**Timestamp:**`, `**Job:**`, `**Reason:**`), its `trigger_count` frontmatter increments by exactly 1 from its pre-run value of 0, and `current_job` is cleared.
  - `deploy_check:` `kubectlnukedev -n dev get deploy/agent-task-executor-personal -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $NF}'`
  - `deploy_target:` `v0.32.0` — the tag the `- feat:` bullet implies; the operator replaces it with the tag actually deployed.

## Verification

### Container-executable (runs inside the YOLO container at prompt time)

```bash
make precommit
go test ./... -run 'TestJobWatcherNamespaces|TestJobWatcherTerminalFailureAcrossNamespaces|TestJobWatcherThreeNamespaces|TestJobWatcherIgnoresUnconfiguredNamespace|TestJobWatcherFailsLoudly|TestJobWatcherReresolvesNamespaceSet|TestJobWatcherAwaitsCacheSync|TestPodListerPerNamespace' -v
awk '/^func Test/{t=$2} /(Fatalf|Errorf|require\.|assert\.|Expect\()/{c[t]++} END{for(k in c) print k, c[k]}' pkg/job_watcher_namespaces_test.go
awk '/^## /{sec=$0} /^- feat: / && /sandbox namespace/{print sec; exit}' CHANGELOG.md
git diff --name-only origin/master | grep -ciE 'rbac|clusterrole|rolebinding'
```

### Operator-executable (runs on the host after PR merge, spec verification ladder)

```bash
# build + push the image for the merged tag, deploy dev per "Deploy Mirrored Agent Service"
kubectlnukedev get ns dev-agents-sandbox -o name
kubectlnukedev -n dev get configs.agent.benjamin-borbe.de -o jsonpath='{range .items[*]}{.metadata.name}={.spec.jobNamespace}{"\n"}{end}'
kubectlnukedev -n dev-agents-sandbox auth can-i list jobs --as=system:serviceaccount:dev:agent-task-executor
kubectlnukedev -n dev-agents-sandbox auth can-i watch pods --as=system:serviceaccount:dev:agent-task-executor
```

The Config dump is the before/after probe for "no live Config's `JobNamespace` was flipped": it must be identical before and after the deploy, with the disposable `watcher-e2e` Config deleted before the after-read. Both `auth can-i` reads must return `yes` **before** the deploy, proving the mechanism needs no new grant — they are the observation point for the no-new-RBAC claim, which cannot be asserted from this repo's diff because the RBAC lives in the quant config repo.

## Desired Behavior

1. `CreateJobWatcher` takes a `libk8s.Provider[agentv1.Config]` alongside the executor namespace. The provider is the same `eventHandlerConfig` value `main` already hands to `CreateServiceReconcileLoop` and to the zombie sweeper.
2. The watched namespace set is `{executorNamespace} ∪ {ResolveJobNamespace(ctx, executorNamespace, cfg.Spec.JobNamespace) for every Config}`. The executor's namespace is a member unconditionally, so its own Jobs are watched exactly as today; an empty `Spec.JobNamespace` contributes it too. Duplicates collapse. A Config whose `jobNamespace` fails resolution (it equals the executor namespace) is skipped with an ERROR naming the Config and the rejected value — it spawns no Jobs either, so skipping it costs nothing and a single bad Config must not stop the watcher for every other namespace.
3. One informer factory is built per namespace, each with the existing 5-minute resync and the `agent.benjamin-borbe.de/task-id` label selector, all started under the same context. **The sync barrier is per namespace**: a namespace's handlers begin processing only once that namespace's Job and Pod caches have synced, and a namespace that never syncs simply never handles events — the healthy namespaces keep working throughout. Two failure classes are distinguished. An error **constructing** a namespace's informers or adding its event handlers fails the run: that is a wiring defect, and running on a subset would be the silent-subset failure this spec removes. A list/watch error **during** sync — a missing namespace, a 403 — is logged at ERROR and retried by client-go indefinitely, and never makes `Run` return: it is an environment condition an operator fixes, and killing the executor for every namespace over one bad Config is the worse outcome. A failure to list the Configs at startup fails the run. The gate lives in `HandleJob`/`HandlePod` themselves, not only in the informer wiring: an event whose namespace is not in the watched set, or whose namespace has not synced, is ignored there. That is what makes AC6 and AC9 testable by calling the handler directly, and it is why the existing direct-call tests must mark their namespace synced.
4. The set is re-resolved on a 60-second interval. A namespace not yet watched gets a new informer started then; an already-watched namespace is left alone. The interval is a package-level variable, not configuration and not an injected clock, so a test overrides it directly. The consumer that makes this necessary: the nuke fleet-move task's Step 4 flips `JobNamespace` on a Config with `kubectl`/helm and does not restart the executor, so without a tick the flip would go unwatched until the next unrelated deploy. A namespace that disappears from the Config set keeps its informer until process exit — bounded because the set is operator-authored and small, and stopping an informer could drop terminal events for Jobs still terminating there. The tick mutates the informer map; it takes the same lock the handlers take when resolving a namespace's lister, so a handler never reads a half-updated set.
5. `PodLister` becomes namespace-scoped: it returns the lister for the requested namespace, or `nil` when that namespace's cache has not synced. Both consumers follow the Job or task's namespace — `terminalPodDetail` uses `job.Namespace`, and the zombie sweeper resolves the task's namespace from the Configs it already fetches each tick, falling back to the executor's namespace when the task's assignee matches no Config (today's behaviour, preserved). When two Configs share an assignee, the sweeper keeps today's rule — the first match wins — so the tie-break is unchanged rather than newly ambiguous. The resulting observation surface is recorded in `docs/agent-crd-specification.md`.
6. Per-namespace terminal handling is unchanged: the same `PublishFailure` call, the same `DeleteIfCurrentJob` guard, the same pod-state classification. Only the set of namespaces it applies to changes.

## Constraints

- No new RBAC of any kind; the origin spec's constraint stands, and the existing `agent-task-executor-dev` ClusterRoleBinding is not leaned on
- The executor's own namespace stays watched unconditionally, so behaviour for every Config that does not set `JobNamespace` is byte-identical
- `JobWatcher`'s interface changes (`PodLister` gains a namespace argument); the counterfeiter fake in `mocks/` and the test doubles in `pkg/handler/*_test.go` ripple with it
- The existing direct-call tests (`pkg/job_watcher_test.go`, `pkg/job_watcher_internal_test.go`) must be updated to mark their namespace watched and synced, and must keep passing — the handler-path gate is a new precondition on every direct `HandleJob`/`HandlePod` call
- Spawning, `ResolveJobNamespace`, the CRD, and the Kafka publishing path are untouched
- `docs/agent-crd-specification.md` remains the source for `jobNamespace` semantics; this spec adds only the observation-surface note and must not change the field's rules
- A wiring failure at startup fails the run; an environmental one (missing namespace, missing RBAC) is logged and retried, never fatal

## Failure Modes

| Trigger | Expected behavior | Detection | Recovery | Reversibility |
|---|---|---|---|---|
| The Config provider fails at startup | `Run` returns an error; the service exits and restarts, exactly as any other boot failure | The pod restarts; `kubectl -n dev logs deploy/agent-task-executor-personal --previous` shows the wrapped error naming the provider | None needed — the restart retries; if it persists, `kubectl get configs.agent.benjamin-borbe.de` shows the API error | Full |
| The provider fails during a re-resolution tick | The tick is logged at ERROR and skipped; already-running informers keep watching | The ERROR line naming the tick appears in the pod log | None needed; the next tick retries | Full |
| A Config's `jobNamespace` equals the executor namespace | That Config is skipped with an ERROR naming it and the rejected value; the other namespaces are unaffected; `Run` does not fail | The ERROR line naming the Config appears at boot and on every re-resolution tick | Correct the Config; it spawns no Jobs in this state either, so nothing else is lost | Full |
| Constructing one namespace's informers returns an error | `Run` returns an error; the service restarts — a wiring defect, not an environment condition | The pod restarts and the wrapped error names the namespace | Fix the wiring; the defect is deterministic, so the restart loop is the signal | Full |
| A Config names a namespace that does not exist | That namespace's cache never syncs, so it never handles events; the other namespaces keep handling. `Run` does **not** return | The informer's list error appears in the pod log with the namespace name; the other namespaces' start lines are present | Create the namespace (`kubectl get ns <name>` then returns `Active`); the informer syncs and starts handling | Full |
| A Config names a namespace the executor lacks RBAC for | Same as above — that namespace never syncs; the others are unaffected; `Run` does not return | List/watch `403` lines in the pod log naming the namespace | Operator grants the RBAC in the quant config repo; confirm with `kubectl auth can-i list jobs -n <ns>`. Outside this project's write scope | Full |
| A namespace's cache never syncs | That namespace handles nothing, indefinitely; no other namespace is affected and the process stays up | The namespace's start line is absent from the pod log while others are present | Treat as one of the two rows above (missing namespace or missing RBAC) | Full |
| The 60-second tick races a handler resolving a lister | Both take the same lock; a handler sees either the old or the new set, never a partial one | Not observable in normal operation; `ENABLE_RACE=true make test` covers it — the repo's default is `-race=false` | None needed | Full |
| The Config provider returns an empty set at boot | The provider is populated asynchronously by the connector's listen loop, so the executor may start having seen no Configs. The set is then `{executorNamespace}`; sandbox namespaces join on the first 60-second tick, and a terminal Job already present is picked up by that informer's next 5-minute resync — the per-namespace sync barrier drops events delivered during the initial LIST, so a Job whose TTL expires before that resync is missed, which is why the TTL must exceed the resync period | The start log names only the executor's namespace; the new namespaces' start lines appear within 60 s | None needed — the tick recovers it. A Job whose TTL expired before the first tick is missed, which is why the TTL exceeds the tick interval | Partial — a terminal event older than the Job's TTL is lost |
| The API server throttles list/watch | The informers' own client-go backoff retries; no custom handling is added | `client-go` rate-limiter warnings in the pod log | None needed | Full |
| The process crashes mid-re-resolution | On restart the set is derived from scratch; already-watched namespaces are re-watched, and no state is lost because none is persisted | The pod restart count increments | None needed | Full |
| Two Configs name the same namespace | One informer, deduplicated by the set | None — visible only as the informer count in the start log | None needed | Full |
| A namespace is added to the Config set mid-run | Watched from the next 60-second tick | The start log line for the new namespace appears within 60 s of the Config write | None needed | Full |
| A namespace is removed from the Config set | Its informer keeps running until process exit | The informer count in the start log never decreases | Restart the executor if the namespace must stop being watched | Full |
| The namespace count grows without bound | Memory grows with one informer set per namespace | Process RSS against the informer count | None — the set is operator-authored and small; revisit if Configs become externally authored | Full |

## Suggested Decomposition

| # | Prompt focus | Covers DBs | Covers ACs | Depends on |
|---|---|---|---|---|
| 1 | Provider in the watcher, namespace-set derivation with the skip-and-log error policy, per-namespace informer construction and start, per-namespace sync barrier, wiring-vs-environment failure split + unit tests | 1, 2, 3 | 2, 3, 4, 5, 6, 7, 9, 11 | — |
| 2 | The 60-second re-resolution tick, its lock, and the package-level interval variable + unit test | 4 | 8, 11 | 1 |
| 3 | Namespace-scoped `PodLister`, both consumers incl. the assignee-miss fallback, the counterfeiter and test-double ripple + unit tests | 5 | 10, 11 | 1 |
| 4 | CHANGELOG entry and the observation-surface note in `docs/agent-crd-specification.md` | 6 | 12, 13 | 1 |

Rationale: row 1 is the core surface — derivation, informers, the per-namespace barrier and the failure split — and is deliberately kept as one prompt so the laziest implementation cannot drop half of it; the tick is split out because it is a separate loop with its own lock and a distinct rejection already recorded in Non-goals. The lister is a third surface that touches the zombie sweeper and every test double; it is deliberately not split into its own spec because a namespace-scoped lister has no standalone value until the watcher watches more than one namespace. AC1 (`make precommit`), AC11 (the anti-empty-test guard), AC14 (no RBAC manifest) and AC15 (Post-Deploy) apply across rows. The 6-DB / 15-AC size is above the guide's 50-cell budget; the decomposition bounds the prompt-creator's research per row, which is what that budget protects, and spec 002 shipped at a comparable ratio.

## Security / Abuse Cases

- **The watcher widens its own observation surface.** It now lists Jobs and Pods in namespaces it did not before. That is bounded by the Config set, which is operator-authored and version-controlled — the same trust boundary spec 002 relies on.
- **No new RBAC is requested.** A namespace the executor cannot list simply fails to sync; the spec does not widen the executor's authority to reach it.
- **A malformed Config could name an unexpected namespace.** The namespace is used only as an informer scope, never as a write target, so the blast radius is read-only observation of objects the executor's own Jobs produce.

## Do-Nothing Option

Flipping `JobNamespace` on any Config leaves failed sandbox Jobs unreported: their tasks keep a `current_job` pointing at a dead Job, no synthetic failure is published, and nothing evicts the store entry. The nuke fleet-move task's Step 4 cannot be run safely, and the parent goal's "Executor sees sandbox Jobs finish" criterion stays unmet.
