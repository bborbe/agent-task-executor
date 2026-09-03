# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

- feat: add a periodic reconcile loop that re-derives running tasks from the vault (via git-rest) and live Jobs, so a task deferred behind `maxConcurrentJobs` whose in-memory deferral was lost across an executor restart resumes without a Kafka event or vault edit; observable via `executor_reconcile_redriven_total` and `event=reconcile*` log lines, with the per-assignee spawn lock taken unconditionally to prevent reconcile/event double-spawns
- docs: document the reconcile loop env config (`GITREST_URL`, `GITREST_GATEWAY_SECRET`, `TASK_GLOB`) in README.md, including the ServiceAccount `get` permission required for the git-rest gateway Secret (data key `gateway-secret`)

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
