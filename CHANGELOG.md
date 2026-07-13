# Changelog

All notable changes to this project will be documented in this file.

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
