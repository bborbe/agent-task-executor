# Changelog

All notable changes to this project will be documented in this file.

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
