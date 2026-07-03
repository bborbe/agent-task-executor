# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

- feat: add explicit `TopicPrefix` config (`arg:"topic-prefix"` / `env:"TOPIC_PREFIX"`), replacing the implicit
  `Branch`-derived Kafka topic prefix. `Branch` (`env:"BRANCH"`) is unchanged and keeps its non-topic uses
  (child-job `BRANCH` env propagation, config image tagging, stage matching). Bumps
  `github.com/bborbe/agent` to v0.72.0 and `github.com/bborbe/cqrs` to v0.6.0.
