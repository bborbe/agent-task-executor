---
status: prompted
approved: "2026-11-19T01:37:13.655293511Z"
generating: "2026-11-19T02:43:58Z"
prompted: "2026-11-19T02:50:27Z"
---

## Summary

Sentry issue `NUKE-DEV-GM` is a real bug in `bborbe/agent-task-executor` at `main.go:311`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

nuke-dev agent-task-executor Deployment's KAFKA_BROKERS resolves to the dangling in-cluster Strimzi ClusterIP 10.43.21.228:9092 (no ready endpoints), so libkafka.CreateSaramaClient returns Sarama ErrOutOfBrokers and application.Run exits non-zero at main.go:311.

## Reproduction

The implicated source location from the deep verdict is `main.go:311` in `bborbe/agent-task-executor` (resolution rule: `candidate position 3 (bborbe/agent-task-executor)`), Sentry issue `NUKE-DEV-GM`.

## Recommended Fix

Set KAFKA_BROKERS in the nuke-dev executor Helm values/overlay to 192.168.178.41:32159,192.168.178.42:32159,192.168.178.43:32159, redeploy, verify startup, then remove the dangling Strimzi Service per docs/deployment-kafka-brokers.md.
