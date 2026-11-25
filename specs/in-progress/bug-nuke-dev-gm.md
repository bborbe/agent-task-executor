---
status: approved
approved: 2026-11-25T01:41:56.239113409Z
kind: bug
---

## Summary

Sentry issue `NUKE-DEV-GM` is a real bug in `bborbe/agent-task-executor` at `main.go:311`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

The nuke-dev agent-task-executor Deployment's KAFKA_BROKERS points at the in-cluster Strimzi Kafka ClusterIP 10.43.21.228:9092, which has no ready endpoints; at startup Sarama exhausts Metadata.Retry.Max and returns ErrOutOfBrokers, main.go:311 wraps it as 'create sarama client', service.Main exits non-zero, and the pod CrashLoopBackOffs, re-emitting the event on every restart.

## Reproduction

The implicated source location from the deep verdict is `main.go:311` in `bborbe/agent-task-executor` (resolution rule: `candidate position 3 (bborbe/agent-task-executor)`), Sentry issue `NUKE-DEV-GM`.

## Recommended Fix

Set the nuke-dev executor Deployment's KAFKA_BROKERS to the external nodeports 192.168.178.41:32159,192.168.178.42:32159,192.168.178.43:32159 in its Helm values source of truth and roll the Deployment; correct the stale nuke-dev.env reference in docs/deployment-kafka-brokers.md. Optional hardening: bounded startup retry or a distinct 'Kafka unreachable - check KAFKA_BROKERS' message.
