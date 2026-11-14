---
status: approved
approved: 2026-11-14T16:12:42.834344826Z
kind: bug
---

## Summary

Sentry issue `NUKE-DEV-FM` is a real bug in `bborbe/agent-task-executor` at `main.go:315`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

The nuke-dev agent-task-executor Deployment uses the in-cluster Kafka default 10.43.21.228:9092 (a k3s ClusterIP with no ready endpoints) instead of the external nodeport brokers, so libkafka.NewSyncProducerFromSaramaClient fails at startup (main.go:315) with Sarama ErrOutOfBrokers and the pod crash-loops.

## Reproduction

The implicated source location from the deep verdict is `main.go:315` in `bborbe/agent-task-executor` (resolution rule: `candidate position 3 (bborbe/agent-task-executor)`), Sentry issue `NUKE-DEV-FM`.

## Recommended Fix

Set the nuke-dev executor Deployment's KAFKA_BROKERS to 192.168.178.41:32159,192.168.178.42:32159,192.168.178.43:32159 (per bborbe/trading/nuke-dev.env), redeploy, and remove the dangling strimzi Service after confirming the current env value.
