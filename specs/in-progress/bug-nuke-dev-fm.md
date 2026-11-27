---
status: approved
approved: 2026-11-27T06:43:27.047110482Z
kind: bug
---

## Summary

Sentry issue `NUKE-DEV-FM` is a real bug in `bborbe/agent-task-executor` at `main.go:317`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

The live nuke-dev agent-task-executor Deployment still sets KAFKA_BROKERS to the dangling in-cluster Strimzi bootstrap Service (10.43.21.228:9092, zero ready endpoints), so Sarama returns ErrOutOfBrokers and the binary exits at startup; committed config is already correct and the documented operator fix was never applied.

## Reproduction

The implicated source location from the deep verdict is `main.go:317` in `bborbe/agent-task-executor` (resolution rule: `candidate position 3 (bborbe/agent-task-executor)`), Sentry issue `NUKE-DEV-FM`.

## Recommended Fix

Set KAFKA_BROKERS to the external nodeport list (192.168.178.41:32159,192.168.178.42:32159,192.168.178.43:32159) in bborbe/trading-lib/nuke-dev.env (mirrored by nuke/agent/values-dev.yaml), re-render/roll the Deployment and verify Ready with no ErrOutOfBrokers, then remove the dangling Strimzi Service last.
