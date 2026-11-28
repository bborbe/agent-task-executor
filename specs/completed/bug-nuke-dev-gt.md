---
status: completed
approved: "2026-11-28T00:14:11.589816469Z"
generating: "2026-11-28T00:34:13Z"
prompted: "2026-11-28T00:54:06Z"
verifying: "2026-11-28T01:18:33Z"
completed: "2026-11-28T01:18:40Z"
---

## Summary

Sentry issue `NUKE-DEV-GT` is a real bug in `bborbe/agent-task-executor` at `pkg/handler/task_event_handler_deferred.go:356`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

isJobLookupDenied only tolerates 401/403, so a transient kube-apiserver 503 ("apiserver not ready") during a deferred-respawn tick propagates out of evalDeferredRespawns, RunDeferredRespawnLoop returns the tick error, service.Run ends and the executor pod restarts.

## Reproduction

The implicated source location from the deep verdict is `pkg/handler/task_event_handler_deferred.go:356` in `bborbe/agent-task-executor` (resolution rule: `candidate position 3 (bborbe/agent-task-executor)`), Sentry issue `NUKE-DEV-GT`.

## Recommended Fix

Classify transient API-server failures (IsServiceUnavailable, IsTimeout/IsServerTimeout, IsTooManyRequests, IsInternalError, net.Error, context deadline) as a distinct non-fatal job-lookup error and log-and-continue in evalDeferredRespawns, plus defence-in-depth log-and-continue on non-ctx tick errors in RunDeferredRespawnLoop.
