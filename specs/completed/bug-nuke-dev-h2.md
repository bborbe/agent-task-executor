---
status: completed
approved: "2026-11-21T02:09:28.353592256Z"
generating: "2026-11-21T02:29:04Z"
prompted: "2026-11-21T02:38:54Z"
verifying: "2026-11-21T03:08:13Z"
completed: "2026-11-21T03:08:18Z"
---

## Summary

Sentry issue `NUKE-DEV-H2` is a real bug in `bborbe/agent-task-executor` at `pkg/handler/token_ceiling.go:41`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

A transient vault-api dial failure is returned unmodified by the best-effort token-ceiling gate (tokenSpendSince -> deferIfOverTokenCeiling) and propagates through the deferred-respawn spawn funnel to a fatal run.Func error that terminates the executor process, contradicting the repo's documented fail-open index-read contract.

## Reproduction

The implicated source location from the deep verdict is `pkg/handler/token_ceiling.go:41` in `bborbe/agent-task-executor` (resolution rule: `candidate position 3 (bborbe/agent-task-executor)`), Sentry issue `NUKE-DEV-H2`.

## Recommended Fix

Fail open in the token-ceiling gate on vault-api read errors (log event=token_ceiling_read_error, bump a {result="token_ceiling_read_error"} metric, return false,nil) and treat transient vault-read errors in evalDeferredRespawns like spawnJobError (log, count, skip/re-park) instead of returning fatal; add regression tests for both.
