---
status: approved
approved: 2026-11-27T07:00:31.896629379Z
kind: bug
---

## Summary

Sentry issue `NUKE-DEV-H1` is a real bug in `bborbe/agent-task-executor` at `pkg/handler/token_ceiling.go:87`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

The tokenCeiling admission gate calls vault-api ReadForAssignee (pkg/claim/vault_api_index_client.go:177) on the spawn path; a vault-api dial failure is returned unmodified through pkg/handler/token_ceiling.go:87 and task_event_handler.go:981-988, so ConsumeMessage treats an advisory budget read as fatal and restarts the executor for every assignee.

## Reproduction

The implicated source location from the deep verdict is `pkg/handler/token_ceiling.go:87` in `bborbe/agent-task-executor` (resolution rule: `candidate position 3 (bborbe/agent-task-executor)`), Sentry issue `NUKE-DEV-H1`.

## Recommended Fix

Fail open in tokenSpendSince/deferIfOverTokenCeiling — on ReadForAssignee/ReadDocument error log at warn, increment a token_ceiling_read_failed metric, and return (false, nil) so the spawn is admitted and reconcile re-evaluates; add a regression test asserting ConsumeMessage returns nil when ReadForAssignee fails.
