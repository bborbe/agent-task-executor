---
status: completed
approved: "2026-11-19T05:40:16.298221493Z"
generating: "2026-11-19T06:34:37Z"
prompted: "2026-11-19T06:40:47Z"
verifying: "2026-11-19T06:57:52Z"
completed: "2026-11-19T06:57:59Z"
---

## Summary

Sentry issue `NUKE-PROD-CV` is a real bug in `bborbe/agent-task-executor` at `main.go:235`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

exitErrorForStatus (main.go:235, called from finishRun at main.go:218) is the intended non-zero exit for a failed agent run, but it drops result.Message, collapsing all 141 planning-phase failures into one opaque Sentry event whose upstream cause (claude-auth probe, GH-token preflight, or transient worktree/PR IO) cannot be identified.

## Reproduction

The implicated source location from the deep verdict is `main.go:235` in `bborbe/agent-task-executor` (resolution rule: `candidate position 3 (bborbe/agent-task-executor)`), Sentry issue `NUKE-PROD-CV`.

## Recommended Fix

Pass result.Message into exitErrorForStatus and include it in the error text (with a test case), so distinct planning failures produce distinct diagnosable Sentry events; separately decide whether this expected non-zero exit should remain at Error level.
