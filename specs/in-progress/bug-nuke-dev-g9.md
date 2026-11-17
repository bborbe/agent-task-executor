---
status: approved
approved: 2026-11-17T11:05:09.569136109Z
kind: bug
---

## Summary

Sentry issue `NUKE-DEV-G9` is a real bug in `bborbe/agent-task-executor` at `main.go:241`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

exitErrorForStatus (main.go:235-249) returns a non-nil error for a failed agent Result carrying only status and phase and discarding result.Message, so all eight execution-phase failure paths collapse into one undifferentiated, untriageable Sentry issue.

## Reproduction

The implicated source location from the deep verdict is `main.go:241` in `bborbe/agent-task-executor` (resolution rule: `candidate position 3 (bborbe/agent-task-executor)`), Sentry issue `NUKE-DEV-G9`.

## Recommended Fix

Rebase and merge the existing tested branch origin/sentry-fix/nuke-dev-g9 (af18439) to thread result.Message into the error, attach phase/status error data, and add the per-phase Sentry fingerprint; the minimum viable change is the one-line message threading at main.go:241.
