---
status: approved
approved: 2026-11-22T10:54:37.890802448Z
kind: bug
---

## Summary

Sentry issue `NUKE-PROD-CV` is a real bug in `bborbe/agent-task-executor` at `main.go:230`, filed automatically by the sentry-fix-agent from the High/High real bug deep verdict — that verdict is the approval gate, so this spec is approved rather than drafted.

## Problem

exitErrorForStatus deliberately returns a non-nil error on Result{Status:failed} (required so the executor does not record a phase success) but formats only the constant status and phase, dropping result.Message and the task identifier, so every execution failure collapses into one undiagnosable Sentry issue.

## Reproduction

The implicated source location from the deep verdict is `main.go:230` in `bborbe/agent-task-executor` (resolution rule: `candidate position 3 (bborbe/agent-task-executor)`), Sentry issue `NUKE-PROD-CV`.

## Recommended Fix

Thread result.Message and the task identifier into the error returned by finishRun/exitErrorForStatus without changing the non-nil contract, so Sentry groups per cause; then triage the dominant underlying failure (likely lifecycle deadline/stall).
