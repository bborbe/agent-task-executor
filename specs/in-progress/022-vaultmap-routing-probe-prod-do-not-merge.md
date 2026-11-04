---
status: approved
tags:
    - dark-factory
    - spec
approved: "2026-11-04T15:25:00Z"
---

## Summary

- Adds one documentation page recording which agent vault this repository is routed to, so the mapping is written down where the code lives instead of only in the deploy repo.
- The page is prose only. No Go code, no Makefile target, no configuration key, no test changes.
- This is a throwaway routing probe. The pull request carrying it is expected to be closed unmerged once the routing observation is recorded.

## Problem

`target_vault` on a dark-factory `CreateTaskCommand` is derived per repository from the watcher's `REPO_VAULT_MAP`. Nothing in this repository records which vault it maps to, so a reader of the code cannot tell that work on this repo is expected to land in the `personal` vault rather than the default `openclaw` one.

## Goal

A short page under `docs/` states this repository's vault mapping and the default that applies to unmapped repositories. Nothing else changes.

## Non-goals

- Any behavioural change. This is documentation only.
- Restating the watcher's parsing rules; the deploy repo and the watcher README own those.

## Acceptance Criteria

- [ ] A page exists under `docs/` naming this repository's vault mapping as `personal`, and stating that an unmapped repository defaults to `openclaw`.
- [ ] No other file is modified.
