---
status: approved
spec: [025-bug-rearm-clobbered-by-stale-park-reemission]
created: "2026-11-22T09:30:00Z"
queued: "2026-11-22T09:49:41Z"
branch: dark-factory/bug-rearm-clobbered-by-stale-park-reemission
---

# Park-marker protocol and terminal-gate documentation, plus release record

<summary>
- The claim-lease protocol document gains a section describing the durable park marker: its ref shape, sentinel cluster, lifecycle, bound, and cross-cluster semantics.
- The vault-api task-index document gains the rule for the new vault-authoritative terminal gate, its log line, its fail-open direction, and its relationship to the existing `index_terminal` gate.
- The changelog records both halves of the fix as an unreleased bug fix, with the measured evidence.
- Released version sections and any Go code are untouched.
</summary>

<objective>
Keep the protocol knowledge in the repo rather than only in the spec: document the durable park marker in `docs/claim-lease-protocol.md`, document the vault-authoritative `terminal_status` gate in `docs/vault-api-task-index.md`, and add the `## Unreleased` changelog entry describing both fixes.
</objective>

<context>
Read `CLAUDE.md` if present, and `README.md`, for project conventions.

Read the shipped code so the docs describe what actually landed:
- `pkg/claim/park_store.go` — the marker store shipped by prompt 1: `ParkRecordPrefix`, `ParkRecordCluster`, the `Record` / `PublishedAt` methods, the ref shape, the `lease_until` encoding, `keepOnly`.
- `pkg/handler/park_store.go` — the per-pod floor (`parkPublishedAt` / `markParkPublished`) and the `park_marker_read_failed` / `park_marker_write_failed` lines.
- `pkg/handler/terminal_gate_divergence.go` — the gate shipped by prompt 2: `terminalGateAllowsEvaluation`, the `event=terminal_gate_divergence` line, the `event=terminal_gate_index_unavailable` fallback.
- `pkg/handler/vault_index_cache.go` — the one-read-per-pass shared cache.

Read the documents to extend:
- `docs/claim-lease-protocol.md` — § *Parking* (the three park paths and their reason tokens) and § *Streak refs* (the closest existing example of a non-claim ref under `claims/`). There is no park-marker section today.
- `docs/vault-api-task-index.md` — § *Divergence check* and § *Refusal gate: `index_terminal`*. The new gate rule belongs beside the `index_terminal` refusal gate.

Read `CHANGELOG.md` — it opens with `# Changelog`, intro lines, then `## v0.70.1`. There is no `## Unreleased` section today. Follow the existing voice: `- fix: **bold summary.** prose`, one bullet per logical change, naming the measured evidence.

Read these coding guides from the coding plugin at `/home/node/.claude/plugins/marketplaces/coding/docs/`:
- `changelog-guide.md` — entry format, verb style, anti-patterns, `## Unreleased` rules
- `documentation-guide.md` — doc voice and structure

Verified facts (do not re-derive):
- The marker ref shape is `claims/park/<taskid>/<phase>/<completed unix seconds>`; the phase component normalises an empty phase to `none` (`RedriveStreakPhaseComponent`).
- `lease_until` is `publishedAt + retention`; the publish time is recovered as `lease_until − retention`.
- One ref per task is the bound: `Record` deletes the task's strictly older completions, so a superseded completion's success record is deleted by the success store and can no longer re-emit a park.
- The sentinel cluster `park-record` never equals a real executor cluster name, so restart recovery (which filters by cluster) never adopts, fences or releases a park ref — the same mechanism that keeps `claims/success/` inert.
- The terminal gate reads the cached index's status value; a non-terminal vault status overrides a terminal view; a terminal status, an absent entry, or a failed read keeps the view-only refusal.
</context>

<requirements>
1. In `docs/claim-lease-protocol.md`, add a new section (for example `## Park markers`, placed after § *Parking* and before § *Restart recovery*, or beside § *Streak refs*) that documents the durable marker shipped by prompt 1:
   - **Purpose:** records that a park was published for a given successful Job completion, so a restarted pod or a peer cluster suppresses a second publish for that completion; the per-pod map remains a floor.
   - **Ref shape:** `claims/park/<taskid>/<phase>/<completed unix seconds>`, with an empty phase normalised to `none`.
   - **Cluster sentinel:** `park-record`, which never equals a real executor cluster name; state that restart recovery filters by cluster and therefore never adopts, fences or releases a park ref.
   - **Publish time encoding:** `lease_until` is `publishedAt + retention` and the publish time is recovered as `lease_until − retention`; both writer and reader use the same retention.
   - **Bound:** one ref per task; `Record` deletes the task's strictly older completions on write. Explain the coupling: the success store deletes a superseded completion's record on write, so a stale completion can no longer re-emit a park and its deleted marker is never needed. A marker recording only that the task was parked (no completion value) would fail to let a later completion park.
   - **Cross-cluster semantics:** the key is the completion, so two clusters publishing a park for the same completion write the identical key and the second write is a 409 no-op; neither cluster can suppress the other's legitimate first park.
   - **Failure direction:** a failed read falls back to the per-pod map (`event=park_marker_read_failed`); a failed write leaves the park standing and the marker missing, so a later pass may re-publish once (`event=park_marker_write_failed`) — the marker failure never re-enables the task-file write-back.
   - **Claim mode off:** no shared store, in-memory only, unchanged.
   The section must contain the literals `claims/park/`, `park-record`, `park_marker_read_failed`, and `park_marker_write_failed`.

2. In `docs/vault-api-task-index.md`, add a subsection beside § *Refusal gate: `index_terminal`* (for example `### Refusal gate: terminal_status`) that documents the vault-authoritative gate shipped by prompt 2:
   - **Rule:** the reconcile `terminal_status` gate refuses a task whose **view** status is terminal **unless** the same pass's vault index reports the same task non-terminal; when the vault reports non-terminal, the task is evaluated rather than refused.
   - **Log contract:** exactly one `event=terminal_gate_divergence task=<id> view_status=<s> vault_status=<s> decision_source=vault` line per evaluation, at `V(2)`, naming the view value, the vault value, and the deciding source.
   - **When it still refuses:** the vault index reports a terminal status, the id is absent from the index, or the read fails — all keep today's view-only refusal.
   - **Fail direction:** a failed read falls back to the view-only decision and logs `event=terminal_gate_index_unavailable gate=terminal_status`; a missing client makes no call and emits nothing.
   - **Read budget:** the gate consumes the pass's single shared index read; it adds no second request.
   - **Distinctness:** `terminal_status` refuses on **view** evidence (with the vault override) while `index_terminal` suppresses on **index** evidence and fails open; the two stay separate and must not be conflated.
   - **Not repaired:** the view is not converged or back-filled; `event=view_divergence` keeps reporting the disagreement.
   - **No new metric:** the existing `executor_reconcile_refused_total{gate="terminal_status"}` counter and the log line carry the signal.
   The subsection must contain the literals `terminal_gate_divergence`, `decision_source=vault`, `terminal_gate_index_unavailable`, and `index_terminal`.

3. In `CHANGELOG.md`, add a `## Unreleased` section immediately under the intro lines and above `## v0.70.1`. One bullet in the file's existing voice (`- fix: **bold summary.** prose`) that:
   - names the defect: a re-armed task (`status: in_progress`) was rewritten back to `status: completed` / `phase: done` within ~90 seconds because a park for a success that already produced one was re-emitted after a restart, and the reconcile then refused the task as terminal;
   - cites the measured reproduction from the spec (05:55:10 re-arm; 05:56:49 `event=parked_succeeded elapsed=86349s`; 05:57:22 file rewritten to completed/done; 05:57:33 `event=reconcile_refused … gate=terminal_status`; 05:57:34 `event=view_divergence …`);
   - states both halves: the park marker is now durable in the shared claim store, keyed per success completion with the per-pod map retained as a floor; and the terminal gate now lets a non-terminal vault index override a terminal view while naming the deciding source;
   - states the observability (`event=park_marker_read_failed` / `event=park_marker_write_failed`, `event=terminal_gate_divergence … decision_source=vault`, `event=terminal_gate_index_unavailable`) and that no new metric was added;
   - states that the view is not repaired and the divergence keeps being reported.
   Do not rename an existing version heading and do not touch a released section.

4. Before finishing, re-run `<verification>` and confirm it passes; walk each requirement above against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do not change any Go code, any test, or any metric name in this prompt.
- The changelog entry goes in `## Unreleased`, above `## v0.70.1`; released sections stay byte-identical.
- The docs must not describe a second index read, a config knob, a repaired view, or a converged view — none of those shipped.
- The docs must not merge `terminal_status` and `index_terminal`; they stay distinct.
- Keep the existing document voice and section structure; add sections/subsections rather than rewriting unrelated prose.
</constraints>

<verification>
- `make precommit` — must exit 0.
- `test "$(grep -c 'claims/park/' docs/claim-lease-protocol.md)" -ge 1 && echo PARK_REF_OK` — must print `PARK_REF_OK`.
- `test "$(grep -c 'park-record' docs/claim-lease-protocol.md)" -ge 1 && echo SENTINEL_OK` — must print `SENTINEL_OK`.
- `test "$(grep -c 'park_marker_read_failed' docs/claim-lease-protocol.md)" -ge 1 && echo READ_FAIL_DOC_OK` — must print `READ_FAIL_DOC_OK`.
- `test "$(grep -c 'decision_source=vault' docs/vault-api-task-index.md)" -ge 1 && echo DIVERGENCE_DOC_OK` — must print `DIVERGENCE_DOC_OK`.
- `test "$(grep -c 'terminal_gate_index_unavailable' docs/vault-api-task-index.md)" -ge 1 && echo UNAVAILABLE_DOC_OK` — must print `UNAVAILABLE_DOC_OK`.
- `head -20 CHANGELOG.md | grep -q '^## Unreleased' && echo CHANGELOG_OK` — must print `CHANGELOG_OK`.
</verification>
