---
status: approved
spec: [015-dual-acquire-claim-keys]
created: "2026-10-23T20:05:00Z"
queued: "2026-10-23T19:51:10Z"
branch: dark-factory/dual-acquire-claim-keys
---

# Dual claim keys: protocol document and changelog

<summary>
- The claim-lease protocol document explains that a repo-keyed claim owns two refs while the fleet is mixed: the per-repo key and, when the body qualifies, the per-PR key.
- It states that the per-repo key is written first and that the task spawns only when both refs are held.
- It states that reviews of different pull requests in one repository serialize again on the shared per-repo key until the dual write is removed.
- The Fence, Takeover and Restart recovery sections each describe what happens to the pair, so an operator reading one section is not left with a single-ref model.
- The two new log lines are documented next to the existing claim observability lines.
- The changelog records the change as a `feat:` bullet, so the release cut yields the next minor tag.
- No code changes.
</summary>

<objective>
Document the dual-acquire protocol in `docs/claim-lease-protocol.md` and record the change in `CHANGELOG.md`, so the pair semantics outlive this spec and an operator can read the protocol without inferring them from the code. End state: the protocol's Claim keys, Fence, Takeover and Restart recovery sections each name the pair, the file contains the phrase `dual acquire`, and the changelog carries one `feat:` bullet under `## Unreleased`.
</objective>

<context>
Read these files fully before editing:

- `docs/claim-lease-protocol.md` — the current protocol. Its sections are `## Constants`, `## Claim keys`, `## Claim state machine`, `## Fence`, `## Takeover`, `## Restart recovery`, `## Streak refs`, `## Lost-response recovery`.
- `CHANGELOG.md` — the top is `# Changelog`, an intro paragraph, then `## v0.33.0` as the first section. There is no `## Unreleased` heading today.
- `pkg/claim/key.go`, `pkg/claim/lease_manager.go`, `pkg/claim/lease_manager_tick.go` — read the behaviour implemented by prompts 1-3 before documenting it. Do not infer behaviour from this prompt alone.
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — changelog style rules.
- `specs/in-progress/015-dual-acquire-claim-keys.md` — the source for the rollout rationale and the accepted costs.

This prompt edits no Go file. The implementation facts are:
- a repo-keyed task derives the per-repo key as primary and, for a `pr-review` task whose body carries a usable standalone pull-request URL, the per-PR key as secondary;
- the primary is written first, the secondary second, and the claim is held only when both writes succeed;
- a secondary conflict releases the primary and the task is treated as claimed elsewhere;
- when the two derivations are equal (task-id-keyed types and a `pr-review` task with no usable URL) exactly one ref is written;
- every release, fence, renew, takeover, lost-claim and restart-recovery path acts on both refs together; a partial release stays pending and is retried;
- restart recovery groups by task, adopts both refs, and repairs a per-PR-only claim by deriving and writing its per-repo partner.
</context>

<requirements>
1. In `docs/claim-lease-protocol.md`, extend `## Claim keys` with a dual-acquire paragraph (or a `### Dual acquire` subsection inside it). It MUST contain the exact phrase `dual acquire`, MUST contain the word `pair` (case-insensitive), and MUST state:
   - the per-repo key is the primary and is written first; the per-PR key is the secondary and is written second;
   - the claim is held only when both writes succeed; a secondary conflict releases the primary and no Job is created;
   - the two keys collapse to one when the derivations are equal — every task-id-keyed type and a `pr-review` task with no usable pull-request URL;
   - while the fleet is mixed, reviews of different pull requests in one repository serialize on the shared per-repo key, and that ends when the dual write is removed in a later release;
   - both keys pass `validateClaimComponent`, so a hostile body cannot inject a path segment.
   Keep the existing key-shape table and the existing fallback paragraph; add the pair, do not replace them.

2. In the same file's observability list under `## Claim keys`, add two lines next to the existing `event=claim_acquired` line:
   - `event=claim_dual_acquired key=<primary> second=<secondary> task=<id>` — info, one per successful dual acquire;
   - `event=claim_dual_conflict key=<secondary> task=<id>` — warning, one per secondary write that returned `409`.

3. In `## Fence`, state that a fenced dual claim deletes both refs, best-effort, after deleting the Job once, and that a failed Job deletion keeps both refs in pending deletion. The section must contain the word `pair` (case-insensitive) or name both keys explicitly.

4. In `## Takeover`, state that a takeover must win both refs: a live secondary refuses the takeover before any write, and a secondary conflict after the primary was taken releases the primary again. The section must contain the word `pair` (case-insensitive).

5. In `## Restart recovery`, state that listed refs are grouped by task, that a running Job's pair is adopted as one claim with both refs, that a per-PR-only claim is repaired by deriving and writing its per-repo partner, and that a per-repo-only claim is adopted as a single claim because the per-PR partner is not derivable from a repository key. The section must contain the word `pair` (case-insensitive).

6. In `## Claim state machine`, add one sentence that a claim's refs are held and released as a unit: a claim is not held while only one of its refs is held, and a release that removes one ref but not the other stays pending-release.

7. Do not change `## Constants` or any value in it. Do not document a knob, flag or opt-out for lease length, tick, fence, takeover or jobless grace. Do not change the claim ref API description.

8. In `CHANGELOG.md`, insert a new `## Unreleased` heading immediately above `## v0.33.0` (one blank line between sections) with exactly one bullet beginning `- feat: `. The bullet MUST contain the phrase `dual acquire` and state: a repo-keyed claim now acquires both the per-repo ref and, for a `pr-review` task with a usable pull-request URL, the per-PR ref, spawning only when both are held; a secondary conflict releases the primary; task-id-keyed tasks and `pr-review` tasks with no usable URL keep one ref and unchanged behaviour; every release, renew, fence, takeover and restart-recovery path acts on both refs; the dual write is removed in a later release. Use the style in `changelog-guide.md` (one bullet per logical change, specific, no prompt filename, no bash comment).

9. Do not modify any Go file, the spec file, or any manifest.
</requirements>

<constraints>
- The documented behaviour must match prompts 1-3 exactly. Where the code and this prompt disagree, the code is right and the prompt is stale — but flag it, do not document an unimplemented behaviour.
- Do not add a knob, flag or opt-out for lease length, tick, fence, takeover or jobless grace.
- Do not document a new metric series; the result set stays at the 14 existing series.
- Do NOT edit the spec file. Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run from the repository root:

```bash
grep -n 'dual acquire' docs/claim-lease-protocol.md
```

This must print at least one line. Then check that each named section mentions the pair:

```bash
for sec in 'Claim keys' 'Fence' 'Takeover' 'Restart recovery'; do
  awk -v s="## $sec" '$0==s{p=1;next} /^## /{p=0} p' docs/claim-lease-protocol.md | grep -qi 'pair' \
    || { echo "missing pair in $sec"; exit 1; }
done
```

Then check the changelog bullet is under `## Unreleased` and not under a released section:

```bash
awk '/^## /{sec=$0} /^- feat: / && /dual acquire/{print sec; exit}' CHANGELOG.md | grep -qx '## Unreleased'
```

Then the final gate:

```bash
GOFLAGS=-buildvcs=false make precommit VERSION=dev
```

`make precommit` must exit 0.

Before you finish, re-run every command above and confirm each passes. Walk AC9 against the document, and confirm the changelog bullet is a `feat:` under `## Unreleased`.
</verification>
